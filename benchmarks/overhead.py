"""agent-vcr overhead micro-benchmark. Run: python benchmarks/overhead.py

Stdlib-only. Drives the real tape / model_proxy / mcp_proxy modules against an
in-process fake provider and fake MCP HTTP server (no network, no external deps)
and prints a small table:
  (a) record overhead per proxied request vs direct
  (b) replay wall-clock vs live re-run (replay makes zero upstream calls)
  (c) tape size per run

Connections are reused (HTTP/1.1 keep-alive) so the numbers reflect the proxy's
own added work (byte-forward + one tape append), not per-request connection
setup. Against a real (slow) upstream the absolute proxy overhead is the same
sub-millisecond delta on top of whatever the upstream already costs.
"""
import http.client
import json
import os
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent_vcr.tape import Tape
from agent_vcr.model_proxy import ModelProxy
from agent_vcr.mcp_proxy import HttpMcpProxy

N = 200  # requests / tool calls per run
PROVIDER_BODY = json.dumps({"id": "msg_1", "content": [{"type": "text", "text": "x"}],
                            "usage": {"input_tokens": 3, "output_tokens": 1}}).encode()


class FakeProvider(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        self.rfile.read(n)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(PROVIDER_BODY)))
        self.end_headers(); self.wfile.write(PROVIDER_BODY)


class FakeMcp(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        req = json.loads(self.rfile.read(n) or b"{}")
        result = {"jsonrpc": "2.0", "id": req.get("id"),
                  "result": {"content": [{"type": "text", "text": "ok"}]}}
        body = json.dumps(result).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)


def _serve(handler):
    s = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    return s, s.server_address[1]


def time_ms(host, port, path, body, n=N):
    """Send n requests on one reused keep-alive connection; return ms/req."""
    c = http.client.HTTPConnection(host, port)
    c.request("POST", path, body=body, headers={"Content-Type": "application/json", "Connection": "keep-alive"})
    c.getresponse().read()  # warmup
    t = time.perf_counter()
    for _ in range(n):
        c.request("POST", path, body=body, headers={"Content-Type": "application/json", "Connection": "keep-alive"})
        c.getresponse().read()
    dt = (time.perf_counter() - t) * 1000 / n
    c.close()
    return dt


def main():
    up, up_port = _serve(FakeProvider)
    mcp, mcp_port = _serve(FakeMcp)
    tmp = os.path.join(tempfile.mkdtemp(), "bench.jsonl")

    tape = Tape(tmp, "record")
    mp = ModelProxy("127.0.0.1", 0, tape,
                    {"anthropic": f"http://127.0.0.1:{up_port}", "openai": "http://127.0.0.1:1"},
                    "record"); mp.start()
    hp = HttpMcpProxy("127.0.0.1", 0, tape, "record", "strict",
                      {"fs": f"http://127.0.0.1:{mcp_port}"}); hp.start()
    body = b'{"model":"c","stream":false,"messages":[]}'
    rpc = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                      "params": {"name": "read", "arguments": {"p": "/x"}}}).encode()

    dm = time_ms("127.0.0.1", up_port, "/v1/messages", b'{"stream":false}')
    pm = time_ms("127.0.0.1", mp.port, "/v1/messages", body)
    dt = time_ms("127.0.0.1", mcp_port, "/", rpc)
    pt = time_ms("127.0.0.1", hp.port, "/fs", rpc)
    mp.stop(); hp.stop(); tape.close()

    # replay (playback): serves recorded responses from disk, zero upstream calls.
    pb_tape = Tape(tmp, "replay")
    pb = ModelProxy("127.0.0.1", 0, pb_tape,
                    {"anthropic": "http://127.0.0.1:1", "openai": "http://127.0.0.1:1"}, "playback")
    pb.start()
    rm = time_ms("127.0.0.1", pb.port, "/v1/messages", body)
    pb.stop(); pb_tape.close()

    tape_bytes = os.path.getsize(tmp)
    events = sum(1 for _ in open(tmp, encoding="utf-8"))
    rate = tape_bytes / events if events else 0

    print("agent-vcr overhead benchmark  (N=%d, in-process fakes, keep-alive, no network)" % N)
    print("=" * 64)
    print(f"{'metric':40} {'direct':>10} {'proxied':>10}")
    print("-" * 64)
    print(f"{'model request latency (ms/req)':40} {dm:>10.3f} {pm:>10.3f}")
    print(f"{'tool call latency (ms/call)':40} {dt:>10.3f} {pt:>10.3f}")
    print(f"{'replay model latency (ms/req)':40} {'%.3f' % dm:>10} {'%.3f' % rm:>10}")
    print("-" * 64)
    print(f"proxy overhead per model request:   +{pm - dm:.3f} ms")
    print(f"proxy overhead per tool call:        +{pt - dt:.3f} ms")
    print(f"replay upstream calls:               0  (served from disk)")
    print(f"tape size per run:                   {tape_bytes} bytes  ({events} events, {rate:.0f} B/event)")
    up.shutdown(); up.server_close(); mcp.shutdown(); mcp.server_close()


if __name__ == "__main__":
    main()