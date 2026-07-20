"""agent-vcr integration self-check. Run: python selfcheck.py. No test framework."""
import http.client
import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(__file__))
from agent_vcr.tape import Tape
from agent_vcr.model_proxy import ModelProxy
from agent_vcr.mcp_proxy import StdioMcpProxy, HttpMcpProxy

PROVIDER_CALLS = {"n": 0}
PROVIDER_LOCK = threading.Lock()


class FakeProvider(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        global PROVIDER_CALLS
        with PROVIDER_LOCK:
            PROVIDER_CALLS["n"] += 1
        length = int(self.headers.get("Content-Length", 0) or 0)
        req = json.loads(self.rfile.read(length))
        # First call: return a tool_use; second: final text.
        if PROVIDER_CALLS["n"] == 1:
            body = {"id": "msg_1", "content": [
                {"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {"path": "/etc/hosts"}}],
                "stop_reason": "tool_use", "usage": {"input_tokens": 5, "output_tokens": 2}}
        else:
            body = {"id": "msg_2", "content": [{"type": "text", "text": "done"}],
                    "stop_reason": "end_turn", "usage": {"input_tokens": 6, "output_tokens": 1}}
        payload = json.dumps(body).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload))); self.end_headers()
        self.wfile.write(payload)


ECHO_SERVER = None
def _write_echo_server(path):
    with open(path, "w") as f:
        f.write(
            "import json, sys\n"
            "for line in sys.stdin:\n"
            "    m=json.loads(line)\n"
            "    if m.get('method')=='tools/call':\n"
            "        r={'jsonrpc':'2.0','id':m['id'],'result':{'content':[{'type':'text','text':'ECHO:'+json.dumps(m['params']['arguments'])}]}}\n"
            "        sys.stdout.write(json.dumps(r)+'\\n'); sys.stdout.flush()\n"
            "    elif 'id' in m:\n"
            "        sys.stdout.write(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':{}})+'\\n'); sys.stdout.flush()\n"
        )


def run_stdio_proxy(stdin_lines, tape_path, mode, on_miss="strict", echo_path=None):
    env = dict(os.environ)
    env["AGENT_VCR_MODE"] = mode
    env["AGENT_VCR_TAPE"] = tape_path
    env["AGENT_VCR_ON_MISS"] = on_miss
    p = subprocess.Popen(
        [sys.executable, "-m", "agent_vcr.cli", "mcp-stdio", "--server", "fs",
         "--", sys.executable, echo_path],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    out, err = p.communicate("".join(l + "\n" for l in stdin_lines), timeout=10)
    return p.returncode, out, err


def main() -> int:
    d = tempfile.mkdtemp()
    echo_path = os.path.join(d, "echo.py")
    _write_echo_server(echo_path)
    tape_path = os.path.join(d, "run.jsonl")

    # fake provider
    up = ThreadingHTTPServer(("127.0.0.1", 0), FakeProvider); up_port = up.server_address[1]
    threading.Thread(target=up.serve_forever, daemon=True).start()

    upstreams = {"anthropic": f"http://127.0.0.1:{up_port}", "openai": f"http://127.0.0.1:{up_port}"}

    # ---- RECORD ----
    tape = Tape(tape_path, "record")
    mp = ModelProxy("127.0.0.1", 0, tape, upstreams, "record"); mp.start()
    # scripted agent: two model turns + one tool call
    def agent_record():
        c = http.client.HTTPConnection("127.0.0.1", mp.port)
        # turn 1: model asks for a tool
        c.request("POST", "/v1/messages", body=json.dumps({"model": "claude", "stream": False, "messages": [{"role": "user", "content": "go"}]}),
                  headers={"Content-Type": "application/json"})
        r1 = json.loads(c.getresponse().read())
        assert r1["content"][0]["type"] == "tool_use", r1
        tool_input = r1["content"][0]["input"]
        # agent dispatches the tool call through the stdio MCP proxy (subprocess)
        rc, out, err = run_stdio_proxy([
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                        "params": {"name": "read_file", "arguments": tool_input}}),
        ], tape_path, "record", echo_path=echo_path)
        assert rc == 0, err
        tool_result = json.loads(out.strip().splitlines()[-1])
        # turn 2: final
        c.request("POST", "/v1/messages", body=json.dumps({"model": "claude", "stream": False, "messages": [{"role": "user", "content": "again"}]}),
                  headers={"Content-Type": "application/json"})
        r2 = json.loads(c.getresponse().read())
        assert r2["stop_reason"] == "end_turn", r2
        c.close()
    agent_record()
    mp.stop(); tape.close()

    rec_tape = Tape(tape_path, "replay")
    rec_tool_result = None
    for e in rec_tape.events():
        if e.get("kind") == "tool_result":
            rec_tool_result = e["result"]
    assert rec_tool_result is not None and "ECHO:" in rec_tool_result["content"][0]["text"], rec_tool_result
    assert PROVIDER_CALLS["n"] == 2, PROVIDER_CALLS

    # ---- REPLAY (tool-stub) ----
    # In tool-stub replay, the model is still live, so provider will be called again.
    PROVIDER_CALLS["n"] = 0
    rp_tape = Tape(tape_path, "replay")
    mp2 = ModelProxy("127.0.0.1", 0, rp_tape, upstreams, "replay"); mp2.start()
    def agent_replay():
        c = http.client.HTTPConnection("127.0.0.1", mp2.port)
        c.request("POST", "/v1/messages", body=json.dumps({"model": "claude", "stream": False, "messages": [{"role": "user", "content": "go"}]}),
                  headers={"Content-Type": "application/json"})
        json.loads(c.getresponse().read())
        rc, out, err = run_stdio_proxy([
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                        "params": {"name": "read_file", "arguments": {"path": "/etc/hosts"}}}),
        ], tape_path, "replay", echo_path=echo_path)
        assert rc == 0, err
        rep_tool_result = json.loads(out.strip().splitlines()[-1])
        assert rep_tool_result["result"] == rec_tool_result, (rep_tool_result, rec_tool_result)
        c.close()
    agent_replay()
    mp2.stop()
    # provider WAS called in tool-stub replay (model is live) — that's expected
    assert PROVIDER_CALLS["n"] >= 1, PROVIDER_CALLS

    # ---- REPLAY --playback (zero provider calls) ----
    PROVIDER_CALLS["n"] = 0
    pb_tape = Tape(tape_path, "replay")
    mp3 = ModelProxy("127.0.0.1", 0, pb_tape, upstreams, "playback"); mp3.start()
    def agent_playback():
        c = http.client.HTTPConnection("127.0.0.1", mp3.port)
        for _ in range(2):
            c.request("POST", "/v1/messages", body=json.dumps({"model": "claude", "stream": False, "messages": [{"role": "user", "content": "go"}]}),
                      headers={"Content-Type": "application/json"})
            json.loads(c.getresponse().read())
        c.close()
    agent_playback()
    mp3.stop()
    assert PROVIDER_CALLS["n"] == 0, f"playback must not call the provider, got {PROVIDER_CALLS}"

    # ---- strict miss exits nonzero ----
    rc, out, err = run_stdio_proxy([
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "read_file", "arguments": {"path": "/never/recorded"}}}),
    ], tape_path, "replay", on_miss="strict", echo_path=echo_path)
    assert rc != 0, (rc, out, err)
    assert "miss" in (err + out).lower(), (err, out)

    up.shutdown(); up.server_close()
    print("selfcheck OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())