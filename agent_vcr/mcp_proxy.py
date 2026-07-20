import http.client as _hc
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse as _urlparse

from .tape import Tape, args_hash


def _is_tools_call(msg: dict) -> bool:
    return msg.get("method") == "tools/call" and "id" in msg


def _tools_call_info(msg: dict):
    params = msg.get("params") or {}
    name = params.get("name")
    arguments = params.get("arguments") or {}
    return name, arguments


class StdioMcpProxy:
    def __init__(self, server_name, tape, mode, on_miss, real_cmd, real_env=None):
        self.server_name = server_name
        self.tape = tape
        self.mode = mode  # record | replay | playback
        self.on_miss = on_miss  # strict | passthrough  (replay/playback only)
        self.real_cmd = real_cmd
        self.real_env = real_env
        self._proc = None  # lazily spawned real server
        self._calls: dict = {}  # id -> {seq, tool, args, ah}

    def _ensure_server(self):
        if self._proc is None:
            self._proc = subprocess.Popen(
                self.real_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=sys.stderr, env=self.real_env, bufsize=1, text=True,
            )
        return self._proc

    def _send(self, obj: dict, to: str):
        line = json.dumps(obj) + "\n"
        if to == "agent":
            sys.stdout.write(line); sys.stdout.flush()
        else:
            self._proc.stdin.write(line); self._proc.stdin.flush()

    def _record_call(self, msg_id: int, tool: str, args: dict) -> int:
        ah = args_hash(args)
        seq = self.tape.write_event({
            "kind": "tool_call", "server": self.server_name, "tool": tool,
            "args": args, "args_hash": ah,
        })
        self._calls[msg_id] = {"seq": seq, "tool": tool, "args_hash": ah}
        return seq

    def _stub_result(self, msg_id: int, tool: str, args: dict) -> bool:
        """Return True on hit, False on miss."""
        ah = args_hash(args)
        rec = self.tape.pop_tool_result(self.server_name, tool, ah)
        if rec is None:
            return False
        self._send({"jsonrpc": "2.0", "id": msg_id, "result": rec["result"]}, "agent")
        return True

    def run(self) -> int:
        if self.mode == "record":
            return self._run_record()
        return self._run_replay()

    # ---- record: proxy everything to the real server, capture tools/call ----
    def _run_record(self) -> int:
        proc = self._ensure_server()
        import threading

        def server_to_agent():
            for line in proc.stdout:
                msg = json.loads(line)
                if "id" in msg and "result" in msg and msg["id"] in self._calls:
                    c = self._calls.pop(msg["id"])
                    self.tape.write_event({
                        "kind": "tool_result", "seq": c["seq"],
                        "server": self.server_name, "tool": c["tool"],
                        "args_hash": c["args_hash"], "result": msg["result"],
                    })
                sys.stdout.write(line); sys.stdout.flush()

        t = threading.Thread(target=server_to_agent, daemon=True)
        t.start()
        for line in sys.stdin:
            msg = json.loads(line)
            if _is_tools_call(msg):
                tool, args = _tools_call_info(msg)
                self._record_call(msg["id"], tool, args)
            proc.stdin.write(line); proc.stdin.flush()
        proc.stdin.close()
        t.join(timeout=1)
        return 0

    # ---- replay/playback: stub from tape; passthrough spawns server on miss ----
    def _run_replay(self) -> int:
        for line in sys.stdin:
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not _is_tools_call(msg):
                # forward non-tools/call traffic to a real server only if we have one
                if self._proc is not None:
                    self._send(msg, "server")
                continue
            msg_id = msg["id"]
            tool, args = _tools_call_info(msg)
            if self._stub_result(msg_id, tool, args):
                continue
            # miss
            if self.on_miss == "passthrough":
                proc = self._ensure_server()
                self._send(msg, "server")
                # remember to capture the response when it comes back; reuse record path
                self._record_call(msg_id, tool, args)
                # ponytail: simplest correct passthrough = fall back to full record loop.
                # Hand off to the record loop for the rest of this process.
                return self._run_record_after_passthrough()
            # strict miss
            sys.stderr.write(f"agent-vcr: tool miss (server={self.server_name} tool={tool} "
                             f"args_hash={args_hash(args)})\n")
            self._send({"jsonrpc": "2.0", "id": msg_id,
                        "error": {"code": -32603, "message": "agent-vcr: unmatched tool call (strict miss)"}},
                       "agent")
            return 2
        return 0

    def _run_record_after_passthrough(self) -> int:
        # Reuse record semantics: server->agent pump captures results for pending _calls.
        return self._run_record()


class _McpHttpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass

    def _split(self):
        parts = self.path.lstrip("/").split("/", 1)
        server = parts[0]
        rest = "/" + parts[1] if len(parts) > 1 else "/"
        return server, rest

    def _handle(self, method):
        server, rest = self._split()
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            req = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            req = {}
        is_sse = "text/event-stream" in (self.headers.get("Accept") or "")

        # replay/playback: try stub for tools/call
        if self.server.mode in ("replay", "playback") and req.get("method") == "tools/call" and "id" in req:
            tool = (req.get("params") or {}).get("name")
            args = (req.get("params") or {}).get("arguments") or {}
            rec = self.server.tape.pop_tool_result(server, tool, args_hash(args))
            if rec is not None:
                payload = json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": rec["result"]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers(); self.wfile.write(payload); return
            if self.server.on_miss == "passthrough":
                pass  # fall through to live forward + capture
            else:
                msg = json.dumps({"error": "agent-vcr: unmatched tool call",
                                  "server": server, "tool": tool,
                                  "args_hash": args_hash(args)}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers(); self.wfile.write(msg); return

        # record or passthrough: forward to real upstream, capture tools/call
        base = self.server.upstreams.get(server)
        if not base:
            self.send_error(404, f"unknown mcp server: {server}"); return
        u = _urlparse(base)
        cls = _hc.HTTPSConnection if u.scheme == "https" else _hc.HTTPConnection
        conn = cls(u.netloc, timeout=600)
        out_headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in ("host", "content-length", "connection", "transfer-encoding")}
        conn.request(method, rest, body=raw, headers=out_headers)
        resp = conn.getresponse()

        seq = None
        if self.server.mode == "record" and req.get("method") == "tools/call" and "id" in req:
            tool = (req.get("params") or {}).get("name")
            args = (req.get("params") or {}).get("arguments") or {}
            seq = self.server.tape.write_event({
                "kind": "tool_call", "server": server, "tool": tool,
                "args": args, "args_hash": args_hash(args),
            })

        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "content-length", "connection"):
                continue
            self.send_header(k, v)
        captured = bytearray()
        streaming = "text/event-stream" in (resp.getheader("Content-Type") or "")
        if streaming:
            self.send_header("Transfer-Encoding", "chunked"); self.end_headers()
            while True:
                chunk = resp.read(4096)
                if not chunk: break
                captured += chunk
                self.wfile.write(b"%x\r\n" % len(chunk) + chunk + b"\r\n"); self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
        else:
            data = resp.read(); captured += data
            self.send_header("Content-Length", str(len(data))); self.end_headers()
            self.wfile.write(data)
        conn.close()

        if seq is not None and self.server.mode == "record":
            result = _find_rpc_result(req.get("id"), bytes(captured).decode("utf-8", "replace"),
                                     resp.getheader("Content-Type") or "")
            if result is not None:
                self.server.tape.write_event({
                    "kind": "tool_result", "seq": seq, "server": server,
                    "tool": (req.get("params") or {}).get("name"),
                    "args_hash": args_hash((req.get("params") or {}).get("arguments") or {}),
                    "result": result,
                })

    def do_POST(self):
        try: self._handle("POST")
        except Exception as e: self.send_error(502, str(e))
    def do_GET(self):
        try: self._handle("GET")
        except Exception as e: self.send_error(502, str(e))


def _find_rpc_result(rpc_id, body: str, content_type: str):
    # ponytail: scan for the JSON-RPC response whose id matches; works for JSON and SSE. Upgrade: full SSE parser.
    if "event-stream" in (content_type or "").lower():
        for line in body.splitlines():
            if line.startswith("data:"):
                try:
                    obj = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue
                if obj.get("id") == rpc_id and "result" in obj:
                    return obj["result"]
        return None
    try:
        obj = json.loads(body)
        if obj.get("id") == rpc_id and "result" in obj:
            return obj["result"]
    except json.JSONDecodeError:
        return None
    return None


class HttpMcpProxy:
    def __init__(self, host, port, tape, mode, on_miss, upstreams):
        self.host = host; self.port = port; self.tape = tape
        self.mode = mode; self.on_miss = on_miss; self.upstreams = upstreams
        self._srv = None; self._thread = None

    def start(self):
        srv = ThreadingHTTPServer((self.host, self.port), _McpHttpHandler)
        srv.tape = self.tape; srv.mode = self.mode; srv.on_miss = self.on_miss
        srv.upstreams = self.upstreams
        self.port = srv.server_address[1]
        self._srv = srv
        self._thread = threading.Thread(target=srv.serve_forever, daemon=True)
        self._thread.start()

    def url_for(self, server_name):
        return f"http://{self.host}:{self.port}/{server_name}"

    def stop(self):
        if self._srv:
            self._srv.shutdown(); self._srv.server_close(); self._srv = None


def _selfcheck() -> None:
    import io, os, tempfile
    from .tape import Tape

    echo_server = os.path.join(tempfile.mkdtemp(), "echo.py")
    with open(echo_server, "w") as f:
        f.write(
            "import json, sys\n"
            "for line in sys.stdin:\n"
            "    m=json.loads(line)\n"
            "    if m.get('method')=='tools/call':\n"
            "        sys.stdout.write(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':{'content':[{'type':'text','text':'echo:'+json.dumps(m['params']['arguments'])}]}})+'\\n'); sys.stdout.flush()\n"
            "    elif m.get('method')=='initialize':\n"
            "        sys.stdout.write(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':{'protocolVersion':'2024-11-05','capabilities':{'tools':{}}}})+'\\n'); sys.stdout.flush()\n"
        )
    tmp = os.path.join(tempfile.mkdtemp(), "t.jsonl")

    def run_proxy(stdin_lines, mode, on_miss="strict"):
        # drive StdioMcpProxy in-process by swapping sys.stdin/stdout
        tape = Tape(tmp, "record" if mode == "record" else "replay")
        proxy = StdioMcpProxy("fs", tape, mode, on_miss, [sys.executable, echo_server])
        old_in, old_out = sys.stdin, sys.stdout
        sys.stdin = io.StringIO("".join(l + "\n" for l in stdin_lines))
        sys.stdout = io.StringIO()
        try:
            rc = proxy.run()
            out = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_in, old_out
        tape.close()
        return rc, out

    call = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": "read_file", "arguments": {"path": "/x"}}})

    # record
    rc, out = run_proxy([call], "record")
    assert rc == 0, out
    r = Tape(tmp, "replay")
    kinds = [e["kind"] for e in r.events()]
    assert kinds == ["tool_call", "tool_result"], kinds
    assert r.events()[1]["result"]["content"][0]["text"] == 'echo:{"path": "/x"}', r.events()[1]

    # replay (stub) — returns recorded result, never touches echo server
    rc, out = run_proxy([call], "replay")
    assert rc == 0, out
    resp = json.loads(out.strip().splitlines()[-1])
    assert resp["result"]["content"][0]["text"] == 'echo:{"path": "/x"}', resp

    # strict miss exits nonzero
    miss = json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                       "params": {"name": "read_file", "arguments": {"path": "/never"}}})
    rc, out = run_proxy([miss], "replay", on_miss="strict")
    assert rc != 0, (rc, out)
    print("mcp stdio selfcheck OK")

    # Streamable HTTP MCP: record then replay-stub
    import http.client as _hc2
    class _Up(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *a): pass
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length",0) or 0))
            m = json.loads(body)
            r = {"jsonrpc": "2.0", "id": m["id"],
                 "result": {"content": [{"type": "text", "text": "http-echo:" + json.dumps(m["params"]["arguments"])}]}}
            payload = json.dumps(r).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload))); self.end_headers(); self.wfile.write(payload)
    up = ThreadingHTTPServer(("127.0.0.1", 0), _Up); up_port = up.server_address[1]
    threading.Thread(target=up.serve_forever, daemon=True).start()

    tmp2 = os.path.join(tempfile.mkdtemp(), "t2.jsonl")
    rt = Tape(tmp2, "record")
    hp = HttpMcpProxy("127.0.0.1", 0, rt, "record", "strict", {"svc": f"http://127.0.0.1:{up_port}"})
    hp.start()
    c = _hc2.HTTPConnection("127.0.0.1", hp.port)
    c.request("POST", "/svc/mcp", body=json.dumps({"jsonrpc":"2.0","id":7,"method":"tools/call",
                 "params":{"name":"get","arguments":{"q":"hi"}}}),
               headers={"Content-Type":"application/json"})
    rr = c.getresponse(); dd = rr.read().decode(); c.close(); hp.stop(); rt.close()
    assert "http-echo:" in dd, dd
    r2 = Tape(tmp2, "replay")
    assert [e["kind"] for e in r2.events()] == ["tool_call", "tool_result"], r2.events()
    # replay stub
    pt = Tape(tmp2, "replay")
    hp2 = HttpMcpProxy("127.0.0.1", 0, pt, "replay", "strict", {"svc": "http://127.0.0.1:1"})
    hp2.start()
    c2 = _hc2.HTTPConnection("127.0.0.1", hp2.port)
    c2.request("POST", "/svc/mcp", body=json.dumps({"jsonrpc":"2.0","id":7,"method":"tools/call",
                  "params":{"name":"get","arguments":{"q":"hi"}}}),
                headers={"Content-Type":"application/json"})
    rr2 = c2.getresponse(); dd2 = rr2.read().decode(); c2.close(); hp2.stop()
    assert "http-echo:" in dd2, dd2
    up.shutdown(); up.server_close()
    print("mcp http selfcheck OK")


if __name__ == "__main__":
    _selfcheck()