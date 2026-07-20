import json
import subprocess
import sys

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


if __name__ == "__main__":
    _selfcheck()