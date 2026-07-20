import hashlib
import json
import os
from collections import deque


def canonical_json(obj) -> str:
    """Stable JSON: sorted keys, no whitespace. Used for args_hash."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def args_hash(args) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(args).encode("utf-8")).hexdigest()


class Tape:
    def __init__(self, path: str, mode: str):
        if mode not in ("record", "replay"):
            raise ValueError(f"mode must be record or replay, got {mode!r}")
        self.path = path
        self.mode = mode
        self._seq = 0
        self._events: list[dict] = []
        self._model_responses: deque[dict] = deque()
        self._tool_index: dict[tuple, deque[dict]] = {}
        self._fh = None
        if mode == "record":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self._fh = open(path, "w", encoding="utf-8")
        else:
            self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"tape not found: {self.path}")
        with open(self.path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"bad JSONL at {self.path}:{i}: {e}") from e
                self._events.append(ev)
                if ev.get("kind") == "model_response":
                    self._model_responses.append(ev)
                if ev.get("kind") == "tool_result":
                    key = (ev.get("server"), ev.get("tool"), ev.get("args_hash"))
                    self._tool_index.setdefault(key, deque()).append(ev)
                seq = ev.get("seq", 0)
                if isinstance(seq, int) and seq > self._seq:
                    self._seq = seq

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def write_event(self, event: dict) -> int:
        if "seq" not in event:
            event["seq"] = self.next_seq()
        else:
            s = event["seq"]
            if isinstance(s, int) and s > self._seq:
                self._seq = s
        if self.mode != "record":
            raise RuntimeError("write_event only valid in record mode")
        self._fh.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._fh.flush()
        self._events.append(event)
        return event["seq"]

    def append_replay_event(self, event: dict) -> int:
        # ponytail: reopen-on-append per call; replay-extended writes are rare. Upgrade: keep a long-lived handle if passthrough becomes hot.
        event["seq"] = self.next_seq()
        event["replay_extended"] = True
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._events.append(event)
        return event["seq"]

    def pop_model_response(self) -> dict | None:
        return self._model_responses.popleft() if self._model_responses else None

    def peek_model_request(self, seq: int) -> dict | None:
        for ev in self._events:
            if ev.get("kind") == "model_request" and ev.get("seq") == seq:
                return ev
        return None

    def pop_tool_result(self, server: str, tool: str, ah: str) -> dict | None:
        key = (server, tool, ah)
        dq = self._tool_index.get(key)
        if not dq:
            return None
        return dq.popleft()

    def events(self) -> list[dict]:
        return list(self._events)

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


def _selfcheck() -> None:
    import tempfile
    tmp = os.path.join(tempfile.mkdtemp(), "t.jsonl")
    # record
    t = Tape(tmp, "record")
    s1 = t.write_event({"kind": "model_request", "provider": "anthropic", "body": {"model": "claude-3"}})
    s2 = t.write_event({"kind": "tool_call", "server": "fs", "tool": "read", "args": {"p": "/x"}, "args_hash": args_hash({"p": "/x"})})
    s3 = t.write_event({"kind": "tool_result", "server": "fs", "tool": "read", "args_hash": args_hash({"p": "/x"}), "result": {"text": "hi"}})
    s4 = t.write_event({"kind": "model_response", "provider": "anthropic", "body": {"ok": True}})
    assert (s1, s2, s3, s4) == (1, 2, 3, 4), (s1, s2, s3, s4)
    t.close()
    # replay
    r = Tape(tmp, "replay")
    assert r.pop_model_response()["body"] == {"ok": True}
    assert r.pop_model_response() is None  # only one recorded
    ah = args_hash({"p": "/x"})
    assert r.pop_tool_result("fs", "read", ah)["result"] == {"text": "hi"}
    assert r.pop_tool_result("fs", "read", ah) is None  # FIFO exhausted
    assert r.peek_model_request(1)["provider"] == "anthropic"
    # canonical_json stability
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    print("tape selfcheck OK")


if __name__ == "__main__":
    _selfcheck()