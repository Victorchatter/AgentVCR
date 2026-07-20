import hashlib
import json
import os
import threading
from collections import deque

# ponytail: cross-process + in-process lock for the tape. The model proxy and
# http-mcp proxy write the tape in-process (threads); each mcp-stdio proxy is a
# separate process writing the same tape. A threading.Lock serializes in-process
# writers; a file lock serializes cross-process writers. Without this, two writers
# would truncate/interleave lines or collide on seq. Ceiling: O(n) max-seq read
# per write — fine for runs up to ~10k events; for high-volume runs, upgrade to a
# single seq-authority process or a sidecar+merge scheme.

if os.name == "nt":
    import msvcrt

    def _flock(fd):
        msvcrt.locking(fd, msvcrt.LK_LOCK, 1)

    def _funlock(fd):
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _flock(fd):
        fcntl.flock(fd, fcntl.LOCK_EX)

    def _funlock(fd):
        fcntl.flock(fd, fcntl.LOCK_UN)


class _FileLock:
    def __init__(self, path):
        self._path = path + ".lock"
        self._fd = None

    def __enter__(self):
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR)
        _flock(self._fd)
        return self

    def __exit__(self, *a):
        try:
            _funlock(self._fd)
        finally:
            os.close(self._fd)
            self._fd = None


def _max_seq_in_file(path: str) -> int:
    if not os.path.exists(path):
        return 0
    m = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                s = json.loads(line).get("seq")
            except json.JSONDecodeError:
                continue
            if isinstance(s, int) and s > m:
                m = s
    return m


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
        self._lock = threading.Lock()
        self._fh = None
        if mode == "record":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            # seed _seq from existing contents so a writer joining a run already
            # in progress (e.g. an mcp-stdio subprocess spawned mid-run) continues
            # the seq monotonically instead of restarting at 1.
            self._seq = _max_seq_in_file(path)
            self._fh = open(path, "a", encoding="utf-8")  # append, never truncate
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

    def _write_locked(self, event: dict, is_replay_extended: bool = False) -> int:
        with self._lock, _FileLock(self.path):
            # reconcile in-memory seq with whatever other writers have committed
            file_max = _max_seq_in_file(self.path)
            if file_max > self._seq:
                self._seq = file_max
            if "seq" not in event:
                self._seq += 1
                event["seq"] = self._seq
            else:
                s = event["seq"]
                if isinstance(s, int) and s > self._seq:
                    self._seq = s
            if is_replay_extended:
                event["replay_extended"] = True
            line = json.dumps(event, ensure_ascii=False) + "\n"
            if self._fh is not None:
                self._fh.write(line)
                self._fh.flush()
            else:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line)
            self._events.append(event)
            return event["seq"]

    def next_seq(self) -> int:
        # ponytail: standalone next_seq does not reserve a slot; callers wanting
        # the authoritative linked seq should use write_event's return value.
        with self._lock, _FileLock(self.path):
            file_max = _max_seq_in_file(self.path)
            if file_max > self._seq:
                self._seq = file_max
            self._seq += 1
            return self._seq

    def write_event(self, event: dict) -> int:
        if self.mode != "record":
            raise RuntimeError("write_event only valid in record mode")
        return self._write_locked(event)

    def append_replay_event(self, event: dict) -> int:
        return self._write_locked(event, is_replay_extended=True)

    def pop_model_response(self) -> dict | None:
        with self._lock:
            return self._model_responses.popleft() if self._model_responses else None

    def peek_model_request(self, seq: int) -> dict | None:
        for ev in self._events:
            if ev.get("kind") == "model_request" and ev.get("seq") == seq:
                return ev
        return None

    def pop_tool_result(self, server: str, tool: str, ah: str) -> dict | None:
        with self._lock:
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
    # a second record-mode writer joining mid-run appends (no truncate) and continues seq
    t2 = Tape(tmp, "record")
    s5 = t2.write_event({"kind": "tool_call", "server": "fs", "tool": "read2", "args": {"p": "/y"}, "args_hash": args_hash({"p": "/y"})})
    assert s5 == 5, s5  # seeded from file max (4), then +1
    t2.close()
    # replay
    r = Tape(tmp, "replay")
    assert r.pop_model_response()["body"] == {"ok": True}
    assert r.pop_model_response() is None
    ah = args_hash({"p": "/x"})
    assert r.pop_tool_result("fs", "read", ah)["result"] == {"text": "hi"}
    assert r.pop_tool_result("fs", "read", ah) is None
    assert r.peek_model_request(1)["provider"] == "anthropic"
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    print("tape selfcheck OK")


if __name__ == "__main__":
    _selfcheck()