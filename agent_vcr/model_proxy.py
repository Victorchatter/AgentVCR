import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .tape import Tape


# ponytail: best-effort usage extraction for `list` display only. SSE/gzip edge cases
# return None; nothing depends on usage being present. Upgrade: full SSE reassembly if needed.
def _extract_usage(provider: str, content_type: str, body: str):
    ct = (content_type or "").lower()
    try:
        if "event-stream" in ct:
            last = None
            for line in body.splitlines():
                if line.startswith("data:"):
                    try:
                        last = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        pass
            if isinstance(last, dict) and isinstance(last.get("usage"), dict):
                return last["usage"]
            return None
        obj = json.loads(body)
        if isinstance(obj, dict) and isinstance(obj.get("usage"), dict):
            return obj["usage"]
    except Exception:
        return None
    return None


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # silence default logging
        pass

    def _provider(self) -> str:
        return "anthropic" if self.path.startswith("/v1/messages") else "openai"

    def _do(self, method: str):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            req = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            req = {}
        streaming = bool(req.get("stream"))
        provider = self._provider()

        if self.server.mode == "playback":
            rec = self.server.tape.pop_model_response() if self.server.tape else None
            if rec is None:
                # ponytail: report next expected seq as count+1; exact seq not tracked for playback.
                next_seq = (self.server._playback_consumed + 1)
                self.server._playback_failed = True
                payload = json.dumps({"error": "playback exhausted", "seq": next_seq}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self.server._playback_consumed += 1
            body = rec["body"].encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", rec.get("content_type", "application/json"))
            if rec.get("content_encoding"):
                self.send_header("Content-Encoding", rec["content_encoding"])
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        base = self.server.upstreams[provider]
        u = urlparse(base)
        conn_cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
        # ponytail: one fresh connection per request; fine for low agent QPS. Upgrade: connection pool if throughput matters.
        conn = conn_cls(u.netloc, timeout=600)
        upstream_path = self.path
        out_headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in ("host", "content-length", "connection", "transfer-encoding")}
        conn.request(method, upstream_path, body=raw, headers=out_headers)
        resp = conn.getresponse()

        # record the request event (record mode only)
        seq = None
        if self.server.tape is not None and self.server.mode == "record":
            seq = self.server.tape.write_event({
                "kind": "model_request",
                "provider": provider,
                "provider_url": base,
                "body": req,
            })

        # forward response status + preserved headers
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "content-length", "connection"):
                continue
            self.send_header(k, v)
        captured = bytearray()
        if streaming:
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                captured += chunk
                self.wfile.write(b"%x\r\n" % len(chunk) + chunk + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
        else:
            data = resp.read()
            captured += data
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        conn.close()

        if self.server.tape is not None and self.server.mode == "record":
            body_text = bytes(captured).decode("utf-8", errors="replace")
            ct = resp.getheader("Content-Type") or "application/json"
            ce = resp.getheader("Content-Encoding")
            self.server.tape.write_event({
                "kind": "model_response",
                "seq": seq,
                "provider": provider,
                "body": body_text,
                "content_type": ct,
                "content_encoding": ce,
                "usage": _extract_usage(provider, ct, body_text),
            })

    def do_POST(self):
        try:
            self._do("POST")
        except Exception as e:  # ponytail: broad guard so one bad request doesn't kill the proxy thread
            if self.server.tape is not None and self.server.mode == "record":
                try:
                    self.server.tape.write_event({"kind": "run_aborted", "reason": f"upstream error: {e}"})
                except Exception:
                    pass
            self.send_error(502, f"upstream error: {e}")


class ModelProxy:
    def __init__(self, host, port, tape, upstreams, mode):
        self.host = host
        self.port = port
        self.tape = tape
        self.upstreams = upstreams
        self.mode = mode
        self._srv = None
        self._thread = None

    def start(self):
        srv = ThreadingHTTPServer((self.host, self.port), _Handler)
        srv.tape = self.tape
        srv.upstreams = self.upstreams
        srv.mode = self.mode
        srv._playback_consumed = 0
        srv._playback_failed = False
        self.port = srv.server_address[1]  # in case port was 0
        self._srv = srv
        self._thread = threading.Thread(target=srv.serve_forever, daemon=True)
        self._thread.start()

    def base_url(self):
        return f"http://{self.host}:{self.port}"

    def playback_failed(self):
        return bool(self._srv and self._srv._playback_failed)

    def stop(self):
        if self._srv:
            self._srv.shutdown()
            self._srv.server_close()
            self._srv = None


def _selfcheck() -> None:
    import tempfile, os, time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from .tape import Tape

    # fake upstream provider: returns a canned non-streaming Anthropic-style body
    class Up(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *a): pass
        def do_POST(self):
            body = b'{"id":"msg_1","content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":3,"output_tokens":1}}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    up = ThreadingHTTPServer(("127.0.0.1", 0), Up); up_port = up.server_address[1]
    threading.Thread(target=up.serve_forever, daemon=True).start()

    tmp = os.path.join(tempfile.mkdtemp(), "t.jsonl")
    tape = Tape(tmp, "record")
    mp = ModelProxy("127.0.0.1", 0, tape, {"anthropic": f"http://127.0.0.1:{up_port}", "openai": "http://127.0.0.1:0"}, "record")
    mp.start()
    # act as an agent: POST /v1/messages
    c = http.client.HTTPConnection("127.0.0.1", mp.port)
    c.request("POST", "/v1/messages", body=b'{"model":"claude-3","stream":false,"messages":[]}',
               headers={"Content-Type": "application/json"})
    r = c.getresponse()
    data = r.read().decode()
    assert r.status == 200 and "hi" in data, (r.status, data)
    c.close()
    mp.stop(); up.shutdown(); up.server_close()
    tape.close()
    # tape has model_request + model_response
    r2 = Tape(tmp, "replay")
    evs = r2.events()
    assert evs[0]["kind"] == "model_request" and evs[0]["provider"] == "anthropic"
    assert evs[1]["kind"] == "model_response" and evs[1]["seq"] == evs[0]["seq"]
    assert evs[1]["usage"] == {"input_tokens": 3, "output_tokens": 1}, evs[1]["usage"]
    print("model_proxy selfcheck OK")

    # playback: no upstream call, returns recorded body
    pb_tape = Tape(tmp, "replay")
    mp2 = ModelProxy("127.0.0.1", 0, pb_tape,
                     {"anthropic": "http://127.0.0.1:1", "openai": "http://127.0.0.1:1"}, "playback")
    mp2.start()
    c2 = http.client.HTTPConnection("127.0.0.1", mp2.port)
    c2.request("POST", "/v1/messages", body=b'{"model":"claude-3","stream":false,"messages":[]}',
               headers={"Content-Type": "application/json"})
    r3 = c2.getresponse(); d3 = r3.read().decode()
    assert r3.status == 200 and "hi" in d3, (r3.status, d3)
    # second request: queue exhausted -> 500
    c2.request("POST", "/v1/messages", body=b'{}', headers={"Content-Type": "application/json"})
    r4 = c2.getresponse(); d4 = r4.read().decode()
    assert r4.status == 500 and "playback exhausted" in d4, (r4.status, d4)
    assert mp2.playback_failed()
    c2.close(); mp2.stop()
    print("model_proxy playback selfcheck OK")


if __name__ == "__main__":
    _selfcheck()