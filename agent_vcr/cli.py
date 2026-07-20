import argparse
import os
import sys
import uuid
from datetime import datetime

from .tape import Tape, args_hash, canonical_json
from .model_proxy import ModelProxy
from .mcp_proxy import HttpMcpProxy, StdioMcpProxy
from .wiring import AgentRunner, _vcr_bin

TAPES_DIR = "tapes"


def _new_tape_path() -> str:
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    return os.path.join(TAPES_DIR, run_id + ".jsonl")


def _latest_tape() -> str:
    if not os.path.isdir(TAPES_DIR):
        sys.exit(f"no tapes dir at ./{TAPES_DIR}")
    files = sorted(f for f in os.listdir(TAPES_DIR) if f.endswith(".jsonl"))
    if not files:
        sys.exit(f"no tapes in ./{TAPES_DIR}")
    return os.path.join(TAPES_DIR, files[-1])


def _upstreams_from_env() -> dict:
    return {
        "anthropic": os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com"),
        "openai": os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
    }


def _build_env(tape_path, mode, on_miss, model_base, mcp_http_base):
    env = dict(os.environ)
    env["ANTHROPIC_BASE_URL"] = model_base
    env["OPENAI_BASE_URL"] = model_base
    env["AGENT_VCR_MODE"] = mode
    env["AGENT_VCR_TAPE"] = tape_path
    env["AGENT_VCR_ON_MISS"] = on_miss
    env["AGENT_VCR_MCP_HTTP_BASE"] = mcp_http_base
    return env


def cmd_record(args) -> int:
    tape_path = args.tape or _new_tape_path()
    tape = Tape(tape_path, "record")
    model = ModelProxy("127.0.0.1", 0, tape, _upstreams_from_env(), "record")
    model.start()
    mcp = HttpMcpProxy("127.0.0.1", 0, tape, "record", "strict", {})
    mcp.start()
    # ponytail: HTTP upstreams are not known until we read the agent's mcp config; populate below.
    env = _build_env(tape_path, "record", "strict", model.base_url(), mcp.url_for(""))
    # populate HTTP upstreams from the real config before the agent hits them
    _populate_http_upstreams(mcp, args.mcp_config)
    runner = AgentRunner(args.agent_cmd, args.mcp_config, env, mcp.url_for(""), _vcr_bin())
    rc = runner.run()
    tape.close()
    print(f"agent-vcr: tape written to {tape_path}", file=sys.stderr)
    return rc


def _populate_http_upstreams(mcp, config_path):
    if not config_path or not os.path.exists(config_path):
        return
    import json
    with open(config_path) as f:
        cfg = json.load(f)
    for name, entry in (cfg.get("mcpServers") or {}).items():
        if isinstance(entry, dict) and "url" in entry and "command" not in entry:
            mcp.upstreams[name] = entry["url"]


def cmd_replay(args) -> int:
    tape_path = args.tape or _latest_tape()
    mode = "playback" if args.playback else "replay"
    tape = Tape(tape_path, "replay")
    model = ModelProxy("127.0.0.1", 0, tape, _upstreams_from_env(), mode)
    model.start()
    mcp = HttpMcpProxy("127.0.0.1", 0, tape, mode, args.on_miss, {})
    mcp.start()
    _populate_http_upstreams(mcp, args.mcp_config)
    env = _build_env(tape_path, mode, args.on_miss, model.base_url(), mcp.url_for(""))
    runner = AgentRunner(args.agent_cmd, args.mcp_config, env, mcp.url_for(""), _vcr_bin())
    rc = runner.run()
    tape.close()
    print(f"agent-vcr: replay done (exit={rc})", file=sys.stderr)
    return rc


def cmd_mcp_stdio(args) -> int:
    env = os.environ
    mode = env.get("AGENT_VCR_MODE", args.mode)
    tape_path = env.get("AGENT_VCR_TAPE", args.tape)
    on_miss = env.get("AGENT_VCR_ON_MISS", args.on_miss)
    tape = Tape(tape_path, "record" if mode == "record" else "replay")
    proxy = StdioMcpProxy(args.server, tape, mode, on_miss, args.real_cmd)
    rc = proxy.run()
    tape.close()
    return rc


def cmd_list(args) -> int:
    if not os.path.isdir(TAPES_DIR):
        return 0
    for name in sorted(os.listdir(TAPES_DIR)):
        if not name.endswith(".jsonl"):
            continue
        path = os.path.join(TAPES_DIR, name)
        try:
            t = Tape(path, "replay")
        except Exception as e:
            print(f"{name}\tSKIPPED ({e})", file=sys.stderr)
            continue
        evs = t.events()
        provider = next((e.get("provider") for e in evs if e.get("kind") == "model_request"), "-")
        model = "-"
        for e in evs:
            if e.get("kind") == "model_request":
                model = (e.get("body") or {}).get("model", "-")
                break
        tools = sum(1 for e in evs if e.get("kind") == "tool_call")
        mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
        print(f"{name}\tevents={len(evs)}\tprovider={provider}\tmodel={model}\ttools={tools}\t{mtime}")
    return 0


def cmd_show(args) -> int:
    t = Tape(args.tape, "replay")
    for ev in t.events():
        seq = ev.get("seq", "-")
        kind = ev.get("kind")
        print(f"--- seq={seq} kind={kind} ---")
        print(_pretty(ev))
    return 0


def _pretty(ev: dict) -> str:
    import json
    keep = {k: v for k, v in ev.items() if k not in ("body",)}
    s = json.dumps(keep, indent=2, ensure_ascii=False)
    if "body" in ev:
        s += "\nbody:\n" + _pretty_body(ev["body"])
    return s


def _pretty_body(body) -> str:
    import json
    if isinstance(body, str):
        try:
            return json.dumps(json.loads(body), indent=2, ensure_ascii=False)
        except Exception:
            return body
    return json.dumps(body, indent=2, ensure_ascii=False)


def cmd_diff(args) -> int:
    a = Tape(args.tape_a, "replay").events()
    b = Tape(args.tape_b, "replay").events()
    for i in range(max(len(a), len(b))):
        ea = a[i] if i < len(a) else None
        eb = b[i] if i < len(b) else None
        if ea != eb:
            print(f"first divergence at index {i}:")
            print("A:", _pretty(ea) if ea else "<none>")
            print("B:", _pretty(eb) if eb else "<none>")
            return 1
    print("tapes identical")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="agent-vcr", description="Record/replay AI agent runs.")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("record", help="record an agent run to a tape")
    pr.add_argument("--mcp-config", default=".mcp.json")
    pr.add_argument("--tape", default=None)
    pr.add_argument("agent_cmd", nargs=argparse.REMAINDER,
                    help='-- <agent command>  (e.g. -- claude -p "...")')
    pr.set_defaults(func=cmd_record)

    pl = sub.add_parser("replay", help="replay a recorded run (tool-stub by default; --playback for full determinism)")
    pl.add_argument("--playback", action="store_true")
    pl.add_argument("--on-miss", choices=["strict", "passthrough"], default="strict")
    pl.add_argument("--mcp-config", default=".mcp.json")
    pl.add_argument("--tape", default=None)
    pl.add_argument("agent_cmd", nargs=argparse.REMAINDER)
    pl.set_defaults(func=cmd_replay)

    ms = sub.add_parser("mcp-stdio", help="(internal) proxy a stdio MCP server")
    ms.add_argument("--server", required=True)
    ms.add_argument("--tape", default=None, help="tape path; defaults to AGENT_VCR_TAPE env")
    ms.add_argument("--mode", choices=["record", "replay", "playback"], default="record")
    ms.add_argument("--on-miss", choices=["strict", "passthrough"], default="strict")
    ms.add_argument("real_cmd", nargs=argparse.REMAINDER, help="-- <real mcp server command>")
    ms.set_defaults(func=cmd_mcp_stdio)

    sub.add_parser("list", help="list tapes").set_defaults(func=cmd_list)
    sh = sub.add_parser("show", help="pretty-print a tape")
    sh.add_argument("tape")
    sh.set_defaults(func=cmd_show)
    df = sub.add_parser("diff", help="show first diverging event between two tapes")
    df.add_argument("tape_a")
    df.add_argument("tape_b")
    df.set_defaults(func=cmd_diff)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    # strip a leading "--" from REMAINDER args
    for attr in ("agent_cmd", "real_cmd"):
        if hasattr(args, attr) and getattr(args, attr) and getattr(args, attr)[0] == "--":
            setattr(args, attr, getattr(args, attr)[1:])
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())