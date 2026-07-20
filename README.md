# agent-vcr

Local, vendor-neutral **record/replay for AI agent runs**. agent-vcr sits as
two wire-level proxies between your agent and the outside world — one for the
model API, one for MCP tool servers — captures a run to a JSONL tape, and
replays it with tool outputs stubbed so you can reproduce bugs offline with no
side effects and no tool re-execution.

## The why

Agent bugs are hard to reproduce because a run touches a live model and live
tools. Re-running means re-paying for tokens, re-mutating state, and hoping the
model makes the same choices. agent-vcr records the wire traffic once, then
replays it deterministically: tools are stubbed from the tape (no server hits),
and optionally the model itself is too (`--playback`), so a flaky run becomes a
repeatable, offline, zero-cost fixture.

Zero agent code changes. Wiring is env vars + MCP-config rewrite.

## Install

```bash
pipx install .
```

(Python 3.11+. No runtime dependencies — stdlib only.)

## Usage

Record a Claude Code run:

```bash
agent-vcr record -- claude -p "refactor utils.py"
# -> tape written to ./tapes/<run-id>.jsonl
```

Replay it with tools stubbed (model still live):

```bash
agent-vcr replay -- claude -p "refactor utils.py"
```

Fully deterministic replay (no provider calls at all):

```bash
agent-vcr replay --playback -- claude -p "refactor utils.py"
```

Read tapes:

```bash
agent-vcr list
agent-vcr show ./tapes/<run-id>.jsonl
agent-vcr diff ./tapes/a.jsonl ./tapes/b.jsonl   # first diverging event
```

### Tool-miss behavior on replay

If the agent calls a tool with arguments not present in the tape:

- `--on-miss strict` (default) — exit nonzero, print the unmatched call. The
  agent diverged from the recorded run; this is a signal, not silently papered
  over.
- `--on-miss passthrough` — call the real server, append the new result to the
  tape, continue. The tape grows (marked `replay_extended`).

## How it works

- **Model proxy** — local HTTP server. `/v1/messages` (Anthropic) and
  `/v1/chat/completions` (OpenAI), including SSE. In record it forwards to the
  real provider and captures the raw response. In `--playback` it returns the
  recorded response and never touches the provider.
- **MCP proxy** — stdio (spawns the real MCP server, proxies JSON-RPC) and
  Streamable HTTP. Captures `tools/call` + `tool_result` keyed by
  `sha256(canonical_json(args))`. On replay, matches by `(server, tool, args_hash)`.
- **Tape** — one JSONL file per run, vendor-neutral envelope, monotonic `seq`.

## Supported agents (v1)

Claude Code first. Config-driven, not code-driven: any agent that honors
`*_BASE_URL` and reads an MCP config (`mcpServers` with stdio `{command,args}`
or HTTP `{url}` entries) works. agent-vcr rewrites the config in place
(backup at `<path>.vcr.bak`, restored on exit) and points `*_BASE_URL` at the
model proxy.

## Scope (v1)

In: `record`, `replay` (tool-stub + `--playback`), `list`, `show`, `diff`;
stdio + Streamable HTTP MCP; Claude Code config.

Out: non-MCP in-process tools, multi-agent orchestration, web UI, remote tape
store, cross-provider content normalization.

## Self-check

```bash
python selfcheck.py
```

MIT licensed.