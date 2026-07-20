# agent-vcr — Design

**Date:** 2026-07-20
**Status:** Approved (pre-implementation)

## One-liner

A local, vendor-neutral record/replay tool for AI agent runs. It sits as two
wire-level proxies between the agent and the outside world — one for the model
API, one for MCP tool servers — captures a run to a JSONL tape, and replays it
with tool outputs stubbed so you can reproduce bugs offline with no side
effects and no tool re-execution.

## Goals

- Reproduce an agent bug from a recorded run without re-hitting external
  tools, APIs, or (optionally) the model.
- Zero agent code changes for any agent that uses MCP tools and a standard
  model API (Anthropic Messages or OpenAI Chat Completions).
- Fully local/offline. No hosted backend, no API keys shipped, no telemetry.
- Small, sharp, single-purpose. Python, installable via pipx.

## Non-goals (v1)

- Multi-agent orchestration or fan-out.
- Web UI / dashboard.
- Remote or shared tape store.
- Non-MCP in-process tool interception.
- Cross-provider normalization of message *content* (we preserve raw bodies).
- Diffing transcripts from different providers semantically.

## Architecture

Two local proxies driven by one CLI. Both proxies are wire-level and
format-preserving: they forward bytes they don't understand and only
introspect the parts needed to record/replay.

### 1. Model proxy

Local HTTP server. Two route families, both incl. SSE streaming:

- `/v1/chat/completions` — OpenAI Chat Completions (request in, streaming or
  non-streaming response out).
- `/v1/messages` — Anthropic Messages (same).

Modes:

- **record** — forward to the real provider (`ANTHROPIC_BASE_URL` /
  `OPENAI_BASE_URL` env holds the upstream), reassemble the streamed response
  into a complete body, write `model_request` + `model_response` events to the
  tape.
- **replay** (default) — forward to the real provider. The model is live; only
  tools are stubbed (by the MCP proxy). This is "tool-stub replay."
- **replay --playback** — return the recorded `model_response` for the
  matching `seq` and never call the provider. Deterministic, zero cost.

### 2. MCP proxy

Sits between agent and MCP server(s). Transports supported in v1:

- **stdio** — agent-vcr spawns the real MCP server as a subprocess and proxies
  JSON-RPC over its stdio.
- **Streamable HTTP** — agent-vcr proxies HTTP/SSE MCP transports.

For each `tools/call` request:

- **record** — forward to the real server, capture `tool_call` +
  `tool_result` events (args, result, `args_hash`).
- **replay** — match by `(server, tool, args_hash)`. On hit: return recorded
  `tool_result`, never touch the real server. On miss: behavior set by
  `--on-miss`:
  - `strict` (default for replay) — exit nonzero, print the unmatched call.
    Signals that the agent diverged from the recorded run.
  - `passthrough` — call the real server, record the new result, continue.

### 3. Tape format

One JSONL file per run at `./tapes/<run-id>.jsonl`. One event per line,
vendor-neutral envelope:

```json
{"kind":"model_request","seq":1,"provider":"anthropic","body":{...}}
{"kind":"model_response","seq":1,"provider":"anthropic","body":{...},"usage":{...}}
{"kind":"tool_call","seq":2,"server":"fs","tool":"read_file","args":{...},"args_hash":"sha256:..."}
{"kind":"tool_result","seq":2,"result":{...}}
{"kind":"run_aborted","reason":"..."}
```

- `seq` is a monotonic per-run counter linking request↔response and
  tool_call↔result.
- `args_hash` = `sha256(canonical_json(args))`. Canonical JSON = sorted keys,
  no whitespace. This is the replay match key for tools.
- `provider` is `anthropic` | `openai` (other OpenAI-compatible providers
  ride the `openai` path and are tagged by their base URL in a `provider_url`
  field on the event).

### 4. CLI

```
agent-vcr record -- <agent command>           # wraps agent with rewritten env
agent-vcr replay <tape> [--playback] [--on-miss strict|passthrough]
agent-vcr list
agent-vcr show <tape>
agent-vcr diff <tape-a> <tape-b>
```

- `record` sets `ANTHROPIC_BASE_URL` / `OPENAI_BASE_URL` to the model proxy
  and rewrites the agent's MCP config to point at the MCP proxy, then spawns
  the agent. Tape written to `./tapes/<run-id>.jsonl`.
- `list` — enumerates tapes with run-id, event count, provider, model, tool
  count, recorded-at.
- `show <tape>` — pretty-prints events in order.
- `diff <a> <b>` — aligns two tapes by `seq` and prints the first diverging
  event (where did two runs split).

## Wiring (zero agent code changes)

`agent-vcr record -- claude -p "..."` does, effectively:

1. Start model proxy on `127.0.0.1:<port>`.
2. Start MCP proxy; for each MCP server in the agent's config, register an
   upstream and expose a proxied endpoint/stdio.
3. Set env: `ANTHROPIC_BASE_URL=http://127.0.0.1:<port>` (and/or
   `OPENAI_BASE_URL`), pass through `ANTHROPIC_API_KEY` etc.
4. Rewrite the agent's MCP config (e.g. `.claude.json` / `mcp.json`) so server
   entries point at the MCP proxy instead of their real URLs/commands.
5. Spawn the agent. On exit, finalize the tape.

Supported agents in v1: any that honor `*_BASE_URL` and use MCP — Claude Code,
Cursor, Codex-with-MCP, custom agents following the same conventions. Agent
support is config-driven, not code-driven.

## Data flow

- **Record**
  - agent → model proxy → provider; response captured.
  - agent → MCP proxy → MCP server; call + result captured.
- **Replay (tool-stub, default)**
  - agent → model proxy → provider (live).
  - agent → MCP proxy → recorded result (no server hit).
- **Replay (--playback)**
  - agent → model proxy → recorded response (no provider).
  - agent → MCP proxy → recorded result (no server hit).

## Error handling

- Provider unreachable in **record** → fail loud; keep partial tape with a
  `run_aborted` tail event.
- MCP server unreachable in **record** → same.
- Replay miss with `--on-miss strict` → exit nonzero, print unmatched call.
- Replay miss with `--on-miss passthrough` → call live, record new event,
  continue. Tape grows during replay in this mode (marked as replay-extended).
- Tape file truncated / bad JSONL line → `replay` refuses and points at the
  bad line; `show` reads up to the bad line and reports it; `list` skips the
  tape with a warning.
- `--playback` but no recorded `model_response` for the next `seq` → exit
  nonzero with the missing `seq`.

## Testing

One runnable self-check, no test framework: `selfcheck.py`.

- Spins up a fake provider HTTP endpoint returning a canned Anthropic
  `tool_use` response (then a canned final text response).
- Spins up a fake MCP server (in-process, stdio) that echoes args as its
  result.
- Runs a scripted "agent" (a small coroutine in the same file) that talks to
  the model proxy and MCP proxy in both record and replay modes.
- Asserts: recorded `tool_result` equals replayed `tool_result`;
  `--playback` makes zero calls to the fake provider (verified by a call
  counter); `strict` miss exits nonzero.

Run: `python selfcheck.py`. Exits nonzero on failure. This is the only test
shipped in v1; everything else is exercised by it.

## Scope / YAGNI notes

- v1 ships `record`, `replay`, `list`, `show`, `diff`. If `diff`/`list`/`show`
  feel like scope creep during implementation, `record` + `replay` alone are
  the minimum usable product — the others are pure readers over the tape and
  can land in a follow-up PR.
- No plugin system, no config DSL. Behavior flags are CLI args only.
- No tape compression / rotation. Tapes are plain JSONL; bring your own
  `gzip` if you care.

## Open questions to resolve during planning

- Exact MCP-config rewrite strategy per supported agent (Claude Code's
  `.claude.json` shape, Cursor's `~/.cursor/mcp.json`, ad-hoc). v1 will
  implement Claude Code first; others documented as config snippets.
- Whether `diff` aligns by `seq` alone or by `(kind, seq)` — defaulting to
  `(kind, seq)`.