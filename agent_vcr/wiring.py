import json
import os
import shutil
import subprocess
import sys


def _vcr_bin() -> list[str]:
    # ponytail: prefer `agent-vcr` on PATH; fall back to `python -m agent_vcr.cli`. Upgrade: resolve via sys.executable only.
    if shutil.which("agent-vcr"):
        return ["agent-vcr"]
    return [sys.executable, "-m", "agent_vcr.cli"]


def rewrite_mcp_config(config_path: str, mcp_http_base: str, vcr_bin: list[str] | None = None) -> dict:
    vcr_bin = vcr_bin or _vcr_bin()
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    servers = cfg.get("mcpServers", {})
    new_servers = {}
    for name, entry in servers.items():
        if isinstance(entry, dict) and "url" in entry and "command" not in entry:
            # Streamable HTTP server
            new_servers[name] = {"url": f"{mcp_http_base}/{name}", "type": "http"}
            for k, v in entry.items():
                if k not in ("url", "command", "args", "env", "type"):
                    new_servers[name][k] = v
        else:
            # stdio server
            cmd = entry.get("command")
            args = entry.get("args", []) or []
            env = dict(entry.get("env") or {})
            new_servers[name] = {
                "command": vcr_bin[0],
                "args": [*vcr_bin[1:], "mcp-stdio", "--server", name, "--", cmd, *args],
                "env": env,
                # ponytail: mode/on-miss/tape are read from AGENT_VCR_* env at spawn time,
                # set by the CLI in AgentRunner.env. Keeps config rewrite mode-agnostic.
            }
    return {**cfg, "mcpServers": new_servers}


class AgentRunner:
    def __init__(self, agent_cmd, mcp_config_path, env, mcp_http_base, vcr_bin=None):
        self.agent_cmd = agent_cmd
        self.mcp_config_path = mcp_config_path
        self.env = env
        self.mcp_http_base = mcp_http_base
        self.vcr_bin = vcr_bin or _vcr_bin()

    def run(self) -> int:
        bak = self.mcp_config_path + ".vcr.bak"
        # ponytail: in-place rewrite + backup/restore. Upgrade: write a sidecar config + agent flag if the agent supports --mcp-config.
        if os.path.exists(self.mcp_config_path):
            shutil.copy2(self.mcp_config_path, bak)
        try:
            new_cfg = rewrite_mcp_config(self.mcp_config_path, self.mcp_http_base, self.vcr_bin)
            with open(self.mcp_config_path, "w", encoding="utf-8") as f:
                json.dump(new_cfg, f, indent=2)
            proc = subprocess.run(self.agent_cmd, env=self.env)
            return proc.returncode
        finally:
            if os.path.exists(bak):
                shutil.move(bak, self.mcp_config_path)


def _selfcheck() -> None:
    import tempfile, os
    d = tempfile.mkdtemp()
    cfg = os.path.join(d, "mcp.json")
    with open(cfg, "w") as f:
        json.dump({"mcpServers": {
            "fs": {"command": "npx", "args": ["-y", "fs-mcp", "/tmp"]},
            "http1": {"url": "https://example.com/mcp", "type": "http"},
        }}, f)
    new = rewrite_mcp_config(cfg, "http://127.0.0.1:9999", vcr_bin=["agent-vcr"])
    s = new["mcpServers"]
    assert s["fs"]["command"] == "agent-vcr"
    assert s["fs"]["args"] == ["mcp-stdio", "--server", "fs", "--", "npx", "-y", "fs-mcp", "/tmp"], s["fs"]["args"]
    assert s["http1"]["url"] == "http://127.0.0.1:9999/http1"
    assert s["http1"]["type"] == "http"
    # backup/restore round-trip
    runner = AgentRunner(["python", "-c", "import sys; sys.exit(0)"], cfg, dict(os.environ),
                         "http://127.0.0.1:9999", vcr_bin=["agent-vcr"])
    rc = runner.run()
    assert rc == 0
    with open(cfg) as f:
        restored = json.load(f)
    assert "npx" in restored["mcpServers"]["fs"]["command"], "config not restored"
    print("wiring selfcheck OK")


if __name__ == "__main__":
    _selfcheck()