#!/usr/bin/env python3
"""Real Hermes ACP peer with a local model fixture; no paid inference.

Usage: python3 rig-acp/scripts/hermes_smoke.py /tmp/hermes-acp-smoke
Build `cargo build -p rig-acp --example acp_peer` first.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

repo = Path(__file__).resolve().parents[2]
hermes = Path(sys.argv[1]).resolve()
receipts = []

class Model(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.reply({"object": "list", "data": [{"id": "fixture-model", "object": "model"}]})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path != "/v1/chat/completions" or "messages" not in body:
            self.send_error(404, "fixture supports only chat completions")
            return
        # Keep only synthetic user inputs, never system prompts or headers.
        users = [m["content"] for m in body["messages"] if m["role"] == "user"]
        receipts.append({"request": len(receipts) + 1, "path": self.path, "users": users})
        message = {"role": "assistant", "content": "Local fixture reply."}
        if body.get("stream"):
            chunk = {"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                     "model": "fixture-model", "choices": [{"index": 0, "delta": message, "finish_reason": "stop"}]}
            self.reply_bytes(("data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n").encode(), "text/event-stream")
        else:
            self.reply({"id": "fixture", "object": "chat.completion", "created": 1,
                        "model": "fixture-model", "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}})

    def reply(self, body):
        self.reply_bytes(json.dumps(body).encode(), "application/json")

    def reply_bytes(self, body, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

server = ThreadingHTTPServer(("127.0.0.1", 0), Model)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    with tempfile.TemporaryDirectory(prefix="rig-acp-hermes-") as home:
        config = Path(home) / ".hermes"
        config.mkdir()
        (config / "config.yaml").write_text(
            "model:\n  provider: custom\n  default: fixture-model\n"
            f"  base_url: http://127.0.0.1:{server.server_port}/v1\n"
            "  api_key: local-fixture-key\n  api_mode: chat_completions\n"
            "auxiliary:\n  title_generation:\n    enabled: false\n"
            "mcp_servers: {}\nmemory:\n  memory_enabled: false\n  user_profile_enabled: false\n"
            "agent:\n  max_iterations: 1\n  disabled_toolsets: [all]\n"
        )
        env = {k: os.environ[k] for k in ("PATH", "LANG", "TZ") if k in os.environ}
        env.update(HOME=home, HERMES_HOME=str(config), PYTHONPATH=str(hermes),
                   HERMES_ACP_SKIP_CONFIGURED_MCP="1", OPENAI_API_KEY="local-fixture-key",
                   OPENAI_BASE_URL=f"http://127.0.0.1:{server.server_port}/v1",
                   ACP_SMOKE_TEXT="Reply with a short greeting; do not use tools.",
                   ACP_EXPECT_TEXT="Local fixture reply.")
        binary = repo / "target/debug/examples/acp_peer"
        completed = subprocess.run([str(binary), str(hermes / ".venv/bin/python"), "-m", "acp_adapter"],
                                   cwd=home, env=env, text=True, capture_output=True, timeout=100)
        print(completed.stdout, end="")
        print("Hermes stderr lines:", len(completed.stderr.splitlines()))
        print("Hermes source:", subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=hermes, text=True).strip())
        print("model fixture receipts:", json.dumps(receipts))
        print("peer exit:", completed.returncode)
        if completed.returncode:
            # The isolated environment contains only the dummy model credential.
            print(completed.stderr.replace("local-fixture-key", "[fixture credential]"))
        assert completed.returncode == 0
        assert [len(r["users"]) for r in receipts] == [1, 2, 1], receipts
        prompt = "Reply with a short greeting; do not use tools."
        assert [r["users"] for r in receipts] == [
            [f"A1: {prompt}"], [f"A1: {prompt}", f"A2: {prompt}"], [f"B1: {prompt}"]
        ], receipts
        print("PASS: real Hermes ACP + Rig agent; exact model histories [A1], [A1, A2], [B1]")
finally:
    server.shutdown()
    server.server_close()
    thread.join()
