#!/usr/bin/env python3
"""Live Hermes/Ollama proof; no fixture answers or model downloads.

Build acp_live with --features rivet. Run from the repository root:
python3 rig-acp/scripts/ollama_live.py HERMES_CHECKOUT text|tools|absent|unavailable [RIVET_BINARY]
Only synthetic inputs are supported. Local terminal execution is not a sandbox.
"""
import ctypes
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
MODEL = "qwen3.5:9b"
DIGEST = "56671c2ab9385f9cfcb404638e32cd62d88e3501d44822208363c010179a3c90"
OLLAMA = "http://127.0.0.1:11434"
LOCK = threading.Lock()


def record(destination, **data):
    with LOCK, Path(destination).open("a") as out:
        out.write(json.dumps({"time": time.time(), **data}) + "\n")


def get(url, token=None):
    headers = {"Authorization": "Bearer " + token} if token else {}
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=10) as response:
        return json.load(response)


def owned_processes():
    """Return this Linux supervisor's descendants, including adopted orphans."""
    processes = {}
    for path in Path("/proc").iterdir():
        if path.name.isdecimal():
            try:
                fields = (path / "stat").read_text().rsplit(") ", 1)[1].split()
                processes[int(path.name)] = (int(fields[1]), fields[19])
            except (OSError, IndexError):
                pass
    owned = {os.getpid()}
    while True:
        found = {pid for pid, (parent, _) in processes.items() if parent in owned}
        if found <= owned:
            return {pid: processes[pid] for pid in owned if pid != os.getpid()}
        owned |= found


def stop_owned_processes(events):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        deadline = time.monotonic() + 5
        signalled = set()
        while time.monotonic() < deadline:
            children = owned_processes()
            for pid, identity in children.items():
                if (pid, identity) not in signalled:
                    record(events, event="cleanup_signal", pid=pid, identity=identity, signal=sig)
                    try:
                        descriptor = os.pidfd_open(pid)
                        try:
                            current = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
                            if current[19] == identity[1]:
                                signal.pidfd_send_signal(descriptor, sig)
                        finally:
                            os.close(descriptor)
                    except (ProcessLookupError, FileNotFoundError):
                        pass
                    signalled.add((pid, identity))
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                    record(events, event="reaped", pid=pid, code=os.waitstatus_to_exitcode(status))
                except ChildProcessError:
                    break
            if not owned_processes():
                return
            time.sleep(.05)
    raise RuntimeError(f"owned processes remain: {owned_processes()}")


def verify_delivery(evidence):
    """Require complete model output for every positive turn, including recovery."""
    events = [json.loads(line) for line in (evidence / "events.jsonl").read_text().splitlines()]
    wire = [r["value"] for r in map(json.loads, (evidence / "wire.jsonl").read_text().splitlines()) if r["event"] == "frame"]
    clients = [json.loads(line) for line in (evidence / "client.jsonl").read_text().splitlines()]
    plan = json.loads((evidence / "plan.json").read_text())
    requests = [r for r in events if r["event"] == "model_request"]
    responses = [r for r in events if r["event"] == "model_response"]
    model_text = {}
    for request in requests:
        rows = [r for r in responses if r["request"] == request["request"]]
        assert rows and all(r["status"] == 200 for r in rows), "missing successful response"
        assert sum(r["data"].strip() == "data: [DONE]" for r in rows) == 1, "incomplete model stream"
        assert any(r["event"] == "model_response_end" and r["request"] == request["request"] for r in events), "missing stream EOF"
        text = ""
        for row in rows:
            line = row["data"].strip()
            if line.startswith("data: ") and line != "data: [DONE]":
                for choice in json.loads(line[6:]).get("choices", []):
                    text += choice.get("delta", {}).get("content") or ""
        model_text[request["request"]] = text
    prompts = [f for f in wire if f.get("method") == "session/prompt"]
    for action, prompt in zip([a for a in plan if not a.get("bad_route")], prompts, strict=True):
        if action.get("cancel_started"):
            continue
        matching = [r for r in requests if action["prompt"] in r["users"][-1]]
        assert matching, "no correlated model request"
        generated = "".join(model_text[r["request"]] for r in matching)
        assert generated.strip(), "no model-generated content"
        end = next(i for i, f in enumerate(wire) if f.get("id") == prompt["id"] and "result" in f)
        updates = wire[wire.index(prompt) + 1:end]
        acp = "".join(f["params"]["update"]["content"]["text"] for f in updates
                      if f.get("method") == "session/update" and f["params"]["sessionId"] == prompt["params"]["sessionId"]
                      and f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        assert acp in (generated, "\n\n" + generated), "model/ACP text differs"
        results = [r for r in clients if r["event"] == "result" and r["label"] == action["label"]]
        assert len(results) == 1 and results[0]["ok"] and results[0]["text"] == acp, "ACP/Rig text differs"


def wire():
    """Copy exact newline-delimited ACP frames; separately record synthetic wire data."""
    log = os.environ["ACP_WIRE_LOG"]
    child = subprocess.Popen([os.environ["HERMES_PYTHON"], "-m", "acp_adapter"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    record(log, event="spawn", pid=child.pid, relay_pid=os.getpid(),
           process_stat=Path(f"/proc/{child.pid}/stat").read_text())

    def copy(source, target, direction):
        for line in iter(source.readline, b""):
            record(log, event="frame", direction=direction, pid=child.pid, value=json.loads(line))
            target.write(line)
            target.flush()
        if direction == "to_agent":
            target.close()

    threading.Thread(target=copy, args=(sys.stdin.buffer, child.stdin, "to_agent"), daemon=True).start()
    try:
        copy(child.stdout, sys.stdout.buffer, "from_agent")
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=10)
        record(log, event="exit", pid=child.pid, code=child.returncode)


def run():
    # Adopt this process's orphaned descendants without changing host services.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "cannot supervise child processes")
    hermes = Path(sys.argv[1]).resolve()
    assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=hermes, text=True).strip() == "952322ca3cf23435af4f95c2b91216d9e3658b84"
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=hermes, text=True).strip()
    mode = sys.argv[2]
    assert mode in ("text", "tools", "absent", "unavailable", "interrupt")
    use_tools = mode in ("tools", "interrupt")
    rivet = Path(sys.argv[3]).resolve() if len(sys.argv) > 3 else None
    transport = "rivet" if rivet else "stdio"
    nonce = uuid.uuid4().hex[:12]
    evidence = ROOT / ".amp/in/artifacts" / f"ollama-{transport}-{mode}-{nonce}"
    evidence.mkdir(parents=True)
    print(evidence, flush=True)
    for source in (Path(__file__), ROOT / "rig-acp/examples/acp_live.rs"):
        (evidence / source.name).write_bytes(source.read_bytes())
    events = evidence / "events.jsonl"
    requests = []
    record(events, event="provenance", source=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
           dirty=subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
           lock_sha256=hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
           hermes=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=hermes, text=True).strip(),
           hermes_dirty=subprocess.check_output(["git", "status", "--porcelain"], cwd=hermes, text=True),
           hermes_lock_sha256=hashlib.sha256((hermes / "uv.lock").read_bytes()).hexdigest(),
           script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
           binary_sha256=hashlib.sha256((ROOT / "target/debug/examples/acp_live").read_bytes()).hexdigest(),
           source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (ROOT / "rig-acp").rglob("*") if p.is_file() and "__pycache__" not in str(p)},
           command=sys.argv, ollama=get(OLLAMA + "/api/version"), tags=get(OLLAMA + "/api/tags"))
    assert any(m["name"] == MODEL and m["digest"] == DIGEST for m in get(OLLAMA + "/api/tags")["models"])

    class Relay(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            connection = http.client.HTTPConnection("127.0.0.1", 11434, timeout=30)
            connection.request("GET", self.path)
            response = connection.getresponse()
            body = response.read()
            record(events, event="model_get", path=self.path, status=response.status)
            self.send_response(response.status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            connection.close()

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            body = json.loads(raw)
            if self.path == "/api/show":
                connection = http.client.HTTPConnection("127.0.0.1", 11434, timeout=30)
                connection.request("POST", self.path, raw, {"Content-Type": "application/json"})
                response = connection.getresponse()
                data = response.read()
                record(events, event="model_metadata", path=self.path, status=response.status,
                       request=body, sha256=hashlib.sha256(data).hexdigest())
                self.send_response(response.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                connection.close()
                return
            index = len(requests) + 1
            entry = {"request": index, "path": self.path, "model": body.get("model"),
                     "users": [m["content"] for m in body.get("messages", []) if m["role"] == "user"],
                     "tool_results": [m for m in body.get("messages", []) if m["role"] == "tool"],
                     "tools": [t["function"]["name"] for t in body.get("tools", [])],
                     "options": {k: v for k, v in body.items() if k not in ("messages", "tools")},
                     "body_sha256": hashlib.sha256(raw).hexdigest()}
            requests.append(entry)
            record(events, event="model_request", **entry)
            if self.path != "/v1/chat/completions" or index > 30:
                self.send_error(400, "unexpected route or request budget")
                return
            if mode == "unavailable":
                # This private relay fails; the shared daemon remains untouched.
                self.send_error(503, "synthetic upstream unavailable")
                record(events, event="upstream_unavailable", request=index)
                return
            connection = http.client.HTTPConnection("127.0.0.1", 11434, timeout=180)
            try:
                connection.request("POST", self.path, raw, {"Content-Type": "application/json"})
                upstream = list(connection.sock.getpeername())
                response = connection.getresponse()
                self.send_response(response.status)
                self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
                self.send_header("Connection", "close")
                self.end_headers()
                digest = hashlib.sha256()
                for line in iter(response.readline, b""):
                    digest.update(line)
                    record(events, event="model_response", request=index, status=response.status,
                           upstream=upstream, data=line.decode())
                    self.wfile.write(line)
                    self.wfile.flush()
                record(events, event="model_response_end", request=index, sha256=digest.hexdigest())
                self.close_connection = True
            finally:
                connection.close()

    relay = ThreadingHTTPServer(("127.0.0.1", 0), Relay)
    relay.daemon_threads = True
    thread = threading.Thread(target=relay.serve_forever, daemon=True)
    thread.start()
    service = None
    client = None
    home = None
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix="rig-acp-ollama-", delete=False) as disposable:
            home = Path(disposable)
            config = home / ".hermes"
            config.mkdir()
            model = "rig-acp-absent-" + nonce if mode == "absent" else MODEL
            configuration = {
                "model": {"default": model, "provider": "custom:local-proof", "ollama_num_ctx": 65536},
                "providers": {"local-proof": {"base_url": f"http://127.0.0.1:{relay.server_port}/v1",
                    "api_key": "no-key-required", "api_mode": "chat_completions",
                    "extra_body": {"temperature": 0, "max_tokens": 512, "seed": 42,
                                   "think": False, "reasoning_effort": "none"}}},
                "platform_toolsets": {"acp": ["terminal", "no_mcp"] if use_tools else []},
                "tools": {"tool_search": {"enabled": "off"}},
                "agent": {"disabled_toolsets": [] if use_tools else ["all"], "reasoning_effort": "none"},
                "terminal": {"backend": "local", "home_mode": "profile", "timeout": 60},
                "approvals": {"mode": "manual", "timeout": 30},
                "auxiliary": {"title_generation": {"enabled": False}, "background_review": {"enabled": False}},
                "mcp_servers": {}, "memory": {"memory_enabled": False, "user_profile_enabled": False},
            }
            (config / "config.yaml").write_text(json.dumps(configuration))
            record(events, event="configuration", config=configuration, cwd=disposable)
            env = {k: os.environ[k] for k in ("PATH", "LANG", "TZ") if k in os.environ}
            env.update(HOME=disposable, HERMES_HOME=str(config), PYTHONPATH=str(hermes),
                       HERMES_ACP_SKIP_CONFIGURED_MCP="1", HERMES_PYTHON=str(hermes / ".venv/bin/python"),
                       ACP_WIRE_LOG=str(evidence / "wire.jsonl"), XDG_DATA_HOME=disposable)
            def action(label, session, prompt, **kwargs):
                return dict(label=label, session=session, prompt=f"{nonce}/{label}: {prompt}", **kwargs)
            if mode == "text":
                plan = [action("A1", "A", "Reply with a short greeting. Do not use tools."),
                        action("invalid", "A", "Do not execute this prompt.", bad_route=True),
                        action("A2", "A", "Reply with a different short greeting. Do not use tools."),
                        action("B1", "B", "Reply with a short greeting. Do not use tools.")]
            elif use_tools:
                for name in ("allow-probe", "deny-probe", "deny-control"):
                    (home / name).write_text("disposable-probe\n")
                plan = []
                for label, session, target, allow in [("allow", "P", "allow-probe", True),
                        ("deny", "P", "deny-probe", False), ("deny-control", "D", "deny-control", True)]:
                    command = f"rm -r -- ./{target}"
                    plan.append(action(label, session,
                        f"Call terminal exactly once with command `{command}`. This removes one disposable test file. "
                        "Do not change the command or use other tools. If permission is denied, stop without retrying. Then reply briefly.",
                        command=command, target=target, allow=allow))
                for label, session in [("natural", "N"), ("cancel", "C")]:
                    command = f"echo $$ > {label}-started; sleep 8; printf finished > {label}-finished"
                    plan.append(action(label, session,
                        f"Call terminal exactly once with command `{command}`, background=false, timeout=60. "
                        "Wait for foreground completion. Do not change the command or use other tools. Then reply briefly.",
                        command=command,
                        **({"cancel_started": "cancel-started"} if label == "cancel" else {})))
                plan.append(action("recovery", "C", "Reply with a short greeting. Do not use any tools."))
            else:
                plan = [action(mode, "F", "Reply with a short greeting. Do not use tools.", failure_control=True)]
            plan_file = evidence / "plan.json"
            plan_file.write_text(json.dumps(plan, indent=2))
            env["ACP_LIVE_PLAN"] = str(plan_file)
            if rivet:
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", 0))
                    port = sock.getsockname()[1]
                launcher = home / "sandbox-agent/bin/agent_processes/mock-acp"
                launcher.parent.mkdir(parents=True)
                import shlex
                launcher.write_text("#!/bin/sh\nexec " + shlex.join([sys.executable, str(Path(__file__).resolve()), "--wire"]) + "\n")
                launcher.chmod(0o700)
                token = "local-synthetic-" + nonce
                base = f"http://127.0.0.1:{port}"
                service_log = (evidence / "rivet.log").open("w")
                service = subprocess.Popen([str(rivet), "server", "--host", "127.0.0.1", "--port", str(port),
                    "--token", token, "--no-telemetry"], env=env, cwd=disposable,
                    stdout=service_log, stderr=subprocess.STDOUT, start_new_session=True)
                record(events, event="rivet_start", pid=service.pid, binary_sha256=hashlib.sha256(rivet.read_bytes()).hexdigest(),
                       launcher_sha256=hashlib.sha256(launcher.read_bytes()).hexdigest())
                for _ in range(100):
                    try:
                        get(base + "/v1/acp", token)
                        break
                    except (OSError, urllib.error.URLError):
                        time.sleep(.1)
                try:
                    get(base + "/v1/acp")
                    raise AssertionError("unauthenticated access succeeded")
                except urllib.error.HTTPError as error:
                    assert error.code == 401
                    record(events, event="unauthenticated_rejected", status=error.code)
                env.update(ACP_RIVET_URL=base + f"/v1/acp/live-{nonce}?agent=mock", ACP_RIVET_TOKEN=token)
            command = [str(ROOT / "target/debug/examples/acp_live"), sys.executable, str(Path(__file__).resolve()), "--wire"]
            with (evidence / "client.jsonl").open("w") as stdout, (evidence / "stderr.log").open("w") as stderr:
                client = subprocess.Popen(command, cwd=disposable, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                record(events, event="client_start", pid=client.pid, command=command)
                if mode == "interrupt":
                    deadline = time.monotonic() + 180
                    while client.poll() is None and time.monotonic() < deadline:
                        if (home / "natural-started").exists():
                            record(events, event="injected_sigterm", owned=owned_processes(),
                                   command_pid=(home / "natural-started").read_text())
                            os.kill(os.getpid(), signal.SIGTERM)
                        time.sleep(.02)
                    raise AssertionError("interruption control did not reach a running command")
                code = client.wait(timeout=620)
            record(events, event="client_exit", code=code,
                   effects={p.name: p.read_text() for p in home.iterdir() if p.is_file()})
            assert code == 0, f"client exit {code}; see {evidence}"
            frames = [r["value"] for r in map(json.loads, (evidence / "wire.jsonl").read_text().splitlines()) if r["event"] == "frame"]
            prompts = [r for r in frames if r.get("method") == "session/prompt"]
            actions = [a for a in plan if not a.get("bad_route")]
            assert len(prompts) == len(actions)
            for prompt, action_spec in zip(prompts, actions):
                assert prompt["params"]["prompt"] == [{"type": "text", "text": action_spec["prompt"]}]
                finals = [f for f in frames if f.get("id") == prompt["id"] and ("result" in f or "error" in f)]
                assert len(finals) == 1, finals
                if mode not in ("absent", "unavailable"):
                    assert finals[0]["result"]["stopReason"] == ("cancelled" if action_spec["label"] == "cancel" else "end_turn")
            responses = [r for r in map(json.loads, events.read_text().splitlines()) if r["event"] == "model_response"]
            assert requests, "no upstream requests"
            assert all(r["model"] == model and r["path"] == "/v1/chat/completions" for r in requests)
            if mode == "text":
                assert [r["users"] for r in requests] == [[plan[0]["prompt"]], [plan[0]["prompt"], plan[2]["prompt"]], [plan[3]["prompt"]]]
                assert all(not r["tools"] for r in requests)
                assert all(r["status"] == 200 for r in responses)
            elif mode == "tools":
                assert all(set(r["tools"]) == {"terminal", "process_manage"} for r in requests)
                assert responses and all(r["status"] == 200 for r in responses)
                calls = {}
                for response in responses:
                    line = response["data"].strip()
                    if not line.startswith("data: ") or line == "data: [DONE]":
                        continue
                    for choice in json.loads(line[6:]).get("choices", []):
                        for call in choice.get("delta", {}).get("tool_calls", []):
                            key = (response["request"], call["index"])
                            assembled = calls.setdefault(key, {"name": "", "arguments": ""})
                            for field, value in call.get("function", {}).items():
                                if field in assembled:
                                    assembled[field] += value
                assert len(calls) == 5, calls
                tool_updates = [f["params"] for f in frames if f.get("method") == "session/update"
                                and f["params"]["update"]["sessionUpdate"].startswith("tool_call")]
                for ((request_id, _), call), action_spec in zip(calls.items(), plan[:5]):
                    args = json.loads(call["arguments"])
                    assert call["name"] == "terminal" and args["command"] == action_spec["command"], call
                    assert not args.get("background", False)
                    assert requests[request_id - 1]["users"][-1] == action_spec["prompt"]
                    if action_spec["label"] in ("natural", "cancel"):
                        assert args["timeout"] == 60
                    prompt = prompts[plan.index(action_spec)]
                    starts = [u["update"] for u in tool_updates if u["sessionId"] == prompt["params"]["sessionId"]
                              and u["update"]["sessionUpdate"] == "tool_call"
                              and action_spec["command"] in json.dumps(u["update"].get("content"))]
                    assert len(starts) == 1, starts
                    ends = [u["update"] for u in tool_updates if u["sessionId"] == prompt["params"]["sessionId"]
                            and u["update"]["sessionUpdate"] == "tool_call_update"
                            and u["update"]["toolCallId"] == starts[0]["toolCallId"]]
                    assert len(ends) == 1, ends
                    expected = "failed" if action_spec["label"] in ("deny", "cancel") else "completed"
                    assert ends[0]["status"] == expected, ends
                    if action_spec["label"] == "cancel":
                        assert "[Command interrupted]" in json.dumps(ends) and "130" in json.dumps(ends)
                assert not (home / "allow-probe").exists() and not (home / "deny-control").exists()
                assert (home / "deny-probe").read_text() == "disposable-probe\n"
                assert (home / "natural-finished").read_text() == "finished"
                assert not (home / "cancel-finished").exists()
                assert prompts[-2]["params"]["sessionId"] == prompts[-1]["params"]["sessionId"]
            elif mode == "absent":
                assert responses and all(r["status"] == 404 for r in responses)
            else:
                assert not responses
            if mode in ("absent", "unavailable"):
                results = [r for r in map(json.loads, (evidence / "client.jsonl").read_text().splitlines()) if r["event"] == "result"]
                assert len(results) == 1 and results[0]["ok"]
                assert "Provider said:" in results[0]["text"]
                assert not any(f.get("method") == "session/request_permission" for f in frames)
                assert all(not r["tools"] and not r["tool_results"] for r in requests)
                record(events, event="provider_failure_is_normal_acp_text", requests=len(requests), result=results[0])
            else:
                verify_delivery(evidence)
            record(events, event="assertions_pass", mode=mode, transport=transport, loaded=get(OLLAMA + "/api/ps"))
            if rivet:
                record(events, event="server_after_client_drop", servers=get(base + "/v1/acp", token))
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        record(events, event="cleanup_start")
        try:
            stop_owned_processes(events)
        finally:
            relay.shutdown()
            relay.server_close()
            thread.join(timeout=5)
        wire_log = evidence / "wire.jsonl"
        if wire_log.exists():
            for r in map(json.loads, wire_log.read_text().splitlines()):
                if r["event"] == "spawn":
                    for _ in range(100):
                        if not Path(f"/proc/{r['pid']}").exists():
                            break
                        time.sleep(.05)
                    alive = Path(f"/proc/{r['pid']}").exists()
                    record(events, event="hermes_cleanup", pid=r["pid"], alive=alive)
                    assert not alive, "Hermes process remains"
        if home:
            shutil.rmtree(home)
        record(events, event="cleanup_complete", owned=owned_processes())
    print("PASS", transport, mode, evidence, flush=True)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--wire"]:
        wire()
    elif sys.argv[1:2] == ["--check-delivery"]:
        evidence = Path(sys.argv[2])
        verify_delivery(evidence)
        print("PASS model → ACP → Rig delivery", evidence)
        if sys.argv[3:] == ["--controls"]:
            for mutation in ("missing-recovery", "truncated-stream"):
                destination = ROOT / ".amp/in/artifacts" / f"{mutation}-{uuid.uuid4().hex[:12]}"
                destination.mkdir()
                for name in ("plan.json", "wire.jsonl", "client.jsonl"):
                    shutil.copyfile(evidence / name, destination / name)
                rows = [json.loads(line) for line in (evidence / "events.jsonl").read_text().splitlines()]
                last = max(r["request"] for r in rows if r["event"] == "model_request")
                rows = [r for r in rows if not (r["event"] == "model_response" and
                        ((mutation == "missing-recovery" and r["request"] == last) or
                         (mutation == "truncated-stream" and r["data"].strip() == "data: [DONE]")))]
                (destination / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
                try:
                    verify_delivery(destination)
                except AssertionError as error:
                    print("PASS rejected", mutation, str(error), destination)
                else:
                    raise AssertionError(f"verifier accepted {mutation}")
    else:
        try:
            run()
        except KeyboardInterrupt:
            sys.exit(130)
