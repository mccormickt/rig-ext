#!/usr/bin/env python3
"""Real-cluster proof. Requires setup.sh and the loopback gateway and edge services."""

import argparse
import datetime
import hashlib
import http.client
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path


def now():
    return datetime.datetime.now(datetime.UTC).isoformat()


def rpc_receipts(logs, method, uid, start, end, worker_uid=None):
    """Select one actor's RPC receipts in a UTC request interval."""
    start, end = map(datetime.datetime.fromisoformat, (start, end))
    matches = []
    for line in logs.splitlines():
        try:
            event = json.loads(line[line.index("{") :])
            stamp = datetime.datetime.fromisoformat(event["time"])
        except (ValueError, KeyError):
            continue
        actor_uid = (event.get("req") or {}).get("actor_uid") or (
            ((event.get("resp") or {}).get("actor") or {}).get("metadata") or {}
        ).get("uid")
        if worker_uid is not None:
            assignment = (
                ((event.get("resp") or {}).get("actor") or {}).get("status") or {}
            ).get("worker_assignment") or {}
            if assignment.get("worker_pod_uid") != worker_uid:
                continue
        if event.get("method") == method and actor_uid == uid and start <= stamp <= end:
            matches.append(event)
    return matches


class Proof:
    def __init__(self, args):
        self.args = args
        self.kube_context = os.environ.get("KUBECTL_CONTEXT", "kind-rig-ollama-e2e")
        self.edge_port = int(os.environ.get("EDGE_PORT", "15020"))
        self.tokens = json.loads(args.tokens.read_text())
        self.out = args.evidence
        self.out.mkdir(parents=True, exist_ok=False)
        self.events = self.out / "events.jsonl"
        self.context = "ctx-" + uuid.uuid4().hex
        self.record(
            "invocation", command=sys.argv, cwd=str(Path.cwd()), python=sys.version
        )
        files = [
            Path("Cargo.lock"),
            Path("rig-a2a/Cargo.toml"),
            *Path("rig-a2a/src").glob("*.rs"),
            Path("rig-a2a/examples/substrate_actor.rs"),
            *Path("scripts/substrate").glob("*"),
        ]
        self.record(
            "source_hashes",
            hashes={
                str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in files
                if p.is_file()
            },
        )

    def record(self, kind, **fields):
        line = json.dumps({"time": now(), "kind": kind, **fields})
        for secret in self.tokens.values():
            if secret:
                line = line.replace(secret, "[REDACTED]")
        with self.events.open("a") as file:
            file.write(line + "\n")

    def check(self, name, actual, expected):
        passed = actual == expected
        self.record(
            "assert", name=name, actual=actual, expected=expected, passed=passed
        )
        if not passed:
            raise AssertionError(f"{name}: {actual!r} != {expected!r}")
        print("PASS", name, flush=True)

    def command(self, *command):
        start = now()
        result = subprocess.run(
            command, text=True, capture_output=True, check=False, timeout=180
        )
        self.record(
            "command",
            command=command,
            start=start,
            exit=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        result.check_returncode()
        return result.stdout

    def ate(self, *command):
        return self.command("kubectl", "ate", "--context", self.kube_context, *command)

    def actor(self, name="rig-1"):
        return json.loads(
            self.ate("get", "actor", name, "-a", "rig-local", "-o", "json")
        )

    def http(
        self, path, body=None, owner="rig-1", headers=None, method=None, port=None
    ):
        start = now()
        port = self.edge_port if port is None else port
        correlation = uuid.uuid4().hex
        hdr = {
            "Content-Type": "application/json",
            "A2A-Version": "1.0",
            "X-Request-ID": correlation,
        }
        hdr["traceparent"] = f"00-{correlation}-{uuid.uuid4().hex[:16]}-01"
        if owner:
            hdr["Authorization"] = "Bearer " + self.tokens[owner]
        hdr.update(headers or {})
        hdr = {key: value for key, value in hdr.items() if value is not None}
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
        conn.request(
            method or ("POST" if body is not None else "GET"),
            path,
            None if body is None else json.dumps(body),
            hdr,
        )
        response = conn.getresponse()
        data = response.read().decode()
        status = response.status
        conn.close()
        try:
            data = json.loads(data)
        except ValueError:
            pass
        self.record(
            "http",
            start=start,
            request=correlation,
            owner=owner,
            path=path,
            input=body,
            status=status,
            response=data,
            port=port,
            a2a_version=hdr.get("A2A-Version"),
        )
        return status, data

    def inspect(self, actor="rig-1"):
        status, result = self.http(f"/actors/{actor}/inspect", owner="admin")
        self.check("inspect HTTP", status, 200)
        return result

    def rpc(self, method, params, actor="rig-1", owner=None):
        status, result = self.http(
            f"/actors/{actor}/jsonrpc",
            {
                "jsonrpc": "2.0",
                "id": uuid.uuid4().hex,
                "method": method,
                "params": params,
            },
            owner=owner or actor,
        )
        self.check("JSON-RPC HTTP", status, 200)
        return result

    def prompt(self, value, context=None, hold=False):
        return json.dumps(
            {"operation": uuid.uuid4().hex, "value": value, "hold": hold},
            sort_keys=True,
        )

    def send(self, prompt, context=None, actor="rig-1"):
        message = {
            "messageId": uuid.uuid4().hex,
            "role": "ROLE_USER",
            "parts": [{"text": prompt}],
        }
        if context:
            message["contextId"] = context
        result = self.rpc("SendMessage", {"message": message}, actor=actor)
        self.check("send has no protocol error", "error" in result, False)
        return result["result"]["task"]

    def settled(self, task):
        self.check("task completed", task["status"]["state"], "TASK_STATE_COMPLETED")
        return json.loads(task["artifacts"][0]["parts"][0]["text"])

    def wait(self, name, condition, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = condition()
            if result:
                self.record("barrier", name=name, result=result)
                return result
            time.sleep(0.1)
        raise TimeoutError(name)

    def suspend(self):
        status, closed = self.http("/admin/close", {}, owner="admin")
        self.check("admission close HTTP", status, 200)
        self.check("admission closed", closed["closed"], True)
        self.wait(
            "forwarding drained",
            lambda: self.http("/admin/status", owner="admin")[1]["inflight"] == 0,
        )
        before = self.inspect()
        self.check("execution cleanup before suspend", before["active"], 0)
        self.check("no held tool before suspend", before["held"], False)
        assignment = self.actor()
        self.ate("suspend", "actor", "rig-1", "-a", "rig-local")
        suspended = self.actor()
        self.check(
            "actor suspended", suspended["status"]["state"], "ACTOR_STATE_SUSPENDED"
        )
        self.check(
            "assignment cleared", "workerAssignment" in suspended["status"], False
        )
        self.check(
            "full external snapshot",
            suspended["status"]["externalSnapshot"]["contentScope"],
            "SNAPSHOT_CONTENT_SCOPE_FULL",
        )
        status, opened = self.http("/admin/open", {}, owner="admin")
        self.check("admission open HTTP", status, 200)
        self.check("admission reopened", opened["closed"], False)
        return before, assignment, suspended

    def provenance(self):
        for command in (
            ("git", "rev-parse", "HEAD"),
            ("git", "status", "--porcelain"),
            ("rustc", "--version"),
            ("cargo", "--version"),
            ("go", "version"),
            ("docker", "version", "--format", "{{.Server.Version}}"),
            ("kubectl", "--context", self.kube_context, "version", "-o", "json"),
        ):
            self.command(*command)
        self.command("git", "-C", self.args.substrate, "rev-parse", "HEAD")
        self.command("git", "-C", self.args.substrate, "status", "--porcelain")
        self.command(
            "kubectl",
            "--context",
            self.kube_context,
            "get",
            "pods",
            "-A",
            "-o",
            "custom-columns=NAMESPACE:.metadata.namespace,POD:.metadata.name,UID:.metadata.uid,NODE:.spec.nodeName,IMAGE:.status.containerStatuses[*].imageID,READY:.status.containerStatuses[*].ready",
        )

    def run(self):
        self.provenance()
        initial = self.inspect()
        for key, expected in {
            "nonce": None,
            "journal": "",
            "active": 0,
            "commits": 0,
            "invocations": 0,
            "completions": 0,
        }.items():
            self.check("empty initial " + key, initial[key], expected)
        card_status, card = self.http("/actors/rig-1/.well-known/agent-card.json")
        self.check("card discovery", card_status, 200)
        self.record("card", value=card)
        first = self.prompt("first-" + uuid.uuid4().hex)
        task1 = self.send(first)
        self.context = task1["contextId"]
        self.check("first Rig prompt", self.settled(task1)["prompts"], [first])
        status, history1 = self.http(
            f"/actors/rig-1/history/{self.context}", owner="admin"
        )
        self.check("history HTTP", status, 200)
        self.check("first canonical history exists", bool(history1), True)
        before, _assignment, suspended = self.suspend()
        self.check("nonce created before suspend", bool(before["nonce"]), True)
        self.check(
            "first effect count", (before["invocations"], before["commits"]), (1, 1)
        )
        second = self.prompt("second-" + uuid.uuid4().hex)
        # No actor HTTP request or manual resume between SUSPENDED and this call.
        task2 = self.send(second, self.context)
        self.check(
            "two-turn Rig history", self.settled(task2)["prompts"], [first, second]
        )
        restored = self.actor()
        self.check(
            "running before any follow-up actor HTTP",
            restored["status"]["state"],
            "ACTOR_STATE_RUNNING",
        )
        after = self.inspect()
        self.check("heap-only nonce restored", after["nonce"], before["nonce"])
        self.check("memory counter restored", after["commits"], 2)
        rows = [json.loads(line) for line in after["journal"].splitlines()]
        self.check(
            "exact file values",
            [row["value"] for row in rows],
            [json.loads(first)["value"], json.loads(second)["value"]],
        )
        self.check(
            "file prefix preserved",
            after["journal"].startswith(before["journal"]),
            True,
        )
        self.check(
            "selected actor is running",
            restored["status"]["state"],
            "ACTOR_STATE_RUNNING",
        )
        self.check(
            "selected actor has a worker",
            bool(restored["status"].get("workerAssignment")),
            True,
        )
        self.check(
            "actor UID retained",
            restored["metadata"]["uid"],
            suspended["metadata"]["uid"],
        )
        status, history2 = self.http(
            f"/actors/rig-1/history/{self.context}", owner="admin"
        )
        self.check("restored history HTTP", status, 200)
        self.check(
            "canonical history prefix retained", history2[: len(history1)], history1
        )
        self.check("canonical history extended", len(history2) > len(history1), True)
        self.record(
            "result",
            status="TWO_TURN_CHECKS_PASS",
            scope="requires independent gateway/worker/actor receipt correlation before accepting FULL restore proof",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--substrate", default="/tmp/rig-substrate-v0.4.0")
    args = parser.parse_args()
    proof = Proof(args)
    try:
        proof.run()
    except Exception as error:
        proof.record(
            "result", status="FAIL", error=str(error), type=type(error).__name__
        )
        raise
