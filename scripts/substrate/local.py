#!/usr/bin/env python3
"""Component checks: real Rust binary, Rig loop, A2A HTTP/SSE; no Substrate."""

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from demo import Proof


class Local(Proof):
    cluster = False

    def http(
        self, path, body=None, owner="rig-1", headers=None, method=None, port=18190
    ):
        if path.startswith("/actors/"):
            _, _, actor, path = path.split("/", 3)
            port = 18190 if actor == "rig-1" else 18191
            path = "/" + path
        return super().http(path, body, None, headers, method, port)

    def stream(self, method, params):
        conn = http.client.HTTPConnection(
            "127.0.0.1", self.edge_port if self.cluster else 18190, timeout=20
        )
        request = {
            "jsonrpc": "2.0",
            "id": uuid.uuid4().hex,
            "method": method,
            "params": params,
        }
        conn.request(
            "POST",
            "/jsonrpc",
            json.dumps(request),
            {
                "Content-Type": "application/json",
                "A2A-Version": "1.0",
                "X-Request-ID": request["id"],
                **(
                    {"Authorization": "Bearer " + self.tokens["rig-1"]}
                    if self.cluster
                    else {}
                ),
            },
        )
        response = conn.getresponse()
        self.check("SSE HTTP", response.status, 200)
        self.check(
            "SSE content type",
            response.getheader("Content-Type").startswith("text/event-stream"),
            True,
        )
        self.record("stream_input", input=request)
        return conn, response

    def event(self, response):
        while line := response.readline():
            if line.startswith(b"data:"):
                event = json.loads(line[5:])
                self.record("stream_event", value=event)
                return event["result"]
        raise EOFError("SSE ended before event")

    def held(self, context=None):
        prompt = self.prompt("held-" + uuid.uuid4().hex, hold=True)
        msg = {
            "messageId": uuid.uuid4().hex,
            "role": "ROLE_USER",
            "parts": [{"text": prompt}],
        }
        if context:
            msg["contextId"] = context
        conn, response = self.stream("SendStreamingMessage", {"message": msg})
        event = self.event(response)
        task = event["task"]
        self.wait("tool held", lambda: self.inspect()["held"])
        return prompt, task, conn, response

    def run(self):
        self.record(
            "scope",
            real=[
                "Rust binary",
                "Rig AgentRunner",
                "file tool",
                "A2A 1.0 JSON-RPC/SSE",
            ],
            fixture=["deterministic inference"],
            excluded=["ACP"]
            if self.cluster
            else ["kind", "agentgateway", "Substrate", "gVisor", "ACP"],
            deployed=self.cluster,
        )
        self.command("git", "rev-parse", "HEAD")
        self.command("git", "status", "--porcelain")
        self.command("rustc", "--version")
        self.record(
            "binary",
            sha256=hashlib.sha256(self.args.binary.read_bytes()).hexdigest(),
            lock_sha256=hashlib.sha256(Path("Cargo.lock").read_bytes()).hexdigest(),
        )
        card = self.http("/.well-known/agent-card.json")[1]
        self.check(
            "card protocol version",
            card["supportedInterfaces"][0]["protocolVersion"],
            "1.0",
        )
        for version in (None, "0.3", "99.0"):
            _, rejected = self.http(
                "/jsonrpc",
                {
                    "jsonrpc": "2.0",
                    "id": "version-control",
                    "method": "GetTask",
                    "params": {"id": "missing"},
                },
                headers={"A2A-Version": version},
            )
            self.check("unsupported version rejected", "error" in rejected, True)
            self.check(
                "version rejection is specific",
                "version" in rejected["error"]["message"].lower(),
                True,
            )
        one, two = self.prompt("alpha"), self.prompt("beta")
        task = self.send(one)
        context = task["contextId"]
        self.check("first prompt exact", self.settled(task)["prompts"], [one])
        self.check(
            "second prompt exact",
            self.settled(self.send(two, context))["prompts"],
            [one, two],
        )
        self.check("context isolation", self.settled(self.send(two))["prompts"], [two])
        state = self.inspect()
        self.check(
            "effect dedup separate from invocations",
            (state["invocations"], state["commits"]),
            (3, 2),
        )
        self.check(
            "uncanceled turns each complete two model calls", state["completions"], 6
        )
        self.check(
            "cross-actor task rejected",
            "error" in self.rpc("GetTask", {"id": task["id"]}, actor="rig-2"),
            True,
        )
        self.check("other actor untouched", self.inspect("rig-2")["commits"], 0)
        bad = self.rpc(
            "SendMessage",
            {
                "message": {
                    "messageId": uuid.uuid4().hex,
                    "role": "ROLE_USER",
                    "parts": [{"data": {"x": 1}}],
                }
            },
        )
        self.check("non-text fails", bad["error"]["code"], -32602)
        self.check("non-text has no effect", self.inspect()["invocations"], 3)

        # Hold a real tool so disconnect cannot occur after completion.
        prompt, task, conn, response = self.held()
        response.close()
        conn.close()
        self.check("disconnect does not stop execution", self.inspect()["active"], 1)
        conn, response = self.stream("SubscribeToTask", {"id": task["id"]})
        self.event(response)
        self.http("/release", {})
        terminal = None
        while terminal is None:
            event = self.event(response)
            if (
                event.get("task", {}).get("status", {}).get("state")
                == "TASK_STATE_COMPLETED"
            ):
                terminal = event["task"]
        response.close()
        conn.close()
        self.check(
            "reconnected result exact", self.settled(terminal)["prompts"], [prompt]
        )
        self.check(
            "task read after stream",
            self.rpc("GetTask", {"id": task["id"]})["result"]["status"]["state"],
            "TASK_STATE_COMPLETED",
        )

        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            prompt, task, conn, response = self.held()
            calls = self.inspect()["invocations"]
            followup = self.prompt("serialized-follower")
            second = pool.submit(self.send, followup, task["contextId"])
            self.wait(
                "concurrent turns admitted", lambda: self.inspect()["active"] == 2
            )
            self.check(
                "second turn waits before effect", self.inspect()["invocations"], calls
            )
            self.http("/release", {})
            self.check(
                "serialized history exact",
                self.settled(second.result(timeout=10))["prompts"],
                [prompt, followup],
            )
            # Do not read the first turn's terminal response before disconnect.
            response.close()
            conn.close()
            before_retry = self.inspect()
            self.check(
                "uncertain response reconciles",
                self.rpc("GetTask", {"id": task["id"]})["result"]["status"]["state"],
                "TASK_STATE_COMPLETED",
            )
            replay = json.loads(prompt)
            replay["hold"] = False
            self.settled(self.send(json.dumps(replay), task["contextId"]))
            after_retry = self.inspect()
            self.check(
                "retransmission invokes tool",
                after_retry["invocations"],
                before_retry["invocations"] + 1,
            )
            self.check(
                "fixture operation dedup prevents second commit",
                after_retry["commits"],
                before_retry["commits"],
            )

            prompt, task, conn, response = self.held()
            baseline = self.inspect()["commits"]
            completions = self.inspect()["completions"]
            cancellation = pool.submit(self.rpc, "CancelTask", {"id": task["id"]})
            self.wait(
                "protocol canceled before cleanup",
                lambda: (
                    self.rpc("GetTask", {"id": task["id"]})["result"]["status"]["state"]
                    == "TASK_STATE_CANCELED"
                ),
            )
            self.check("terminal task is not quiescent", self.inspect()["active"], 1)
            self.check("cancel waits for in-flight tool", cancellation.done(), False)
            self.http("/release", {})
            cancellation.result(timeout=10)
            self.wait("cancel cleanup complete", lambda: self.inspect()["active"] == 0)
            self.check(
                "accepted in-flight effect completes once",
                self.inspect()["commits"],
                baseline + 1,
            )
            self.check(
                "cancellation prevents next model dispatch",
                self.inspect()["completions"],
                completions,
            )
            response.close()
            conn.close()
            self.settled(self.send(self.prompt("after-cancel"), task["contextId"]))

            prompt, task, conn, response = self.held()
            queued = self.prompt("queued")
            msg = {
                "messageId": uuid.uuid4().hex,
                "contextId": task["contextId"],
                "role": "ROLE_USER",
                "parts": [{"text": queued}],
            }
            conn2, response2 = self.stream("SendStreamingMessage", {"message": msg})
            queued_task = self.event(response2)["task"]
            self.wait("two executions admitted", lambda: self.inspect()["active"] == 2)
            calls = self.inspect()["invocations"]
            self.check(
                "context lock prevents parallel effects", self.inspect()["held"], True
            )
            cancel = pool.submit(self.rpc, "CancelTask", {"id": queued_task["id"]})
            self.wait(
                "queued canceled",
                lambda: (
                    self.rpc("GetTask", {"id": queued_task["id"]})["result"]["status"][
                        "state"
                    ]
                    == "TASK_STATE_CANCELED"
                ),
            )
            self.http("/release", {})
            cancel.result(timeout=10)
            self.wait("all executions settled", lambda: self.inspect()["active"] == 0)
            self.check(
                "queued cancellation has no effect",
                self.inspect()["invocations"],
                calls,
            )
            for item in (response, conn, response2, conn2):
                item.close()

            prompt, task, conn, response = self.held()
            rejected = self.rpc("CancelTask", {"id": task["id"], "tenant": "other"})
            self.check("mismatched tenant rejected", "error" in rejected, True)
            self.check(
                "rejected cancellation preserves task state",
                self.rpc("GetTask", {"id": task["id"]})["result"]["status"]["state"],
                "TASK_STATE_WORKING",
            )
            self.check(
                "rejected cancellation preserves execution", self.inspect()["active"], 1
            )
            self.http("/release", {})
            self.wait("tenant control settled", lambda: self.inspect()["active"] == 0)
            response.close()
            conn.close()

        calls = self.inspect()["invocations"]
        for prompt in ("[]", "42"):
            task = self.send(prompt)
            self.check(
                "invalid fixture shape fails",
                task["status"]["state"],
                "TASK_STATE_FAILED",
            )
            self.check(
                "invalid fixture shape has no tool effect",
                self.inspect()["invocations"],
                calls,
            )
            self.check(
                "invalid fixture shape releases execution", self.inspect()["active"], 0
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument(
        "--binary", type=Path, default=Path("target/debug/examples/substrate_actor")
    )
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    with tempfile.TemporaryDirectory(prefix="rig-local-proof-") as directory:
        args.tokens = Path(directory) / "tokens.json"
        args.tokens.write_text("{}")
        proof = Local(args)
        processes = []
        logs = []
        try:
            for index in range(2):
                log = (proof.out / f"actor-{index + 1}.log").open("w")
                logs.append(log)
                env = dict(
                    os.environ,
                    BIND=f"127.0.0.1:{18190 + index}",
                    DATA_DIR=f"{directory}/data-{index}",
                )
                processes.append(
                    subprocess.Popen(
                        [str(args.binary)], env=env, stdout=log, stderr=log
                    )
                )
            for _ in range(100):
                try:
                    proof.inspect()
                    break
                except ConnectionRefusedError:
                    time.sleep(0.1)
            proof.run()
            before = proof.inspect()
            processes[0].terminate()
            processes[0].wait(timeout=10)
            env = dict(
                os.environ, BIND="127.0.0.1:18190", DATA_DIR=f"{directory}/data-0"
            )
            processes[0] = subprocess.Popen(
                [str(args.binary)], env=env, stdout=logs[0], stderr=logs[0]
            )
            for _ in range(100):
                try:
                    cold = proof.inspect()
                    break
                except ConnectionRefusedError:
                    time.sleep(0.1)
            proof.check(
                "cold start preserves only file", cold["journal"], before["journal"]
            )
            proof.check("cold start loses nonce", cold["nonce"], None)
            proof.check("cold start loses memory counter", cold["commits"], 0)
            proof.settled(proof.send(proof.prompt("cold-start-control")))
            proof.check(
                "cold start cannot pass memory continuity",
                proof.inspect()["nonce"] == before["nonce"],
                False,
            )
            proof.record(
                "result",
                status="PASS",
                scope="local component checks only; cold restart is not Substrate restore",
            )
        except Exception as error:
            proof.record(
                "result", status="FAIL", error=str(error), type=type(error).__name__
            )
            raise
        finally:
            for process in processes:
                process.terminate()
                process.wait(timeout=10)
            for log in logs:
                log.close()
