#!/usr/bin/env python3
"""Live inference checks through A2A. Model prose is not an effect receipt."""

import argparse
import json
import uuid
from pathlib import Path

from demo import Proof, rpc_receipts


class Live(Proof):
    def http(
        self, path, body=None, owner="rig-1", headers=None, method=None, port=None
    ):
        if self.args.direct_port:
            if path.startswith("/actors/"):
                path = "/" + path.split("/", 3)[3]
            port, owner = self.args.direct_port, None
        return super().http(path, body, owner, headers, method, port)

    def history(self, context):
        status, history = self.http(f"/actors/rig-1/history/{context}", owner="admin")
        self.check("history HTTP", status, 200)
        self.check(
            "canonical history present",
            isinstance(history, list) and bool(history),
            True,
        )
        return history

    def run(self):
        worker_logs = {}
        self.record(
            "scope",
            inference="real Ollama",
            transport="direct A2A component"
            if self.args.direct_port
            else "authenticated edge and deployed gateway",
            restore=not bool(self.args.direct_port),
        )
        if not self.args.direct_port:
            self.provenance()
            initial_actor = self.actor()
            self.check(
                "initial SUSPENDED",
                initial_actor["status"]["state"],
                "ACTOR_STATE_SUSPENDED",
            )
            template = json.loads(
                self.ate(
                    "get",
                    "actor-template",
                    initial_actor["actorTemplate"]["name"],
                    "-a",
                    "rig-local",
                    "-o",
                    "json",
                )
            )
            golden = template.get("status", {}).get("goldenSnapshotStatus", {})
            self.check("golden tag exists", bool(golden.get("goldenTag")), True)
            self.check("golden has no error", bool(golden.get("errorMessage")), False)
            tag = json.loads(
                self.ate(
                    "get",
                    "tag",
                    golden["goldenTag"]["name"],
                    "-a",
                    golden["goldenTag"]["atespace"],
                    "-o",
                    "json",
                )
            )
            self.check(
                "golden durable copy complete",
                tag["status"]["snapshot"]["durableSnapshot"]["status"],
                "SNAPSHOT_STORAGE_STATUS_COMPLETED",
            )
            self.check(
                "golden MEMORY",
                tag["status"]["snapshot"]["fidelity"],
                "SNAPSHOT_FIDELITY_MEMORY",
            )
            self.record("golden_tag", tag=tag)
            self.record("template_image_golden", template=template)
        initial = self.inspect()
        self.check("expected actor build", initial["build"], self.args.build)
        for key, value in {
            "nonce": None,
            "active": 0,
            "journal": "",
            "commits": 0,
            "invocations": 0,
            "completions": 0,
        }.items():
            self.check("fresh " + key, initial[key], value)
        operation1, operation2 = uuid.uuid4().hex, uuid.uuid4().hex
        secret = "violet-" + uuid.uuid4().hex[:12]
        first = f"Call record with operation {operation1} and value {secret}."
        second = f"Call record with operation {operation2}. Use the exact value from my preceding request as value. Do not invent a new value."
        task1 = self.send(first)
        self.check(
            "first live task completed",
            task1["status"]["state"],
            "TASK_STATE_COMPLETED",
        )
        self.context = task1["contextId"]
        history1 = self.history(self.context)
        before = self.inspect()
        self.check(
            "first effect counts", (before["invocations"], before["commits"]), (1, 1)
        )
        self.check("heap nonce created", bool(before["nonce"]), True)
        self.check("no fixture inference", before["completions"], 0)
        rows = [json.loads(line) for line in before["journal"].splitlines()]
        self.check(
            "first actual effect",
            [(r["operation"], r["value"]) for r in rows],
            [(operation1, secret)],
        )
        if not self.args.direct_port:
            before, assignment, suspended = self.suspend(reopen=False)
            snapshot = self.snapshot(suspended)
            self.record("snapshot_join", actor=suspended, worker_before=assignment)
            if self.args.replace_worker:
                worker = assignment["status"]["workerAssignment"]
                self.check(
                    "dedicated cluster for worker removal",
                    self.kube_context,
                    "kind-rig-ollama-e2e",
                )
                self.check("dedicated worker pool", worker["workerPool"], "rig")
                self.check(
                    "dedicated worker namespace", worker["workerNamespace"], "rig-local"
                )
                worker_logs[worker["workerPodUid"]] = self.command(
                    "kubectl",
                    "--context",
                    self.kube_context,
                    "logs",
                    "-n",
                    "rig-local",
                    worker["workerPod"],
                    "--all-containers=true",
                )
                if self.args.cross_node:
                    destination = "rig-ollama-e2e-control-plane"
                    self.check(
                        "source is worker node",
                        worker["nodeName"],
                        "rig-ollama-e2e-worker",
                    )
                    self.check(
                        "snapshot local to source",
                        snapshot["locality"],
                        worker["nodeName"],
                    )
                    self.command(
                        "docker",
                        "exec",
                        destination,
                        "sh",
                        "-ec",
                        f"test -d /var/lib/ate/actors; test ! -e /var/lib/ate/actors/{initial_actor['metadata']['uid']}/local-checkpoint/{snapshot['uuid']}",
                    )
                    self.command(
                        "kubectl",
                        "--context",
                        self.kube_context,
                        "patch",
                        "workerpool",
                        "rig",
                        "-n",
                        "rig-local",
                        "--type=merge",
                        "-p",
                        json.dumps(
                            {
                                "spec": {
                                    "template": {
                                        "nodeSelector": {
                                            "kubernetes.io/hostname": destination
                                        },
                                        "tolerations": [
                                            {
                                                "key": "node-role.kubernetes.io/control-plane",
                                                "operator": "Exists",
                                                "effect": "NoSchedule",
                                            }
                                        ],
                                    }
                                }
                            }
                        ),
                    )
                self.command(
                    "kubectl",
                    "--context",
                    self.kube_context,
                    "delete",
                    "pod",
                    "-n",
                    "rig-local",
                    worker["workerPod"],
                    "--wait=true",
                )
                self.command(
                    "kubectl",
                    "--context",
                    self.kube_context,
                    "rollout",
                    "status",
                    "deployment/rig",
                    "-n",
                    "rig-local",
                    "--timeout=120s",
                )
                self.check(
                    "still suspended after worker removal",
                    self.actor()["status"]["state"],
                    "ACTOR_STATE_SUSPENDED",
                )
            self.open_admission()
        # No discovery, inspect, or manual resume precedes the waking message.
        task2 = self.send(second, self.context)
        self.check(
            "second live task completed",
            task2["status"]["state"],
            "TASK_STATE_COMPLETED",
        )
        if not self.args.direct_port:
            restored = self.actor()
            self.check(
                "request woke actor", restored["status"]["state"], "ACTOR_STATE_RUNNING"
            )
            self.check(
                "actor UID preserved",
                restored["metadata"]["uid"],
                initial_actor["metadata"]["uid"],
            )
            if self.args.replace_worker:
                self.check(
                    "different worker Pod UID",
                    restored["status"]["workerAssignment"]["workerPodUid"]
                    != worker["workerPodUid"],
                    True,
                )
                self.check(
                    "restore node",
                    restored["status"]["workerAssignment"]["nodeName"],
                    "rig-ollama-e2e-control-plane"
                    if self.args.cross_node
                    else worker["nodeName"],
                )
            self.check(
                "runtime continuity",
                restored["status"]["workerAssignment"]["sandboxRuntime"],
                snapshot["sandboxRuntime"],
            )
            self.check(
                "one IPv4 worker address",
                len(restored["status"]["workerAssignment"]["workerPodIps"]),
                1,
            )
            self.record("restore_join", actor=restored)
        after = self.inspect()
        self.check("heap nonce continuity", after["nonce"], before["nonce"])
        self.check("effect counts", (after["invocations"], after["commits"]), (2, 2))
        self.check("no fixture inference after second turn", after["completions"], 0)
        rows = [json.loads(line) for line in after["journal"].splitlines()]
        self.check(
            "actual recalled value",
            [(r["operation"], r["value"]) for r in rows],
            [(operation1, secret), (operation2, secret)],
        )
        self.check("file prefix", after["journal"].startswith(before["journal"]), True)
        history2 = self.history(self.context)
        self.check("canonical history prefix", history2[: len(history1)], history1)
        self.check("canonical history extended", len(history2) > len(history1), True)
        for prompt in (first, second):
            self.check(
                "actual user prompt in canonical history",
                prompt in json.dumps(history2),
                True,
            )
        receipts = [
            json.loads(line) for line in self.args.receipts.read_text().splitlines()
        ]
        for prompt, operation, task in (
            (first, operation1, task1),
            (second, operation2, task2),
        ):
            calls = [
                r
                for r in receipts
                if [
                    m["content"]
                    for m in r["request"]["messages"]
                    if m["role"] == "user"
                ][-1:]
                == [prompt]
            ]
            self.check("two independent model calls per turn", len(calls), 2)
            for call in calls:
                self.check("real provider status", call["status"], 200)
                self.check("provider completed", call["response"]["done"], True)
                self.check(
                    "provider evaluated tokens",
                    call["response"]["eval_count"] > 0,
                    True,
                )
                self.check(
                    "fixed local provider",
                    call["upstream"],
                    "http://127.0.0.1:11434/api/chat",
                )
            tool_calls = calls[0]["response"]["message"]["tool_calls"]
            self.check("one actual model tool call", len(tool_calls), 1)
            tool = tool_calls[0]["function"]
            self.check(
                "model selected real effect",
                (
                    tool["name"],
                    tool["arguments"]["operation"],
                    tool["arguments"]["value"],
                ),
                ("record", operation, secret),
            )
            result = json.loads(calls[1]["request"]["messages"][-1]["content"])
            self.check(
                "actual tool result returned to model",
                (result["operation"], result["nonce"]),
                (operation, before["nonce"]),
            )
            text = calls[1]["response"]["message"]["content"]
            self.check("model final text present", bool(text.strip()), True)
            self.check(
                "model text delivered through A2A",
                task["artifacts"][0]["parts"][0]["text"],
                text,
            )
            self.record(
                "independent_model_join",
                operation=operation,
                task=task["id"],
                receipts=calls,
            )
        if not self.args.direct_port:
            restored_worker = restored["status"]["workerAssignment"]["workerPodUid"]
            worker_logs[restored_worker] = self.command(
                "kubectl",
                "--context",
                self.kube_context,
                "logs",
                "-n",
                "rig-local",
                restored["status"]["workerAssignment"]["workerPod"],
                "--tail=-1",
            )
            gateway = self.command(
                "kubectl",
                "--context",
                self.kube_context,
                "logs",
                "-n",
                "ate-system",
                "deployment/atenet-router",
                "--tail=-1",
            )
            api = self.command(
                "kubectl",
                "--context",
                self.kube_context,
                "logs",
                "-n",
                "ate-system",
                "-l",
                "app=ate-api-server",
                "--tail=-1",
                "--prefix",
            )
            uid = restored["metadata"]["uid"]
            events = [json.loads(line) for line in self.events.read_text().splitlines()]
            turns = [
                e
                for e in events
                if e["kind"] == "http"
                and (e.get("input") or {}).get("method") == "SendMessage"
            ]
            for turn in turns:
                matches = [
                    line
                    for line in gateway.splitlines()
                    if "trace.id=" + turn["request"] in line
                ]
                self.check("unique gateway request receipt", len(matches), 1)
                self.check(
                    "gateway selected actor and route",
                    all(
                        s in matches[0]
                        for s in (
                            "ate.actor.uid=" + uid,
                            "route=default/substrate-actors",
                            "http.status=200",
                        )
                    ),
                    True,
                )
            self.check(
                "waking request triggered resume",
                "ate.router.resume=triggered"
                in next(
                    line
                    for line in gateway.splitlines()
                    if "trace.id=" + turns[-1]["request"] in line
                ),
                True,
            )
            effects = []
            for pod_uid, logs in worker_logs.items():
                for line in logs.splitlines():
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if (
                        event.get("event") == "actor_effect"
                        and event.get("labels", {}).get("ate.actor.uid") == uid
                    ):
                        effects.append(
                            (
                                event["operation"],
                                event["nonce"],
                                event["commits"],
                                pod_uid,
                            )
                        )
            self.check(
                "exact worker effect receipts",
                sorted(effects),
                sorted(
                    [
                        (
                            operation1,
                            before["nonce"],
                            1,
                            assignment["status"]["workerAssignment"]["workerPodUid"],
                        ),
                        (operation2, before["nonce"], 2, restored_worker),
                    ]
                ),
            )
            snapshot_uri = snapshot["durableSnapshot"]["object"]["snapshotUri"]
            wake = turns[-1]
            restores = [
                e
                for e in rpc_receipts(
                    worker_logs[restored_worker],
                    "/ateom.Ateom/RestoreWorkload",
                    uid,
                    wake["start"],
                    wake["time"],
                )
                if e.get("err") is None
                and e["req"].get("fidelity") == 3
                and e["req"].get("snapshot_uri") == snapshot_uri
            ]
            self.check("executed successful MEMORY restore", len(restores), 1)
            self.check(
                "restore local-copy choice",
                restores[0]["req"].get("preserve_restore_dir", False),
                not self.args.cross_node,
            )
            self.record("executed_restore", snapshot=snapshot, records=restores)
            joins = [
                event
                for event in rpc_receipts(
                    api,
                    "/ateapi.Control/ResumeActor",
                    uid,
                    wake["start"],
                    wake["time"],
                    restored_worker,
                )
                if event["err"] is None
                and event["resp"].get("resumed") is True
                and any(
                    s["uuid"] == snapshot["uuid"]
                    and s["generation"] == snapshot["generation"]
                    for s in event["resp"]["actor"]["status"]["snapshots"]
                )
                and event["principal"]["ID"]
                == "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
            ]
            self.check(
                "control plane resumed the captured snapshot in waking interval",
                len(joins),
                1,
            )
            self.record(
                "control_plane_join",
                actor_uid=uid,
                snapshot=snapshot,
                records=joins,
                note="Join separate control-plane trace IDs by actor UID, snapshot, worker assignment and request interval; they are not client trace IDs.",
            )
        self.record(
            "result",
            status="PASS",
            scope="real model and tool receipts; cluster mode includes gateway, actor, worker and snapshot joins",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--build", required=True)
    parser.add_argument("--receipts", type=Path, required=True)
    parser.add_argument("--substrate", default="/tmp/rig-substrate-v0.5.0")
    parser.add_argument("--direct-port", type=int)
    parser.add_argument(
        "--cross-node",
        action="store_true",
        help="move the owned pool to the other owned node",
    )
    parser.add_argument(
        "--replace-worker",
        action="store_true",
        help="delete only the suspended actor's worker in the disposable cluster",
    )
    args = parser.parse_args()
    if args.cross_node and not args.replace_worker:
        parser.error("--cross-node requires --replace-worker")
    proof = Live(args)
    try:
        proof.run()
    except Exception as error:
        proof.record(
            "result", status="FAIL", error=str(error), type=type(error).__name__
        )
        raise
