#!/usr/bin/env python3
"""Delayed and failed wake readiness against the deterministic actor image."""

import argparse
import json
import uuid
from pathlib import Path

from demo import Proof, now, rpc_receipts

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    proof = Proof(parser.parse_args())
    try:
        before = proof.inspect()
        proof.check(
            "arm delayed readiness",
            proof.http("/actors/rig-1/readiness/3", {}, owner="admin")[0],
            200,
        )
        proof.suspend()
        prompt = proof.prompt("readiness-delayed")
        proof.settled(proof.send(prompt))
        proof.check(
            "delayed turn commits once",
            proof.inspect()["commits"],
            before["commits"] + 1,
        )
        actor = proof.actor()
        worker = actor["status"]["workerAssignment"]["workerPod"]
        logs = proof.command(
            "kubectl",
            "--context",
            proof.kube_context,
            "logs",
            "-n",
            "rig-local",
            worker,
        )
        events = []
        for line in logs.splitlines():
            try:
                event = json.loads(line)
                if (
                    event.get("labels", {}).get("ate.actor.uid")
                    == actor["metadata"]["uid"]
                ):
                    events.append(event)
            except ValueError:
                pass
        probes = [
            (i, e["remaining"])
            for i, e in enumerate(events)
            if e.get("event") == "readiness"
        ]
        proof.check(
            "three failed probes then readiness",
            [n for _, n in probes][-4:],
            [3, 2, 1, 0],
        )
        operation = json.loads(prompt)["operation"]
        effect_index = next(
            i
            for i, e in enumerate(events)
            if e.get("event") == "actor_effect" and e.get("operation") == operation
        )
        proof.check(
            "effect follows successful probe", effect_index > probes[-1][0], True
        )
        proof.check(
            "arm failed readiness",
            proof.http("/actors/rig-1/readiness/1000000", {}, owner="admin")[0],
            200,
        )
        proof.suspend()
        operation = uuid.uuid4().hex
        body = {
            "jsonrpc": "2.0",
            "id": operation,
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": operation,
                    "role": "ROLE_USER",
                    "parts": [
                        {
                            "text": json.dumps(
                                {"operation": operation, "value": "must-not-run"}
                            )
                        }
                    ],
                }
            },
        }
        start = now()
        status, response = proof.http("/jsonrpc", body)
        proof.check("readiness budget returns timeout", status, 504)
        timed_out = proof.actor()
        proof.check(
            "gateway cancellation leaves resume pending",
            timed_out["status"]["state"],
            "ACTOR_STATE_RESUMING",
        )
        proof.record("readiness_failure", actor=timed_out, response=response)
        logs = proof.command(
            "kubectl",
            "--context",
            proof.kube_context,
            "logs",
            "-n",
            "rig-local",
            "-l",
            "ate.dev/worker-pool=rig",
            "--tail=-1",
            "--prefix",
        )
        proof.check(
            "failed readiness never dispatched operation", operation in logs, False
        )
        cancellations = [
            e
            for e in rpc_receipts(
                logs,
                "/ateom.Ateom/RestoreWorkload",
                timed_out["metadata"]["uid"],
                start,
                now(),
            )
            if "wakeup probe cancelled" in (e.get("err") or "")
        ]
        proof.check("this restore probe was canceled", len(cancellations), 1)
        proof.record("cancellation_join", records=cancellations)
        proof.record(
            "result",
            status="PASS",
            scope="readiness gates routing; gateway deadline cancels the probe and leaves RESUMING; worker's longer timeout is not reached",
        )
    except Exception as error:
        proof.record(
            "result", status="FAIL", error=str(error), type=type(error).__name__
        )
        raise
