#!/usr/bin/env python3
"""Compare warmed native route caching with cacheTtl0s on an owned worker."""

import argparse
import datetime
import json
from pathlib import Path

from demo import Proof, rpc_receipts

parser = argparse.ArgumentParser()
parser.add_argument("--tokens", type=Path, required=True)
parser.add_argument("--evidence", type=Path, required=True)
parser.add_argument("--cache", choices=("default", "0s"), required=True)
p = Proof(parser.parse_args())
try:
    p.check("owned cluster", p.kube_context, "kind-rig-ollama-e2e")
    p.check(
        "other actor suspended",
        p.actor("rig-2")["status"]["state"],
        "ACTOR_STATE_SUSPENDED",
    )
    p.inspect()
    p.inspect()
    before, assignment, snapshot = p.suspend(reopen=False)
    worker = assignment["status"]["workerAssignment"]
    p.check("owned worker pool", worker["workerPool"], "rig")
    pods = json.loads(
        p.command(
            "kubectl",
            "--context",
            p.kube_context,
            "get",
            "pods",
            "-n",
            "rig-local",
            "-l",
            "ate.dev/worker-pool=rig",
            "-o",
            "json",
        )
    )
    p.check(
        "ready spare worker",
        any(
            pod["metadata"]["uid"] != worker["workerPodUid"]
            and not pod["metadata"].get("deletionTimestamp")
            and any(
                c["type"] == "Ready" and c["status"] == "True"
                for c in pod["status"].get("conditions", [])
            )
            for pod in pods["items"]
        ),
        True,
    )
    p.command(
        "kubectl",
        "--context",
        p.kube_context,
        "delete",
        "pod",
        "-n",
        "rig-local",
        worker["workerPod"],
        "--grace-period=0",
        "--force",
        "--wait=false",
    )
    p.open_admission()
    prompt = p.prompt("cache-after-worker-replacement")
    status, response = p.http(
        "/jsonrpc",
        {
            "jsonrpc": "2.0",
            "id": "cache-control",
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": "cache-control",
                    "role": "ROLE_USER",
                    "parts": [{"text": prompt}],
                }
            },
        },
    )
    api = p.command(
        "kubectl",
        "--context",
        p.kube_context,
        "logs",
        "-n",
        "ate-system",
        "-l",
        "app=ate-api-server",
        "--tail=-1",
        "--prefix",
    )
    gateway = p.command(
        "kubectl",
        "--context",
        p.kube_context,
        "logs",
        "-n",
        "ate-system",
        "deployment/atenet-router",
        "--tail=-1",
    )
    events = [json.loads(l) for l in p.events.read_text().splitlines()]
    warms = [
        e for e in events if e["kind"] == "http" and e["path"].endswith("/inspect")
    ]
    second = warms[1]
    lookups = rpc_receipts(
        api,
        "/ateapi.Control/ResumeActor",
        assignment["metadata"]["uid"],
        second["start"],
        second["time"],
    )
    p.check(
        "warm request lookup count", len(lookups), 0 if p.args.cache == "default" else 1
    )
    request = next(e for e in events if e["kind"] == "http" and e["path"] == "/jsonrpc")
    receipts = [
        line
        for line in gateway.splitlines()
        if "trace.id=" + request["request"] in line
    ]
    p.check("unique cache request receipt", len(receipts), 1)
    elapsed = (
        datetime.datetime.fromisoformat(receipts[0].split()[0])
        - datetime.datetime.fromisoformat(second["start"])
    ).total_seconds()
    if p.args.cache == "default":
        p.check(
            "gateway receipt inside default five-second TTL", 0 <= elapsed < 5, True
        )
        p.record(
            "native_cache_observation",
            status=status,
            response=response,
            elapsed_from_warm=elapsed,
            failed_expectation=status != 200,
        )
    else:
        p.check("uncached request succeeds", status, 200)
    if status == 200:
        p.check(
            "cache request task completed",
            response["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED",
        )
        p.check(
            "cache request effect once", p.inspect()["commits"], before["commits"] + 1
        )
        p.check(
            "different assigned worker",
            p.actor()["status"]["workerAssignment"]["workerPodUid"]
            != worker["workerPodUid"],
            True,
        )
    p.record(
        "result",
        status="OBSERVED",
        cache=p.args.cache,
        http_status=status,
        scope="a native failure stays a failed expectation; does not authorize removing cacheTtl0s",
    )
except Exception as error:
    p.record("result", status="FAIL", error=str(error), type=type(error).__name__)
    raise
