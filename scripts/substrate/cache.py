#!/usr/bin/env python3
"""Compare warmed native route caching with cacheTtl0s on an owned worker."""

import argparse
import json
import time
from pathlib import Path

from demo import Proof, rpc_receipts

parser = argparse.ArgumentParser()
parser.add_argument("--tokens", type=Path, required=True)
parser.add_argument("--evidence", type=Path, required=True)
parser.add_argument("--cache", choices=("default", "0s"), required=True)
p = Proof(parser.parse_args())
try:
    p.check("owned cluster", p.kube_context, "kind-rig-ollama-e2e")
    p.inspect()
    warm_start = time.monotonic()
    p.inspect()
    before, assignment, snapshot = p.suspend(reopen=False)
    worker = assignment["status"]["workerAssignment"]
    p.check("owned worker pool", worker["workerPool"], "rig")
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
    p.command(
        "kubectl",
        "--context",
        p.kube_context,
        "rollout",
        "status",
        "deployment/rig",
        "-n",
        "rig-local",
        "--timeout=120s",
    )
    elapsed = time.monotonic() - warm_start
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
    p.command(
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
    if p.args.cache == "default":
        p.check("attempt inside default five-second TTL", elapsed < 5, True)
        p.record(
            "native_cache_observation",
            status=status,
            response=response,
            elapsed_from_warm=elapsed,
            failed_expectation=status != 200,
        )
    else:
        p.check("uncached request succeeds", status, 200)
        p.check(
            "uncached task completed",
            response["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED",
        )
        p.check("uncached effect once", p.inspect()["commits"], before["commits"] + 1)
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
