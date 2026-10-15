#!/usr/bin/env python3
"""A VOLUMES cold restore must fail the memory-continuity contract."""

import argparse
import json
import uuid
from pathlib import Path

from demo import Proof, rpc_receipts

parser = argparse.ArgumentParser()
parser.add_argument("--tokens", type=Path, required=True)
parser.add_argument("--evidence", type=Path, required=True)
p = Proof(parser.parse_args())
try:
    p.check("owned cluster", p.kube_context, "kind-rig-ollama-e2e")
    p.check(
        "fresh suspended actor", p.actor()["status"]["state"], "ACTOR_STATE_SUSPENDED"
    )
    template = json.loads(
        p.ate("get", "actor-template", "rig-poc", "-a", "rig-local", "-o", "json")
    )
    template.pop("status")
    name = "rig-volumes-" + uuid.uuid4().hex[:8]
    template["metadata"] = {"name": name, "atespace": "rig-local"}
    template["snapshotConfig"] = {
        "preferredFidelity": "SNAPSHOT_FIDELITY_VOLUMES",
        "storageLocation": "gs://ate-snapshots/rig-volumes/",
        "goldenSnapshotConfig": {"mode": "GOLDEN_SNAPSHOT_MODE_DISABLED"},
    }
    path = p.out / "template.json"
    path.write_text(json.dumps(template))
    p.ate("create", "actor-template", "-f", str(path))
    p.ate("delete", "actor", "rig-1", "-a", "rig-local")
    p.ate("create", "actor", "rig-1", "-a", "rig-local", "--template", name)
    first = p.prompt("volume-before")
    task = p.send(first)
    p.settled(task)
    p.http("/admin/close", {}, owner="admin")
    p.wait(
        "forwarding drained",
        lambda: p.http("/admin/status", owner="admin")[1]["inflight"] == 0,
    )
    before = p.inspect()
    p.check("execution cleanup", before["active"], 0)
    p.check("no held tool", before["held"], False)
    p.check("first effect counts", (before["invocations"], before["commits"]), (1, 1))
    p.check("first heap nonce exists", bool(before["nonce"]), True)
    first_input = json.loads(first)
    first_rows = [json.loads(line) for line in before["journal"].splitlines()]
    p.check(
        "first journal row",
        first_rows,
        [
            {
                "operation": first_input["operation"],
                "value": first_input["value"],
                "prompts": [first],
            }
        ],
    )
    p.ate("suspend", "actor", "rig-1", "-a", "rig-local")
    suspended = p.actor()
    p.check("suspended", suspended["status"]["state"], "ACTOR_STATE_SUSPENDED")
    snapshot = max(suspended["status"]["snapshots"], key=lambda s: s["generation"])
    p.check("VOLUMES captured", snapshot["fidelity"], "SNAPSHOT_FIDELITY_VOLUMES")
    p.check(
        "durable completed",
        snapshot["durableSnapshot"]["status"],
        "SNAPSHOT_STORAGE_STATUS_COMPLETED",
    )
    p.open_admission()
    second = p.prompt("volume-after")
    result = p.settled(p.send(second, task["contextId"]))
    after = p.inspect()
    second_input = json.loads(second)
    p.check(
        "exact retained and new journal rows",
        [json.loads(line) for line in after["journal"].splitlines()],
        first_rows
        + [
            {
                "operation": second_input["operation"],
                "value": second_input["value"],
                "prompts": [second],
            }
        ],
    )
    p.check("heap nonce changes", after["nonce"] != before["nonce"], True)
    p.check("history is not restored", result["prompts"], [second])
    p.check(
        "heap effect counters start again",
        (after["invocations"], after["commits"]),
        (1, 1),
    )
    actor = p.actor()
    logs = p.command(
        "kubectl",
        "--context",
        p.kube_context,
        "logs",
        "-n",
        "rig-local",
        actor["status"]["workerAssignment"]["workerPod"],
        "--tail=-1",
    )
    events = [json.loads(line) for line in p.events.read_text().splitlines()]
    wake = [
        e
        for e in events
        if e["kind"] == "http" and (e.get("input") or {}).get("method") == "SendMessage"
    ][-1]
    restores = [
        e
        for e in rpc_receipts(
            logs,
            "/ateom.Ateom/RestoreWorkload",
            actor["metadata"]["uid"],
            wake["start"],
            wake["time"],
        )
        if e.get("err") is None and e["req"].get("fidelity") == 1
    ]
    p.check("executed VOLUMES restore", len(restores), 1)
    p.record("executed_restore", records=restores)
    p.record(
        "result",
        status="PASS",
        scope="VOLUMES is a cold restore, not memory continuity; runtime-mismatch automatic fallback remains untested",
    )
except Exception as error:
    p.record("result", status="FAIL", error=str(error), type=type(error).__name__)
    raise
