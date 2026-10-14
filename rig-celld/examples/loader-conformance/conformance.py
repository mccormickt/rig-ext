#!/usr/bin/env python3
"""Check the bundled Rust adapter on one real local runtime."""
import argparse
import json
from urllib.request import urlopen

parser = argparse.ArgumentParser()
parser.add_argument("--runtime", choices=["celld", "workerd"], required=True)
parser.add_argument("--url", required=True)
args = parser.parse_args()


def get(path):
    with urlopen(args.url.rstrip("/") + path, timeout=120) as response:
        assert response.status == 200
        return json.load(response)


run = get("/run")
assert run["count"] == 12, run
assert len(set(run["passed"])) == 12, run
assert "This worker is not permitted to access the internet" in run["outbound_error"], run
enforced = args.runtime == "celld"
assert run["zero_subrequests_denied"] is enforced, run
assert run["second_subrequest_denied"] is enforced, run
print(json.dumps({"runtime": args.runtime, "run": run}))

for disposal in ["throw", "getter", "missing"]:
    for fault, primary in [("overflow", "exceeds 3 bytes"),
                           ("body", "body-primary"), ("invoke", "invoke-primary"),
                           ("abandon", "read-abandoned")]:
        result = get(f"/cleanup?dispose={disposal}&fault={fault}")
        assert primary in result["primary"], result
        assert result["stats"] == {
            "dispose": 0 if disposal == "missing" else 1,
            "entrypointDispose": 0,
            "read": 0 if fault == "invoke" else 1,
            "cancel": 0 if fault == "invoke" else 1,
            "release": 0 if fault == "invoke" else 1,
        }, result
        print(json.dumps({"dispose": disposal, "fault": fault, **result}))

# Independent integer sum, reduced to a signed i32, for the finite CPU probe.
n = 100_000_000
unsigned = n * (n - 1) // 2 % (1 << 32)
expected = str(unsigned if unsigned < (1 << 31) else unsigned - (1 << 32))
control = get("/cpu-control")
assert control == {"cpu_enforced": False, "body": expected}, control
cpu = get("/cpu")
assert cpu["cpu_enforced"] is enforced, cpu
if enforced:
    assert "CPU limit of 1 ms" in cpu["error"], cpu
else:
    assert cpu["body"] == expected, cpu
print(json.dumps({"cpu": cpu, "cpu_control": control}))
print(f"PASS: {args.runtime}: 12 runtime groups, 12 cleanup fault cases, CPU control/probe")
