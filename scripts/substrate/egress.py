#!/usr/bin/env python3
"""Default-deny and exact local allow rule, with independent sink counters."""

import argparse
import json
import urllib.request
from pathlib import Path

from demo import Proof

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--sink", required=True, help="bridge URL of receipt_proxy.py")
    proof = Proof(parser.parse_args())

    def sink(path):
        with urllib.request.urlopen(proof.args.sink + path, timeout=5) as response:
            value = json.load(response)
        proof.record("independent_sink", path=path, value=value)
        return value

    try:
        before = sink("/stats")
        positive = sink("/probe")
        proof.check("sink live before denial", positive["probes"], before["probes"] + 1)
        for actor in ("rig-1", "rig-2"):
            status, denied = proof.http(f"/actors/{actor}/egress", owner="admin")
            proof.check("actor egress handler reached", status, 200)
            proof.check("default-deny explicit HTTP denial", denied, {"status": 403})
        proof.check("denied traffic did not reach sink", sink("/stats"), positive)
        policy = proof.out / "allow.json"
        policy.write_text(
            json.dumps(
                {
                    "rules": [
                        {
                            "http": {
                                "hostnames": ["ollama.rig-local.svc"],
                                "ports": {"numbers": [15024]},
                            }
                        }
                    ]
                }
            )
        )
        proof.ate(
            "create", "egress-policy", "rig-1", "-a", "rig-local", "-f", str(policy)
        )
        proof.ate("get", "egress-policy", "rig-1", "-a", "rig-local", "-o", "json")
        status, allowed = proof.http("/actors/rig-1/egress", owner="admin")
        proof.check("allowed HTTP handler", status, 200)
        proof.check("narrow allowed destination", allowed, {"status": 200})
        after = sink("/stats")
        proof.check(
            "one independent permitted probe", after["probes"], positive["probes"] + 1
        )
        proof.check("probe is not inference", after["calls"], positive["calls"])
        proof.check(
            "other actor remains denied",
            proof.http("/actors/rig-2/egress", owner="admin")[1],
            {"status": 403},
        )
        proof.check("other actor did not reach sink", sink("/stats"), after)
        for path in ("wrong-host", "wrong-port"):
            proof.check(
                "excluded " + path,
                proof.http("/actors/rig-1/egress/" + path, owner="admin")[1],
                {"status": 403},
            )
        proof.check("excluded requests did not reach sink", sink("/stats"), after)
        control = proof.out / "control-allow.json"
        control.write_text(
            json.dumps(
                {
                    "rules": [
                        {
                            "http": {
                                "hostnames": [
                                    "ollama.rig-local.svc",
                                    "ollama.rig-local.svc.cluster.local",
                                ],
                                "ports": {"numbers": [15024, 15027]},
                            }
                        }
                    ]
                }
            )
        )
        proof.ate(
            "create", "egress-policy", "rig-2", "-a", "rig-local", "-f", str(control)
        )
        for path in ("wrong-host", "wrong-port"):
            proof.check(
                "excluded destination live under control grant " + path,
                proof.http("/actors/rig-2/egress/" + path, owner="admin")[1],
                {"status": 200},
            )
        proof.check(
            "independent excluded-destination controls",
            sink("/stats")["probes"],
            after["probes"] + 2,
        )
        proof.ate("delete", "egress-policy", "rig-2", "-a", "rig-local")
        proof.check(
            "control grant removed",
            proof.http("/actors/rig-2/egress", owner="admin")[1],
            {"status": 403},
        )
        proof.suspend()
        proof.record(
            "result",
            status="PASS",
            scope="two actors, HTTP default-deny and one host/port allow; DNS port53 is outside this contract",
        )
    except Exception as error:
        proof.record(
            "result", status="FAIL", error=str(error), type=type(error).__name__
        )
        raise
