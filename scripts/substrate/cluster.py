#!/usr/bin/env python3
"""Run deterministic protocol controls through the protected deployed edge."""

import argparse
from pathlib import Path

from demo import Proof
from local import Local


class Cluster(Local):
    cluster = True

    def http(
        self, path, body=None, owner="rig-1", headers=None, method=None, port=None
    ):
        if path in ("/inspect", "/release"):
            path, owner = "/actors/rig-1" + path, "admin"
        return Proof.http(self, path, body, owner, headers, method, port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument(
        "--binary", type=Path, default=Path("target/debug/examples/substrate_actor")
    )
    parser.add_argument("--substrate", required=True)
    parser.add_argument("--build", required=True)
    proof = Cluster(parser.parse_args())
    try:
        proof.provenance()
        for actor in ("rig-1", "rig-2"):
            state = proof.inspect(actor)
            proof.check("actor build", state["build"], proof.args.build)
            proof.check("empty actor journal", state["journal"], "")
            proof.check("empty actor nonce", state["nonce"], None)
        proof.run()
        proof.record(
            "result",
            status="PASS",
            scope="deployed deterministic A2A mechanics; not a concurrent suspension barrier",
        )
    except Exception as error:
        proof.record(
            "result", status="FAIL", error=str(error), type=type(error).__name__
        )
        raise
