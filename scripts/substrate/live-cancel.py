#!/usr/bin/env python3
"""Real Ollama dispatch, held file effect, and cancellation cleanup receipts."""

import argparse
import concurrent.futures
import json
import urllib.request
from pathlib import Path

from cluster import Cluster

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--sink", required=True)
    p = Cluster(parser.parse_args())

    def calls():
        with urllib.request.urlopen(p.args.sink + "/stats", timeout=5) as response:
            state = json.load(response)
        p.record("independent_model_counter", **state)
        return state["calls"]

    try:
        before, model_calls = p.inspect(), calls()
        prompt, task, conn, response = p.held()
        p.check("one real inference before held effect", calls(), model_calls + 1)
        p.check(
            "live tool invoked", p.inspect()["invocations"], before["invocations"] + 1
        )
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            cancellation = executor.submit(p.rpc, "CancelTask", {"id": task["id"]})
            p.wait(
                "task marked canceled before cleanup",
                lambda: (
                    p.rpc("GetTask", {"id": task["id"]})["result"]["status"]["state"]
                    == "TASK_STATE_CANCELED"
                ),
            )
            p.check("cancellation waits for live effect", cancellation.done(), False)
            p.check("canceled is not quiescent", p.inspect()["active"], 1)
            p.http("/release", {})
            cancellation.result(timeout=30)
        p.wait("live cancellation cleanup", lambda: p.inspect()["active"] == 0)
        p.check(
            "accepted live effect committed once",
            p.inspect()["commits"],
            before["commits"] + 1,
        )
        p.check(
            "cancellation stopped next real model request", calls(), model_calls + 1
        )
        response.close()
        conn.close()
        p.record(
            "result",
            status="PASS",
            scope="cancellation during a live model's tool effect; does not abort a running remote inference",
        )
    except Exception as error:
        p.record("result", status="FAIL", error=str(error), type=type(error).__name__)
        raise
