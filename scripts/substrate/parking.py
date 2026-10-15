#!/usr/bin/env python3
"""Native gateway parking at max=1, with a fixed one-worker pool and no HPA."""

import argparse
import concurrent.futures
import json
import urllib.request
import uuid
from pathlib import Path

from demo import Proof

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--metrics-port", type=int, default=15025)
    p = Proof(parser.parse_args())

    def parked():
        with urllib.request.urlopen(
            f"http://127.0.0.1:{p.args.metrics_port}/metrics", timeout=5
        ) as response:
            lines = response.read().decode().splitlines()
        samples = [
            line
            for line in lines
            if line.startswith("agentgateway_substrate_request_parking_active")
        ]
        p.record("parking_gauge", samples=samples)
        return sum(float(line.split()[-1]) for line in samples) >= 1

    def body(value):
        identity = uuid.uuid4().hex
        return {
            "jsonrpc": "2.0",
            "id": identity,
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": identity,
                    "role": "ROLE_USER",
                    "parts": [{"text": p.prompt(value)}],
                }
            },
        }

    try:
        config = p.command(
            "kubectl",
            "--context",
            p.kube_context,
            "-n",
            "ate-system",
            "get",
            "configmap",
            "atenet-router-agentgateway-config",
            "-o",
            "json",
        )
        p.check(
            "native small parking capacity configured",
            "max: 1," in json.loads(config)["data"]["config.yaml"],
            True,
        )
        pool = json.loads(
            p.command(
                "kubectl",
                "--context",
                p.kube_context,
                "-n",
                "rig-local",
                "get",
                "workerpool",
                "rig",
                "-o",
                "json",
            )
        )
        p.check("fixed worker pool", pool["spec"]["replicas"], 1)
        hpa = json.loads(
            p.command(
                "kubectl", "--context", p.kube_context, "get", "hpa", "-A", "-o", "json"
            )
        )
        p.check("no Kubernetes autoscaler", hpa["items"], [])
        for actor in ("rig-1", "rig-2"):
            p.check(
                "fresh suspended actor",
                p.actor(actor)["status"]["state"],
                "ACTOR_STATE_SUSPENDED",
            )
        p.settled(p.send(p.prompt("occupy-worker")))
        p.check("occupying actor cleanup done", p.inspect()["active"], 0)
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            request = body("parked-effect")
            waiting = executor.submit(p.http, "/actors/rig-2/jsonrpc", request, "rig-2")
            p.wait("one request parked", parked, timeout=4)
            status, response = p.http(
                "/actors/rig-2/jsonrpc", body("overflow-must-not-run"), "rig-2"
            )
            p.check("parking overflow rejected", status, 503)
            p.check(
                "capacity error specific",
                "request parking capacity exhausted" in response,
                True,
            )
            p.check("first request still waiting", waiting.done(), False)
            p.ate("suspend", "actor", "rig-1", "-a", "rig-local")
            status, response = waiting.result(timeout=20)
            p.check("parked request served after release", status, 200)
            p.check(
                "parked A2A task completed",
                response["result"]["task"]["status"]["state"],
                "TASK_STATE_COMPLETED",
            )
        state = p.inspect("rig-2")
        p.check(
            "one effect, no overflow effect",
            (state["invocations"], state["commits"]),
            (1, 1),
        )
        p.check(
            "exact parked operation",
            json.loads(state["journal"])["operation"],
            json.loads(request["params"]["message"]["parts"][0]["text"])["operation"],
        )
        timed_request = body("budget-must-not-run")
        status, response = p.http("/actors/rig-1/jsonrpc", timed_request)
        p.check("parking wait budget", status, 504)
        p.check("budget error specific", "request timed out" in response, True)
        p.check(
            "budget target stays suspended",
            p.actor()["status"]["state"],
            "ACTOR_STATE_SUSPENDED",
        )
        p.check("other actor cleanup done", p.inspect("rig-2")["active"], 0)
        p.ate("suspend", "actor", "rig-2", "-a", "rig-local")
        p.settled(p.send(p.prompt("after-timeout")))
        p.wait("fresh request cleanup", lambda: p.inspect()["active"] == 0)
        recovered = p.inspect()
        p.check(
            "no late invocation or effect",
            (recovered["invocations"], recovered["commits"]),
            (2, 2),
        )
        p.check(
            "only accepted operations after capacity release",
            [json.loads(line)["value"] for line in recovered["journal"].splitlines()],
            ["occupy-worker", "after-timeout"],
        )
        p.command(
            "kubectl",
            "--context",
            p.kube_context,
            "-n",
            "rig-local",
            "get",
            "workerpool",
            "rig",
            "-o",
            "json",
        )
        p.record(
            "result",
            status="PASS",
            scope="native request parking and fixed capacity, not Kubernetes autoscaling",
        )
    except Exception as error:
        p.record("result", status="FAIL", error=str(error), type=type(error).__name__)
        raise
