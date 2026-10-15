#!/usr/bin/env python3
"""Record the real deployed path, including a blocked actor startup."""

import argparse
import http.client
import json
import os
import socket
import uuid
from pathlib import Path

from demo import Proof


def duplicates(proof, port, method, values):
    with socket.create_connection(("127.0.0.1", port), timeout=30) as conn:
        target = "rig-1:80" if method == "CONNECT" else "/inspect"
        headers = "".join(f"ate-target-actor: {value}\r\n" for value in values)
        conn.sendall(
            f"{method} {target} HTTP/1.1\r\nHost: rig-1:80\r\n{headers}\r\n".encode()
        )
        response = http.client.HTTPResponse(conn)
        response.begin()
        outer = response.status
        if method == "CONNECT" and outer == 200:
            conn.sendall(
                b"GET /inspect HTTP/1.1\r\nHost: rig-1:80\r\nConnection: close\r\n\r\n"
            )
            response = http.client.HTTPResponse(conn)
            response.begin()
        body = response.read().decode()
        proof.record(
            "selector_control",
            port=port,
            method=method,
            selectors=values,
            outer_status=outer,
            status=response.status,
            body=body,
        )
        return response.status


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--substrate", default="/tmp/rig-substrate-v0.4.0")
    p = Proof(parser.parse_args())
    gateway_port = int(os.environ.get("GATEWAY_PORT", "15021"))
    connect_port = int(os.environ.get("CONNECT_PORT", "15022"))
    p.provenance()
    p.command("uname", "-r")
    p.ate("get", "actor-template", "rig-poc", "-a", "rig-local", "-o", "json")
    before = p.actor()
    other_before = p.actor("rig-2")
    p.check(
        "probe starts from SUSPENDED",
        before["status"]["state"],
        "ACTOR_STATE_SUSPENDED",
    )
    body = {
        "jsonrpc": "2.0",
        "id": "cluster-wake-" + uuid.uuid4().hex,
        "method": "SendMessage",
        "params": {
            "message": {
                "messageId": uuid.uuid4().hex,
                "role": "ROLE_USER",
                "parts": [{"text": p.prompt("cluster-first-turn")}],
            }
        },
    }
    for owner, headers, path, expected in [
        (None, {}, "/actors/rig-1/jsonrpc", 401),
        (
            None,
            {"Authorization": "Bearer invalid-fixture"},
            "/actors/rig-1/jsonrpc",
            401,
        ),
        ("rig-1", {}, "/actors/rig-2/jsonrpc", 403),
        ("rig-1", {}, "/actors/no-such-actor/jsonrpc", 403),
        (
            "rig-1",
            {"ate-target-actor": "rig-local/rig-1"},
            "/actors/rig-1/jsonrpc",
            400,
        ),
    ]:
        p.check(
            "edge authorization control",
            p.http(path, body, owner, headers)[0],
            expected,
        )
    for method in ("GET", "CONNECT"):
        p.check(
            "edge duplicate selector denied",
            duplicates(p, p.edge_port, method, ["rig-local/rig-1", "rig-local/rig-2"]),
            405 if method == "CONNECT" else 400,
        )
    p.check("denied requests do not wake", p.actor(), before)
    p.check("other denied actor unchanged", p.actor("rig-2"), other_before)
    status, response = p.http("/actors/rig-1/jsonrpc", body)
    after = p.actor()
    p.record(
        "actual_wake",
        http_status=status,
        actor_before=before,
        actor_after=after,
        response=response,
    )
    # Unknown routes must never be answered by a fallback agent.
    bad_status, bad_response = p.http(
        "/jsonrpc",
        body,
        owner=None,
        port=gateway_port,
        headers={"ate-target-actor": "rig-local/does-not-exist"},
    )
    p.check("unknown actor status", bad_status, 404)
    p.check(
        "broken route cannot pass", bad_response, 'actor "does-not-exist" not found'
    )
    for method, port in (("GET", gateway_port), ("CONNECT", connect_port)):
        for values in (
            ["rig-local/does-not-exist"],
            ["rig-local/does-not-exist", "rig-local/rig-1"],
            ["rig-local/rig-1", "rig-local/does-not-exist"],
            ["rig-local/rig-1", "rig-local/rig-1"],
            ["rig-local/rig-1, rig-local/does-not-exist"],
        ):
            actual = duplicates(p, port, method, values)
            if values[0] == "rig-local/does-not-exist":
                p.check("native first unknown selector returns 404", actual, 404)
            elif (
                values[0] == "rig-local/rig-1"
                and after["status"]["state"] == "ACTOR_STATE_RUNNING"
            ):
                p.check(
                    "native duplicate acceptance (failed security expectation)",
                    actual,
                    200,
                )
    actors = json.loads(p.ate("get", "actors", "-a", "rig-local", "-o", "json"))
    p.check(
        "unknown actor not created",
        any(a["metadata"]["name"] == "does-not-exist" for a in actors["actors"]),
        False,
    )
    for namespace, selector in (
        ("rig-local", "ate.dev/worker-pool=rig"),
        ("ate-system", "app=ate-api-server"),
        ("ate-system", "app=atenet-router"),
    ):
        p.command(
            "kubectl",
            "--context",
            p.kube_context,
            "logs",
            "-n",
            namespace,
            "-l",
            selector,
            "--since=15m",
            "--tail=1000",
            "--prefix",
        )
    startup_blocked = (
        status == 500 and after["status"]["state"] == "ACTOR_STATE_CRASHED"
    )
    completed = (
        status == 200
        and isinstance(response, dict)
        and response.get("result", {}).get("task", {}).get("status", {}).get("state")
        == "TASK_STATE_COMPLETED"
    )
    result = (
        "BLOCKED"
        if startup_blocked
        else ("REQUIRES_FULL_DEMO" if completed else "FAIL")
    )
    p.record(
        "result",
        status=result,
        scope="actor execution and FULL snapshot proof",
        reason="inspect actor crash and worker errors above; host dmesg is not required",
    )
    if result in ("FAIL", "BLOCKED"):
        raise SystemExit(2 if result == "BLOCKED" else 1)
