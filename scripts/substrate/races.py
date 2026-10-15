#!/usr/bin/env python3
"""Force admission/drain and concurrent-wake boundaries with a held real tool."""

import argparse
import concurrent.futures
import json
from pathlib import Path

from cluster import Cluster

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    p = Cluster(parser.parse_args())
    try:
        baseline = p.inspect()
        prompt, task, conn, response = p.held()
        response.close()
        conn.close()
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            suspension = pool.submit(p.suspend)
            p.wait(
                "admission closed",
                lambda: p.http("/admin/status", owner="admin")[1]["closed"],
            )
            body = {
                "jsonrpc": "2.0",
                "id": "rejected-during-drain",
                "method": "SendMessage",
                "params": {
                    "message": {
                        "messageId": "rejected-during-drain",
                        "role": "ROLE_USER",
                        "parts": [{"text": p.prompt("must-not-commit")}],
                    }
                },
            }
            p.check(
                "racing turn rejected during drain", p.http("/jsonrpc", body)[0], 503
            )
            p.check("suspension waits for cleanup", suspension.done(), False)
            p.check(
                "actor remains RUNNING during cleanup",
                p.actor()["status"]["state"],
                "ACTOR_STATE_RUNNING",
            )
            p.check("held execution still active", p.inspect()["active"], 1)
            p.http("/release", {})
            before, _, suspended = suspension.result(timeout=90)
            p.check(
                "accepted effect completes before snapshot",
                before["commits"],
                baseline["commits"] + 1,
            )
            p.check(
                "suspended before concurrent wake",
                suspended["status"]["state"],
                "ACTOR_STATE_SUSPENDED",
            )
            a, b = p.prompt("wake-a"), p.prompt("wake-b")
            turns = [pool.submit(p.send, text, task["contextId"]) for text in (a, b)]
            replies = [p.settled(turn.result(timeout=60))["prompts"] for turn in turns]
            p.check(
                "concurrent turns serialize", sorted(len(r) for r in replies), [2, 3]
            )
            longer = max(replies, key=len)
            p.check("held history preserved", longer[0], prompt)
            p.check("both racing prompts retained", sorted(longer[1:]), sorted([a, b]))
            p.check(
                "shorter history is canonical prefix", min(replies, key=len), longer[:2]
            )
        after = p.inspect()
        p.check("exact accepted effects", after["commits"], baseline["commits"] + 3)
        rows = [json.loads(line) for line in after["journal"].splitlines()]
        p.check(
            "rejected turn has no effect",
            any(r["value"] == "must-not-commit" for r in rows),
            False,
        )
        p.check("nonce survived raced wake", after["nonce"], before["nonce"])
        p.record(
            "result",
            status="PASS",
            scope="controlled drain with disconnected held tool and concurrent wake; not a general detached-handler quiescence guarantee",
        )
    except Exception as error:
        p.record("result", status="FAIL", error=str(error), type=type(error).__name__)
        raise
