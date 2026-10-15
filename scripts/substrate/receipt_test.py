#!/usr/bin/env python3
"""Reject unrelated RPC evidence without a running cluster."""

import copy
import json
import unittest

from demo import rpc_receipts


class ReceiptTest(unittest.TestCase):
    def test_resume_identity_and_interval(self):
        event = {
            "time": "2026-10-15T12:00:02Z",
            "method": "/ateapi.Control/ResumeActor",
            "resp": {
                "actor": {
                    "metadata": {"uid": "actor-a"},
                    "status": {"worker_assignment": {"worker_pod_uid": "worker-b"}},
                }
            },
        }

        def select(value):
            return rpc_receipts(
                "[pod/api] " + json.dumps(value),
                event["method"],
                "actor-a",
                "2026-10-15T12:00:01+00:00",
                "2026-10-15T12:00:03+00:00",
                "worker-b",
            )

        self.assertEqual(select(event), [event])
        for stamp in ("2026-10-15T12:00:00Z", "2026-10-15T12:00:04Z"):
            self.assertEqual(select({**event, "time": stamp}), [])
        other = copy.deepcopy(event)
        other["resp"]["actor"]["status"]["worker_assignment"]["worker_pod_uid"] = (
            "worker-c"
        )
        self.assertEqual(select(other), [])
        other = copy.deepcopy(event)
        other["resp"]["actor"]["metadata"]["uid"] = "actor-b"
        self.assertEqual(select(other), [])

    def test_cancellation_from_other_actor_is_not_evidence(self):
        event = {
            "time": "2026-10-15T12:00:02Z",
            "method": "/ateom.Ateom/RestoreWorkload",
            "req": {"actor_uid": "actor-a"},
            "err": "wakeup probe cancelled",
        }

        def select(value):
            return rpc_receipts(
                json.dumps(value),
                event["method"],
                "actor-a",
                "2026-10-15T12:00:01Z",
                "2026-10-15T12:00:03Z",
            )

        self.assertEqual(select(event), [event])
        self.assertEqual(select({**event, "req": {"actor_uid": "actor-b"}}), [])
        self.assertEqual(select({**event, "time": "2026-10-15T11:00:02Z"}), [])


if __name__ == "__main__":
    unittest.main()
