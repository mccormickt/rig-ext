#!/usr/bin/env python3
"""Reject unrelated RPC evidence without a running cluster."""

import copy
import http.client
import http.server
import json
import subprocess
import threading
import unittest
from pathlib import Path

from demo import rpc_receipts
from receipt_proxy import Receipt


class ReceiptTest(unittest.TestCase):
    def test_setup_refuses_external_config_before_tools(self):
        result = subprocess.run(
            ["/bin/bash", str(Path(__file__).with_name("setup.sh"))],
            env={"ATE_CONFIG": "/do-not-load", "PATH": "/no-executables"},
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("ATE_CONFIG", result.stderr)

    def test_probe_only_has_no_provider_calls(self):
        Receipt.probe_only = True
        Receipt.calls = Receipt.probes = 0
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Receipt)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        client = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        try:
            client.request("GET", "/probe?nonce=isolated-control")
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read()), {"calls": 0, "probes": 1})
            client.request("POST", "/api/chat", "{}")
            response = client.getresponse()
            self.assertEqual(response.status, 403)
            response.read()
            client.request("GET", "/stats")
            response = client.getresponse()
            self.assertEqual(json.loads(response.read()), {"calls": 0, "probes": 1})
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()
            Receipt.probe_only = False

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
