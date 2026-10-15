#!/usr/bin/env python3
"""Public HTTP controls for the disposable edge; no Substrate or inference."""

import http.client
import http.server
import json
import socket
import threading
import time
import unittest

from edge import Edge


class EdgeTest(unittest.TestCase):
    def test_admission_counts_partial_body_and_rejects_closed_work(self):
        class Sink(http.server.BaseHTTPRequestHandler):
            calls = 0

            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                Sink.calls += 1
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        gateway = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Sink)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Edge)
        server.tokens = {"admin": "admin-test", "rig-1": "actor-test"}
        server.gateway_port = gateway.server_port
        Edge.closed, Edge.inflight = False, 0
        for item in (gateway, server):
            threading.Thread(target=item.serve_forever, daemon=True).start()

        def post(path, token, body=b"{}", extra=None):
            conn = http.client.HTTPConnection(
                "127.0.0.1", server.server_port, timeout=5
            )
            conn.request(
                "POST",
                path,
                body,
                {"Authorization": "Bearer " + token, **(extra or {})},
            )
            response = conn.getresponse()
            status, value = response.status, json.loads(response.read())
            conn.close()
            return status, value

        slow = socket.create_connection(server.server_address, timeout=5)
        try:
            slow.sendall(
                b"POST /jsonrpc HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer actor-test\r\nContent-Length: 2\r\n\r\n{"
            )
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                state = post("/admin/status", "admin-test")[1]
                if state["inflight"] == 1:
                    break
                time.sleep(0.01)
            self.assertEqual(state["inflight"], 1)
            self.assertEqual(
                post("/admin/close", "admin-test")[1], {"closed": True, "inflight": 1}
            )
            self.assertEqual(post("/jsonrpc", "actor-test")[0], 503)
            self.assertEqual(post("/actors/rig-1/jsonrpc", "admin-test")[0], 503)
            self.assertEqual(post("/jsonrpc", "wrong")[0], 401)
            self.assertEqual(
                post(
                    "/jsonrpc",
                    "actor-test",
                    extra={"ate-target-actor": "rig-local/rig-1"},
                )[0],
                400,
            )
            slow.sendall(b"}")
            self.assertIn(b"200", slow.recv(4096).split(b"\r\n")[0])
            deadline = time.monotonic() + 2
            while (
                post("/admin/status", "admin-test")[1]["inflight"]
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            self.assertEqual(Sink.calls, 1)
            self.assertEqual(post("/admin/status", "admin-test")[1]["inflight"], 0)
            post("/admin/open", "admin-test")
            self.assertEqual(post("/jsonrpc", "actor-test")[0], 200)
            self.assertEqual(Sink.calls, 2)
        finally:
            slow.close()
            for item in (server, gateway):
                item.shutdown()
                item.server_close()


if __name__ == "__main__":
    unittest.main()
