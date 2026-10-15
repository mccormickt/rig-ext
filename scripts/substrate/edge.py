#!/usr/bin/env python3
"""Loopback-only POC trust boundary. No fallback model or actor provisioning."""

import argparse
import http.client
import http.server
import json
import threading
import time
from pathlib import Path


class Edge(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    lock = threading.Lock()
    closed = False
    inflight = 0

    def log_message(self, *args):
        pass

    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)

    def do_CONNECT(self):
        self.reply(405, {"error": "CONNECT is not exposed"})

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        # Public actor selectors are never used, even if equal to the binding.
        if self.headers.get_all("ate-target-actor"):
            self.reply(400, {"error": "client actor selector forbidden"})
            return
        auth = self.headers.get_all("Authorization", [])
        if len(auth) != 1 or not auth[0].startswith("Bearer "):
            self.reply(401, {"error": "authentication required"})
            return
        token = auth[0][7:]
        owner = next(
            (name for name, secret in self.server.tokens.items() if secret == token),
            None,
        )
        if owner is None:
            self.reply(401, {"error": "invalid credential"})
            return
        if self.path in ("/admin/close", "/admin/open", "/admin/status"):
            if owner != "admin":
                self.reply(403, {"error": "admin required"})
                return
            with Edge.lock:
                if self.path == "/admin/close":
                    Edge.closed = True
                elif self.path == "/admin/open":
                    Edge.closed = False
                status = {"closed": Edge.closed, "inflight": Edge.inflight}
            self.reply(200, status)
            return
        if self.path in ("/jsonrpc", "/.well-known/agent-card.json"):
            actor, path = owner, self.path
        else:
            segments = self.path.split("/", 3)
            if len(segments) != 4 or segments[1] != "actors":
                self.reply(404, {"error": "route not found"})
                return
            actor, path = segments[2], "/" + segments[3]
        if actor not in ("rig-1", "rig-2") or owner not in (actor, "admin"):
            self.reply(403, {"error": "actor not authorized"})
            return
        if owner != "admin" and not (
            path == "/jsonrpc" or path == "/.well-known/agent-card.json"
        ):
            self.reply(403, {"error": "control endpoint not exposed"})
            return
        lengths = self.headers.get_all("Content-Length", [])
        if self.headers.get("Transfer-Encoding") or len(lengths) > 1:
            self.reply(400, {"error": "ambiguous framing"})
            return
        try:
            length = int(lengths[0]) if lengths else 0
            if not 0 <= length <= 1024 * 1024:
                raise ValueError()
        except ValueError:
            self.reply(413, {"error": "invalid body length"})
            return
        admitted = False
        with Edge.lock:
            drain_control = owner == "admin" and (
                (
                    self.command == "GET"
                    and (path == "/inspect" or path.startswith("/history/"))
                )
                or (self.command == "POST" and path == "/release")
            )
            if not Edge.closed or drain_control:
                Edge.inflight += 1
                admitted = True
        if not admitted:
            self.reply(503, {"error": "admission closed"})
            return
        correlation = self.headers.get("X-Request-ID", "missing")
        print(
            json.dumps(
                {
                    "time": time.time(),
                    "event": "forward",
                    "request": correlation,
                    "actor": actor,
                    "path": path,
                }
            ),
            flush=True,
        )
        conn = http.client.HTTPConnection(
            "127.0.0.1", self.server.gateway_port, timeout=120
        )
        started = False
        try:
            self.connection.settimeout(10)
            body = self.rfile.read(length)
            if len(body) != length:
                self.reply(400, {"error": "incomplete body"})
                return
            headers = {
                "ate-target-actor": "rig-local/" + actor,
                "X-Request-ID": correlation,
            }
            for name in ("Content-Type", "A2A-Version", "traceparent"):
                if name in self.headers:
                    headers[name] = self.headers[name]
            conn.request(self.command, path, body, headers)
            response = conn.getresponse()
            self.send_response(response.status)
            self.send_header(
                "Content-Type", response.getheader("Content-Type", "application/json")
            )
            self.send_header("Connection", "close")
            self.end_headers()
            started = True
            self.close_connection = True
            while chunk := response.read1(65536):
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except (OSError, http.client.HTTPException) as error:
            if not started:
                self.reply(502, {"error": type(error).__name__})
        finally:
            conn.close()
            with Edge.lock:
                Edge.inflight -= 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--gateway-port", type=int, default=18080)
    args = parser.parse_args()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Edge)
    server.tokens = json.loads(args.tokens.read_text())
    server.gateway_port = args.gateway_port
    server.serve_forever()
