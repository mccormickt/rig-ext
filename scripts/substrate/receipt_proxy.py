#!/usr/bin/env python3
"""Disposable, fixed-destination Ollama receipt endpoint on a private bridge."""

import argparse
import datetime
import http.client
import http.server
import json
import threading
import uuid
from urllib.parse import urlsplit


class Receipt(http.server.BaseHTTPRequestHandler):
    lock = threading.Lock()
    calls = 0
    probes = 0
    probe_only = False

    def log_message(self, *args):
        pass

    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        with self.lock:
            if urlsplit(self.path).path == "/probe":
                Receipt.probes += 1
                print(
                    json.dumps(
                        {
                            "time": datetime.datetime.now(datetime.UTC).isoformat(),
                            "event": "probe",
                            "path": self.path,
                            "peer": self.client_address,
                            "probe": self.probes,
                        }
                    ),
                    flush=True,
                )
            elif self.path != "/stats":
                self.reply(404, b"{}")
                return
            self.reply(
                200, json.dumps({"calls": self.calls, "probes": self.probes}).encode()
            )

    def do_POST(self):
        if self.probe_only:
            self.reply(403, b'{"error":"probe-only endpoint"}')
            return
        if self.path != "/api/chat":
            self.reply(404, b"{}")
            return
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 1024 * 1024:
            self.reply(413, b"{}")
            return
        request = json.loads(self.rfile.read(length))
        if request.get("model") != "qwen3.5:9b" or request.get("stream"):
            self.reply(400, b'{"error":"only non-streaming qwen3.5:9b is permitted"}')
            return
        with self.lock:
            Receipt.calls += 1
            call = self.calls
        receipt = {
            "time": datetime.datetime.now(datetime.UTC).isoformat(),
            "id": uuid.uuid4().hex,
            "call": call,
            "upstream": "http://127.0.0.1:11434/api/chat",
            "request": request,
        }
        upstream = http.client.HTTPConnection("127.0.0.1", 11434, timeout=180)
        try:
            upstream.request(
                "POST",
                "/api/chat",
                json.dumps(request),
                {"Content-Type": "application/json"},
            )
            response = upstream.getresponse()
            body = response.read()
            receipt.update(status=response.status, response=json.loads(body))
            print(json.dumps(receipt), flush=True)
            self.reply(response.status, body)
        except (OSError, http.client.HTTPException) as error:
            receipt["error"] = str(error)
            print(json.dumps(receipt), flush=True)
            self.reply(502, b'{"error":"Ollama request failed"}')
        finally:
            upstream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", required=True)
    parser.add_argument("--port", type=int, default=15024)
    parser.add_argument("--probe-only", action="store_true")
    args = parser.parse_args()
    Receipt.probe_only = args.probe_only
    if args.bind in ("0.0.0.0", "::"):
        parser.error("bind to the dedicated Docker bridge address, not all interfaces")
    http.server.ThreadingHTTPServer((args.bind, args.port), Receipt).serve_forever()
