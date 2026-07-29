#!/usr/bin/env python3
"""Minimal Braintrust API used by the real-session release smoke test."""

import gzip
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

port = int(os.environ.get("MOCK_COLLECTOR_PORT", "53999"))
summary_path = Path(os.environ["MOCK_COLLECTOR_OUT"])
summary = {"logs3Requests": 0, "totalRows": 0}


def save_summary() -> None:
    summary_path.write_text(json.dumps(summary), encoding="utf-8")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        print(f"mock-braintrust: {format % args}", flush=True)

    def send_json(self, value: object) -> None:
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/version":
            self.send_json({"logs3_payload_max_bytes": None})
        else:
            self.send_json({})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        if self.path == "/api/apikey/login":
            base = f"http://127.0.0.1:{port}"
            self.send_json(
                {
                    "org_info": [
                        {
                            "id": "smoke-org",
                            "name": "smoke",
                            "api_url": base,
                            "proxy_url": base,
                        }
                    ]
                }
            )
            return
        if self.path == "/api/project/register":
            self.send_json(
                {
                    "project": {
                        "id": "00000000-0000-0000-0000-000000000000",
                        "name": "smoke",
                    }
                }
            )
            return
        if self.path in ("/logs3", "/logs3/overflow"):
            try:
                rows = json.loads(body or b"{}").get("rows", [])
            except (json.JSONDecodeError, AttributeError):
                rows = []
            summary["logs3Requests"] += 1
            summary["totalRows"] += len(rows)
            save_summary()
            print(
                f"mock-braintrust: received {len(rows)} row(s), "
                f"{summary['totalRows']} total",
                flush=True,
            )
        self.send_json({})


save_summary()
ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
