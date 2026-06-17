#!/usr/bin/env python3
"""Tiny webhook server to capture alert calls for R2 testing."""
import http.server
import json
import sys
import threading
from datetime import datetime

LOG_FILE = sys.argv[2] if len(sys.argv) > 2 else "webhook_log.txt"


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        line = f"[{datetime.utcnow().isoformat()}] {self.path} body={body}\n"
        with open(LOG_FILE, "a") as f:
            f.write(line)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, fmt, *args):
        pass  # silence default


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8193
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"webhook server on {port}, log={LOG_FILE}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
