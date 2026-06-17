#!/usr/bin/env python3
"""Simple HTTP server that can be controlled via POST to toggle health."""
import http.server
import json
import sys
import threading

healthy = True
lock = threading.Lock()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        with lock:
            h = healthy
        if self.path == "/health":
            if h:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')
            else:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"down"}')
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global healthy
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        try:
            data = json.loads(body)
            with lock:
                healthy = bool(data.get("healthy", True))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"healthy": healthy}).encode())
        except Exception as e:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def log_message(self, fmt, *args):
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9991
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"http server on {port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
