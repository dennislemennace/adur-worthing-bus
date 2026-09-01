#!/usr/bin/env python3
"""
Preview server: serves the site AND proxies the API through one origin.

    python scripts/dev_server.py                 # http://<your-lan-ip>:8765
    python scripts/dev_server.py --api http://localhost:8000

Why this exists rather than `python -m http.server`:

Previewing on a phone means the page is served from a LAN address, and a
page on 192.168.x.x calling the deployed API is a cross-origin request the
API is right to refuse. Running the API locally instead sidesteps CORS but
needs a BODS key, so no live buses appear.

Proxying /api/* through this server solves both. The browser only ever
talks to one origin, so there is no CORS to satisfy, and the upstream call
is server-to-server, which carries the deployed API's own credentials and
therefore its live data.

Static files are served with no-store, because the whole point of a
preview is seeing the change you just made.
"""
import argparse
import http.server
import socketserver
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_API = "https://adur-worthing-bus.onrender.com"


def make_handler(api_base: str):
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=str(ROOT), **kw)

        def do_GET(self):
            if self.path.startswith("/api/"):
                return self._proxy()
            return super().do_GET()

        def _proxy(self):
            url = api_base.rstrip("/") + self.path
            try:
                with urllib.request.urlopen(url, timeout=120) as up:
                    body = up.read()
                    self.send_response(up.status)
                    self.send_header("Content-Type",
                                     up.headers.get("Content-Type", "application/json"))
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(body)
            except urllib.error.HTTPError as err:
                body = err.read()
                self.send_response(err.code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as err:                      # noqa: BLE001
                msg = f'{{"detail":"proxy failed: {err}"}}'.encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)

        def end_headers(self):
            if not self.path.startswith("/api/"):
                self.send_header("Cache-Control", "no-store, must-revalidate")
            super().end_headers()

        def log_message(self, fmt, *args):
            if "/api/" in (args[0] if args else ""):
                super().log_message(fmt, *args)

    return Handler


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> None:
    ap = argparse.ArgumentParser(description="Preview server with an API proxy.")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--api", default=DEFAULT_API,
                    help=f"upstream API to proxy /api/* to (default {DEFAULT_API})")
    args = ap.parse_args()

    with Server(("0.0.0.0", args.port), make_handler(args.api)) as httpd:
        print(f"Serving {ROOT} on 0.0.0.0:{args.port}")
        print(f"Proxying /api/* -> {args.api}")
        httpd.serve_forever()


if __name__ == "__main__":
    main()
