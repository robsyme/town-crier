#!/usr/bin/env python3
"""
Simple HTTP server for receiving Town Crier webhook notifications.

Usage:
    python server.py [port]

Then update your nextflow.config:
    towncrier.endpoint = 'http://localhost:8080'
"""

import json
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ANSI colors for pretty output
COLORS = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "dim": "\033[2m",
    "cyan": "\033[36m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "magenta": "\033[35m",
    "blue": "\033[34m",
}


def colorize(text, *styles):
    prefix = "".join(COLORS.get(s, "") for s in styles)
    return f"{prefix}{text}{COLORS['reset']}"


class WebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, _format, *_args):
        # Suppress default logging
        pass

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            payload = json.loads(body)
            self.print_event(payload)
        except json.JSONDecodeError:
            print(colorize("Failed to parse JSON:", "yellow"), body[:200])

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

    def print_event(self, payload):
        now = datetime.now().strftime("%H:%M:%S")

        print()
        print(colorize("━" * 60, "dim"))
        print(
            colorize(f"[{now}]", "dim"),
            colorize("FILE PUBLISHED", "bold", "green"),
        )
        print(colorize("━" * 60, "dim"))

        # Process and target file
        process = payload.get("process", "unknown")
        target = payload.get("target", "")
        if target.startswith("file://"):
            target = target[7:]  # Strip file:// prefix for readability

        print(f"  {colorize('Process:', 'cyan')} {colorize(process, 'bold')}")
        print(f"  {colorize('File:', 'cyan')}    {target}")

        # Labels if present
        labels = payload.get("labels", [])
        if labels:
            print(f"  {colorize('Labels:', 'cyan')}  {', '.join(labels)}")

        # Metadata
        metadata = payload.get("metadata", {})
        if metadata:
            print(f"  {colorize('Metadata:', 'cyan')}")
            for key, value in metadata.items():
                if isinstance(value, dict):
                    print(f"    {colorize(key + ':', 'magenta')}")
                    for k, v in value.items():
                        print(f"      {k}: {colorize(str(v), 'yellow')}")
                else:
                    print(f"    {key}: {colorize(str(value), 'yellow')}")

        # Workflow info
        workflow = payload.get("workflow", {})
        run_name = workflow.get("runName", "")
        if run_name:
            print(f"  {colorize('Run:', 'cyan')}     {run_name}")

        print()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = HTTPServer(("", port), WebhookHandler)

    print()
    print(colorize("🔔 Town Crier Test Server", "bold"))
    print(colorize("━" * 40, "dim"))
    print(f"  Listening on: {colorize(f'http://localhost:{port}', 'cyan')}")
    print()
    print(colorize("  Update your nextflow.config:", "dim"))
    print(colorize(f"    towncrier.endpoint = 'http://localhost:{port}'", "yellow"))
    print()
    print(colorize("  Press Ctrl+C to stop", "dim"))
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(colorize("\n\nServer stopped.", "dim"))
        sys.exit(0)


if __name__ == "__main__":
    main()
