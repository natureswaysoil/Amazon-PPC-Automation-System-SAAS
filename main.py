"""
HTTP entry point for the Cloud Run **service**.

Cloud Scheduler triggers optimization by POSTing to route paths on this
service (e.g. POST /optimize-bids). The previous version only implemented
GET for health checks, so every scheduled POST returned 501 and the jobs
never ran. This dispatcher maps each route to its job runner.

Routes:
    GET  /              -> health check
    GET  /health        -> health check
    POST /optimize-bids -> bid_optimizer.main()
    POST /monitor-budget-> budget_monitor.main()
    POST /harvest-keywords -> keyword harvester (graceful no-op if absent)

Jobs run synchronously within the request (Cloud Run keeps CPU allocated
during a request; background work after the response can be throttled, so
inline is the safe choice). Job runners may call sys.exit(1) on failure;
that SystemExit is caught here and turned into an HTTP 500.
"""

import json
import os
import sys
import time
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Make the app root importable (Dockerfile WORKDIR is /app)
sys.path.insert(0, "/app")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _run_optimize_bids():
    from bid_optimizer import main as run
    run()
    return {"job": "optimize-bids", "status": "ok"}


def _run_monitor_budget():
    from budget_monitor import main as run
    run()
    return {"job": "monitor-budget", "status": "ok"}


def _run_harvest_keywords():
    # No harvester module exists yet. Respond honestly without hard-failing
    # the scheduler every hour. Wire this up when keyword_harvester lands.
    try:
        from keyword_harvester import main as run  # type: ignore
    except Exception:
        logger.info("harvest-keywords requested but no harvester is implemented yet")
        return {"job": "harvest-keywords", "status": "not_implemented"}
    run()
    return {"job": "harvest-keywords", "status": "ok"}


# Map POST routes to job runners
ROUTES = {
    "/optimize-bids": _run_optimize_bids,
    "/monitor-budget": _run_monitor_budget,
    "/harvest-keywords": _run_harvest_keywords,
}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if self.path in ("/", "/health"):
            self._send(200, {"status": "ok", "service": "amazon-ppc-automation"})
        else:
            self._send(404, {"status": "not_found", "path": self.path})

    def do_POST(self):
        # Normalize path (ignore any query string)
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        runner = ROUTES.get(path)
        if runner is None:
            self._send(404, {"status": "not_found", "path": path})
            return

        # Drain request body if present (scheduler may send one)
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length:
            try:
                self.rfile.read(length)
            except Exception:
                pass

        logger.info("▶️ Running job for %s", path)
        started = time.time()
        try:
            result = runner()
            result["duration_s"] = round(time.time() - started, 2)
            logger.info("✅ Job %s finished in %.2fs", path, result["duration_s"])
            self._send(200, result)
        except SystemExit as e:
            # Job runners call sys.exit(1) on failure
            code = e.code if isinstance(e.code, int) else 1
            dur = round(time.time() - started, 2)
            logger.error("❌ Job %s exited with code %s after %.2fs", path, code, dur)
            self._send(500, {"status": "failed", "path": path, "exit_code": code, "duration_s": dur})
        except Exception as e:
            dur = round(time.time() - started, 2)
            logger.exception("❌ Job %s raised after %.2fs", path, dur)
            self._send(500, {"status": "error", "path": path, "error": str(e), "duration_s": dur})

    def log_message(self, fmt, *args):
        logger.info("%s - %s", self.address_string(), fmt % args)


def main():
    port = int(os.environ.get("PORT", 8080))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    logger.info("Listening on 0.0.0.0:%s (routes: %s)", port, ", ".join(sorted(ROUTES)))
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
