"""
Main entry point for the container.

This provides a simple HTTP server for Cloud Run service health checks.
The actual job logic is in bid_optimizer.py and budget_monitor.py,
which are executed via Cloud Run Jobs with custom --command flags.
"""

import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging

# Configure logging to flush immediately to stdout for Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for health checks"""
    
    def do_GET(self):
        """Handle GET requests"""
        if self.path == "/" or self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            try:
                self.wfile.write(b"OK - Amazon PPC Automation System\n")
                self.wfile.write(b"This container is designed for Cloud Run Jobs.\n")
                self.wfile.write(b"See bid_optimizer.py and budget_monitor.py for job logic.\n")
            except (BrokenPipeError, ConnectionResetError):
                # Client closed connection early, ignore
                pass
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            try:
                self.wfile.write(b"Not Found\n")
            except (BrokenPipeError, ConnectionResetError):
                # Client closed connection early, ignore
                pass
    
    def log_message(self, format, *args):
        """Override to use Python logging"""
        logger.info(f"{self.address_string()} - {format}", *args)


def main():
    """Start HTTP server on PORT from environment"""
    port = int(os.environ.get("PORT", 8080))
    
    logger.info(f"Starting health check server on port {port}")
    logger.info("Note: This container is designed for Cloud Run Jobs")
    logger.info("Jobs should use --command=python,<job_script>.py")
    
    # Create and bind the server
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    
    # Log that server is ready to accept connections
    logger.info(f"Server successfully bound to 0.0.0.0:{port}")
    logger.info("Server is ready to accept connections")
    sys.stdout.flush()  # Ensure logs are immediately visible to Cloud Run
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server")
        server.shutdown()


if __name__ == "__main__":
    main()
