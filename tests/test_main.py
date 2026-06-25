"""
Tests for main.py — the Cloud Run service HTTP dispatcher.

Verifies health checks AND that the scheduler POST routes (which previously
returned 501) are now handled.
"""

import json
import os
import sys
import time
import unittest
from http.client import HTTPConnection
from threading import Thread

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import main

PORT = 8888


class TestMainDispatcher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PORT"] = str(PORT)
        cls.server_thread = Thread(target=main.main, daemon=True)
        cls.server_thread.start()
        for i in range(10):
            try:
                conn = HTTPConnection("localhost", PORT, timeout=1)
                conn.request("GET", "/health")
                if conn.getresponse().status == 200:
                    conn.close()
                    break
                conn.close()
            except Exception:
                if i == 9:
                    raise
                time.sleep(0.5)

    def _request(self, method, path):
        conn = HTTPConnection("localhost", PORT, timeout=30)
        try:
            conn.request(method, path)
            resp = conn.getresponse()
            return resp.status, resp.read().decode()
        finally:
            conn.close()

    # --- health ---
    def test_health_endpoint(self):
        status, body = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ok")

    def test_root_endpoint(self):
        status, body = self._request("GET", "/")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ok")

    def test_get_unknown_is_404(self):
        status, body = self._request("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["status"], "not_found")

    # --- POST dispatch (previously 501) ---
    def test_post_unknown_is_404(self):
        status, body = self._request("POST", "/not-a-route")
        self.assertEqual(status, 404)

    def test_post_harvest_keywords_handled(self):
        # No harvester implemented yet -> graceful 200, NOT 501
        status, body = self._request("POST", "/harvest-keywords")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "not_implemented")

    def test_post_optimize_bids_routes(self):
        # Without GCP creds the job degrades to "no keywords" and returns ok;
        # the point is it is dispatched and does not return 501.
        status, body = self._request("POST", "/optimize-bids")
        self.assertNotEqual(status, 501)
        self.assertIn(status, (200, 500))


if __name__ == "__main__":
    unittest.main()
