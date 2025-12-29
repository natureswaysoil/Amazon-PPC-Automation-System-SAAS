"""
Tests for main.py health check server
"""

import unittest
import os
import sys
from http.client import HTTPConnection
from threading import Thread
import time

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import main


class TestMainHealthCheck(unittest.TestCase):
    """Test the main.py health check server"""
    
    @classmethod
    def setUpClass(cls):
        """Start server in background thread"""
        os.environ['PORT'] = '8888'  # Use non-standard port for testing
        cls.server_thread = Thread(target=main.main, daemon=True)
        cls.server_thread.start()
        
        # Wait for server to be ready with retry logic
        max_retries = 10
        for i in range(max_retries):
            try:
                conn = HTTPConnection('localhost', 8888, timeout=1)
                conn.request('GET', '/health')
                response = conn.getresponse()
                conn.close()
                if response.status == 200:
                    break
            except Exception:
                if i == max_retries - 1:
                    raise
                time.sleep(0.5)
    
    def test_health_endpoint(self):
        """Test /health endpoint returns 200"""
        conn = HTTPConnection('localhost', 8888)
        try:
            conn.request('GET', '/health')
            response = conn.getresponse()
            
            self.assertEqual(response.status, 200)
            body = response.read().decode()
            self.assertIn('OK', body)
            self.assertIn('Amazon PPC Automation System', body)
        finally:
            conn.close()
    
    def test_root_endpoint(self):
        """Test / endpoint returns 200"""
        conn = HTTPConnection('localhost', 8888)
        try:
            conn.request('GET', '/')
            response = conn.getresponse()
            
            self.assertEqual(response.status, 200)
            body = response.read().decode()
            self.assertIn('OK', body)
        finally:
            conn.close()
    
    def test_not_found(self):
        """Test unknown endpoint returns 404"""
        conn = HTTPConnection('localhost', 8888)
        try:
            conn.request('GET', '/unknown')
            response = conn.getresponse()
            
            self.assertEqual(response.status, 404)
            body = response.read().decode()
            self.assertIn('Not Found', body)
        finally:
            conn.close()


if __name__ == '__main__':
    unittest.main()
