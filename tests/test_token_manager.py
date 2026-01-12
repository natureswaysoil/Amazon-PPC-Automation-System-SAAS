"""
Unit tests for token management
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone
from automation.shared.token_manager import TokenManager

class TestTokenManager:
    @patch('automation.shared.token_manager.secretmanager.SecretManagerServiceClient')
    def test_needs_refresh_no_token(self, mock_sm):
        """Test refresh needed when no token exists"""
        manager = TokenManager()
        manager.access_token = None
        
        assert manager._needs_refresh() == True
    
    @patch('automation.shared.token_manager.secretmanager.SecretManagerServiceClient')
    def test_needs_refresh_expired(self, mock_sm):
        """Test refresh needed when token expired"""
        manager = TokenManager()
        manager.access_token = "test_token"
        manager.token_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        
        assert manager._needs_refresh() == True
    
    @patch('automation.shared.token_manager.secretmanager.SecretManagerServiceClient')
    def test_needs_refresh_valid(self, mock_sm):
        """Test no refresh needed for valid token"""
        manager = TokenManager()
        manager.access_token = "test_token"
        manager.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        
        assert manager._needs_refresh() == False

    @patch('automation.shared.token_manager.secretmanager.SecretManagerServiceClient')
    def test_refresh_flow_updates_tokens(self, mock_sm):
        """Token refresh obtains access token and rotates refresh token when provided."""
        # Stub Secret Manager client
        instance = mock_sm.return_value
        # Make access_secret_version return an object with payload.data
        class _Payload:
            def __init__(self, data):
                self.data = data
        class _Resp:
            def __init__(self, val):
                self.payload = _Payload(val.encode('UTF-8'))
        instance.access_secret_version.side_effect = [
            _Resp('client_id'),
            _Resp('client_secret'),
            _Resp('refresh_token')
        ]

        manager = TokenManager()

        # Patch requests.post to simulate Amazon token endpoint
        import requests
        from unittest.mock import MagicMock
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "access_token": "new_access",
            "expires_in": 1800,
            "refresh_token": "new_refresh_token"
        }

        # Spy on _update_secret to ensure rotation persisted
        with patch.object(requests, 'post', return_value=mock_response):
            with patch.object(manager, '_update_secret', return_value=True) as update_secret:
                token = manager.force_refresh()
                assert token == "new_access"
                assert manager.access_token == "new_access"
                assert manager.refresh_token == "new_refresh_token"
                # Called with rotated refresh token
                update_secret.assert_called_with("amazon_refresh_token", "new_refresh_token")
