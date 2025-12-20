"""
Unit tests for token management
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
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
        manager.token_expires_at = datetime.utcnow() - timedelta(minutes=1)
        
        assert manager._needs_refresh() == True
    
    @patch('automation.shared.token_manager.secretmanager.SecretManagerServiceClient')
    def test_needs_refresh_valid(self, mock_sm):
        """Test no refresh needed for valid token"""
        manager = TokenManager()
        manager.access_token = "test_token"
        manager.token_expires_at = datetime.utcnow() + timedelta(hours=1)
        
        assert manager._needs_refresh() == False
