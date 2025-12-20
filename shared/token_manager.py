"""
Amazon Ads API token management with automatic refresh
Stores refreshed tokens back to Secret Manager
"""

import requests
from datetime import datetime, timedelta
from google.cloud import secretmanager
from typing import Optional
import json
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class TokenManager:
    """
    Manages Amazon Advertising API access tokens
    - Fetches from Secret Manager
    - Refreshes before expiration
    - Stores new refresh tokens back to Secret Manager
    """
    
    TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    TOKEN_EXPIRY_BUFFER = 300  # Refresh 5 minutes before expiry
    
    def __init__(self):
        self.project_id = settings.project_id
        self.sm_client = secretmanager.SecretManagerServiceClient()
        
        # Token state
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
        # Credentials
        self.client_id = self._get_secret("amazon_client_id")
        self.client_secret = self._get_secret("amazon_client_secret")
        
        # Load initial refresh token
        self.refresh_token = self._get_secret("amazon_refresh_token")
    
    def _get_secret(self, secret_name: str) -> str:
        """Fetch secret from Google Secret Manager"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
            response = self.sm_client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8")
            logger.info(f"✅ Retrieved secret: {secret_name}")
            return secret_value
        except Exception as e:
            logger.error(f"❌ Error fetching secret {secret_name}: {e}")
            raise
    
    def _update_secret(self, secret_name: str, new_value: str):
        """
        Update secret in Google Secret Manager
        Creates a new version of the secret
        """
        try:
            parent = f"projects/{self.project_id}/secrets/{secret_name}"
            
            # Add new version
            response = self.sm_client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {"data": new_value.encode("UTF-8")}
                }
            )
            
            logger.info(f"✅ Updated secret: {secret_name} (version: {response.name})")
            return True
        except Exception as e:
            logger.error(f"❌ Error updating secret {secret_name}: {e}")
            return False
    
    def get_valid_access_token(self) -> str:
        """
        Get a valid access token
        Refreshes automatically if expired or about to expire
        
        Returns:
            Valid access token string
        """
        # Check if we need to refresh
        if self._needs_refresh():
            logger.info("🔄 Access token expired or missing, refreshing...")
            self._refresh_access_token()
        else:
            logger.info("✅ Using cached access token")
        
        return self.access_token
    
    def _needs_refresh(self) -> bool:
        """Check if token needs refresh"""
        if not self.access_token:
            return True
        
        if not self.token_expires_at:
            return True
        
        # Refresh if within buffer time of expiry
        time_until_expiry = (self.token_expires_at - datetime.utcnow()).total_seconds()
        needs_refresh = time_until_expiry < self.TOKEN_EXPIRY_BUFFER
        
        if needs_refresh:
            logger.info(f"Token expires in {time_until_expiry:.0f}s, refreshing...")
        
        return needs_refresh
    
    def _refresh_access_token(self):
        """
        Refresh access token using refresh token
        Updates Secret Manager if refresh token changes
        """
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            logger.info("📡 Requesting new access token from Amazon...")
            response = requests.post(self.TOKEN_URL, data=payload, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            
            # Update tokens
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
            self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            logger.info(f"✅ Access token refreshed (expires in {expires_in}s)")
            
            # Check if refresh token was updated (Amazon sometimes rotates them)
            new_refresh_token = token_data.get("refresh_token")
            if new_refresh_token and new_refresh_token != self.refresh_token:
                logger.info("🔄 Refresh token rotated by Amazon, updating Secret Manager...")
                self.refresh_token = new_refresh_token
                
                # Store new refresh token
                if self._update_secret("amazon_refresh_token", new_refresh_token):
                    logger.info("✅ New refresh token stored in Secret Manager")
                else:
                    logger.error("❌ Failed to store new refresh token!")
            
            return self.access_token
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ HTTP error during token refresh: {e}")
            logger.error(f"Response: {e.response.text if e.response else 'N/A'}")
            raise Exception(f"Failed to refresh Amazon access token: {e}")
        except Exception as e:
            logger.error(f"❌ Token refresh failed: {e}")
            raise
    
    def force_refresh(self):
        """Force immediate token refresh"""
        logger.info("🔄 Forcing token refresh...")
        self.access_token = None
        self.token_expires_at = None
        return self._refresh_access_token()
    
    def get_token_status(self) -> dict:
        """Get current token status for debugging"""
        if not self.token_expires_at:
            return {
                "has_access_token": bool(self.access_token),
                "has_refresh_token": bool(self.refresh_token),
                "expires_at": None,
                "is_valid": False
            }
        
        time_until_expiry = (self.token_expires_at - datetime.utcnow()).total_seconds()
        
        return {
            "has_access_token": bool(self.access_token),
            "has_refresh_token": bool(self.refresh_token),
            "expires_at": self.token_expires_at.isoformat(),
            "seconds_until_expiry": int(time_until_expiry),
            "is_valid": time_until_expiry > self.TOKEN_EXPIRY_BUFFER
        }


# Singleton instance
_token_manager = None

def get_token_manager() -> TokenManager:
    """Get or create singleton TokenManager instance"""
    global _token_manager
    if _token_manager is None:
        _token_manager = TokenManager()
    return _token_manager
