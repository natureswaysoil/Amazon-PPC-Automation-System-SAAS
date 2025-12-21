"""
Amazon Advertising API client wrapper
"""

import requests
from google.cloud import secretmanager
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class AmazonAdsClient:
    """
    Wrapper for Amazon Advertising API
    Handles authentication and API calls
    """
    
    BASE_URL = "https://advertising-api.amazon.com"
    TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    
    def __init__(self):
        self.client_id = self._get_secret("amazon_client_id")
        self.client_secret = self._get_secret("amazon_client_secret")
        self.refresh_token = self._get_secret("amazon_refresh_token")
        self.profile_id = self._get_secret("amazon_profile_id")
        
        self.access_token = None
        self._authenticate()
    
    def _get_secret(self, secret_name: str) -> str:
        """Fetch secret from Google Secret Manager"""
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{settings.project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Error fetching secret {secret_name}: {e}")
            raise
    
    def _authenticate(self):
        """Get access token using refresh token"""
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            response = requests.post(self.TOKEN_URL, data=payload)
            response.raise_for_status()
            self.access_token = response.json()["access_token"]
            logger.info("✅ Authenticated with Amazon Ads API")
        except Exception as e:
            logger.error(f"❌ Amazon API authentication failed: {e}")
            raise
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": self.profile_id,
            "Content-Type": "application/json"
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def update_keyword_bid(self, keyword_id: str, new_bid: float) -> bool:
        """
        Update keyword bid via Amazon API
        
        Returns True if successful
        """
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} to ${new_bid}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/keywords/{keyword_id}"
        payload = {"bid": new_bid}
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("Access token expired, re-authenticating...")
                self._authenticate()
                return self.update_keyword_bid(keyword_id, new_bid)
            else:
                logger.error(f"❌ Failed to update keyword {keyword_id}: {e}")
                return False
        except Exception as e:
            logger.error(f"❌ Error updating keyword {keyword_id}: {e}")
            return False
    
    def batch_update_keyword_bids(self, updates: List[Dict]) -> Dict:
        """
        Batch update multiple keywords
        
        Args:
            updates: List of {"keywordId": str, "bid": float}
        
        Returns:
            {"success": int, "failed": int, "errors": List}
        """
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would batch update {len(updates)} keywords")
            return {"success": len(updates), "failed": 0, "errors": []}
        
        url = f"{self.BASE_URL}/v2/sp/keywords"
        
        results = {"success": 0, "failed": 0, "errors": []}
        
        # Amazon API typically limits batch size
        batch_size = 100
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            try:
                response = requests.put(url, headers=self._get_headers(), json=batch)
                response.raise_for_status()
                results["success"] += len(batch)
                logger.info(f"✅ Batch updated {len(batch)} keywords")
            except Exception as e:
                results["failed"] += len(batch)
                results["errors"].append(str(e))
                logger.error(f"❌ Batch update failed: {e}")
        
        return results
import logging
from aov_fetcher import aov_fetcher
from bid_optimizer import optimize_bids

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting Amazon PPC optimization job...")
    
    # STEP 1: Fetch real-time AOV data
    aov_fetcher.fetch_all()
    
    # STEP 2: Run optimization (now AOV-aware)
    optimize_bids()
    
    logger.info("✅ Optimization complete")

if __name__ == "__main__":
    main()
