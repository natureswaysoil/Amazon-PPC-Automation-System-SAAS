"""
Amazon Advertising API client with automatic token refresh
"""

import requests
from google.cloud import secretmanager
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import settings
from .logger import get_logger
from .token_manager import get_token_manager

logger = get_logger(__name__)

class AmazonAdsClient:
    """
    Wrapper for Amazon Advertising API
    Handles authentication with automatic token refresh
    """
    
    BASE_URL = "https://advertising-api.amazon.com"
    
    def __init__(self):
        self.token_manager = get_token_manager()
        self.profile_id = self._get_secret("amazon_profile_id")
        
        # Ensure we have a valid token
        self.access_token = self.token_manager.get_valid_access_token()
        
        logger.info("✅ AmazonAdsClient initialized")
        logger.info(f"Token status: {self.token_manager.get_token_status()}")
    
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
    
    def _get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests
        Automatically refreshes token if needed
        """
        # Get fresh token (will refresh if needed)
        self.access_token = self.token_manager.get_valid_access_token()
        
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.token_manager.client_id,
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
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} to ${new_bid:.2f}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/keywords/{keyword_id}"
        payload = {"bid": new_bid}
        
        try:
            response = requests.put(
                url, 
                headers=self._get_headers(), 
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid:.2f}")
            return True
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                # Token might be invalid, force refresh and retry once
                logger.warning("⚠️ Got 401, forcing token refresh...")
                self.token_manager.force_refresh()
                
                # Retry once with new token
                response = requests.put(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()
                logger.info(f"✅ Updated keyword {keyword_id} after token refresh")
                return True
            else:
                logger.error(f"❌ Failed to update keyword {keyword_id}: {e.response.status_code}")
                logger.error(f"Response: {e.response.text}")
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
        
        # Amazon API typically limits batch size to 100
        batch_size = 100
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            try:
                response = requests.put(
                    url,
                    headers=self._get_headers(),
                    json=batch,
                    timeout=60
                )
                response.raise_for_status()
                results["success"] += len(batch)
                logger.info(f"✅ Batch updated {len(batch)} keywords")
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    # Retry batch with refreshed token
                    self.token_manager.force_refresh()
                    try:
                        response = requests.put(
                            url,
                            headers=self._get_headers(),
                            json=batch,
                            timeout=60
                        )
                        response.raise_for_status()
                        results["success"] += len(batch)
                        logger.info(f"✅ Batch updated {len(batch)} keywords after token refresh")
                    except Exception as retry_error:
                        results["failed"] += len(batch)
                        results["errors"].append(f"Batch retry failed: {retry_error}")
                        logger.error(f"❌ Batch retry failed: {retry_error}")
                else:
                    results["failed"] += len(batch)
                    results["errors"].append(f"{e.response.status_code}: {e.response.text}")
                    logger.error(f"❌ Batch update failed: {e}")
                    
            except Exception as e:
                results["failed"] += len(batch)
                results["errors"].append(str(e))
                logger.error(f"❌ Batch update failed: {e}")
        
        return results
    
    def test_connection(self) -> bool:
        """
        Test API connection and authentication
        Useful for verification
        """
        url = f"{self.BASE_URL}/v2/profiles"
        
        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                timeout=30
            )
            response.raise_for_status()
            profiles = response.json()
            logger.info(f"✅ API connection successful, found {len(profiles)} profiles")
            return True
        except Exception as e:
            logger.error(f"❌ API connection test failed: {e}")
            return False
