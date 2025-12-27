cat > automation/shared/amazon_client.py << 'PYEOF'
import requests
import time
from google.cloud import secretmanager
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import settings
from .logger import get_logger
from .token_manager import get_token_manager

logger = get_logger(__name__)

class AmazonAdsClient:
    BASE_URL = "https://advertising-api.amazon.com"
    
    def __init__(self):
        self.token_manager = get_token_manager()
        self.profile_id = None
        
        # Safe Init: Load Profile ID from Secrets
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{settings.project_id}/secrets/amazon_profile_id/versions/latest"
            response = client.access_secret_version(request={"name": name})
            self.profile_id = response.payload.data.decode("UTF-8")
            logger.info("✅ AmazonAdsClient initialized")
        except Exception as e:
            if settings.dry_run:
                logger.warning(f"⚠️ Could not load Amazon Profile ID (Dry Run): {e}")
                self.profile_id = "MOCK_PROFILE_ID"
            else:
                logger.error(f"❌ Failed to load Amazon Profile ID: {e}")
                raise

    def _get_headers(self) -> Dict[str, str]:
        """Construct headers with valid access token"""
        access_token = self.token_manager.get_valid_access_token()
        return {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.token_manager.client_id,
            "Amazon-Advertising-API-Scope": self.profile_id,
            "Content-Type": "application/json"
        }

    def _make_request(self, method: str, endpoint: str, payload: Optional[List|Dict] = None) -> bool:
        """
        Centralized request handler with 401 (Token Expiry) handling
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method == "POST":
                response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            elif method == "PUT":
                response = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
            else:
                return False

            response.raise_for_status()
            return True

        except requests.exceptions.HTTPError as e:
            # Handle Token Expiry (401)
            if e.response.status_code == 401:
                logger.warning("⚠️ Got 401 Unauthorized, forcing token refresh and retrying...")
                self.token_manager.force_refresh()
                
                # Retry once with new token
                try:
                    if method == "POST":
                        requests.post(url, headers=self._get_headers(), json=payload, timeout=30).raise_for_status()
                    elif method == "PUT":
                        requests.put(url, headers=self._get_headers(), json=payload, timeout=30).raise_for_status()
                    return True
                except Exception as retry_e:
                    logger.error(f"❌ Retry failed after token refresh: {retry_e}")
                    return False
            
            # Handle Rate Limiting (429)
            elif e.response.status_code == 429:
                logger.warning("⚠️ Got 429 Too Many Requests. (Tenacity will retry if configured)")
                raise e # Raise to let Tenacity handle backing off
            
            else:
                logger.error(f"❌ HTTP Error {e.response.status_code}: {e.response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Request failed: {e}")
            return False

    @retry(
        stop=stop_after_attempt(5), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.HTTPError)
    )
    def create_keyword(self, campaign_id: str, ad_group_id: str, keyword_text: str, 
                       match_type: str, bid: float) -> bool:
        """Create a new keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: {keyword_text} ({match_type}) @ ${bid:.2f}")
            return True
        
        payload = [{
            "campaignId": campaign_id,
            "adGroupId": ad_group_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED",
            "bid": bid
        }]
        
        if self._make_request("POST", "/v2/sp/keywords", payload):
            logger.info(f"✅ Created keyword: {keyword_text}")
            return True
        return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(self, campaign_id: str, keyword_text: str, 
                                match_type: str = "NEGATIVE_EXACT") -> bool:
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative: {keyword_text}")
            return True
        
        payload = [{
            "campaignId": campaign_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED"
        }]
        
        if self._make_request("POST", "/v2/sp/campaignNegativeKeywords", payload):
            logger.info(f"✅ Added negative keyword: {keyword_text}")
            return True
        return False

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
    def update_keyword_bid(self, keyword_id: str, new_bid: float) -> bool:
        """Update existing keyword bid"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} to ${new_bid:.2f}")
            return True
        
        payload = {"bid": new_bid}
        
        if self._make_request("PUT", f"/v2/sp/keywords/{keyword_id}", payload):
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid:.2f}")
            return True
        return False
PYEOF
