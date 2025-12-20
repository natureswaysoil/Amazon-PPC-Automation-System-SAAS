cd ~/amazon-ppc-automation

cat > automation/shared/amazon_client.py << 'PYEOF'
import requests
from google.cloud import secretmanager
from typing import List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import settings
from .logger import get_logger
from .token_manager import get_token_manager

logger = get_logger(__name__)

class AmazonAdsClient:
    BASE_URL = "https://advertising-api.amazon.com"
    
    def __init__(self):
        self.token_manager = get_token_manager()
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{settings.project_id}/secrets/amazon_profile_id/versions/latest"
        response = client.access_secret_version(request={"name": name})
        self.profile_id = response.payload.data.decode("UTF-8")
        self.access_token = self.token_manager.get_valid_access_token()
        logger.info("✅ AmazonAdsClient initialized")
    
    def _get_headers(self) -> Dict[str, str]:
        self.access_token = self.token_manager.get_valid_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.token_manager.client_id,
            "Amazon-Advertising-API-Scope": self.profile_id,
            "Content-Type": "application/json"
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def create_keyword(self, campaign_id: str, ad_group_id: str, keyword_text: str, 
                      match_type: str, bid: float) -> bool:
        """Create a new keyword from harvested search term"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: {keyword_text} ({match_type}) @ ${bid:.2f}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/keywords"
        payload = [{
            "campaignId": campaign_id,
            "adGroupId": ad_group_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED",
            "bid": bid
        }]
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Created keyword: {keyword_text}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("⚠️ Got 401, forcing token refresh...")
                self.token_manager.force_refresh()
                response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
                response.raise_for_status()
                return True
            logger.error(f"❌ Failed to create keyword {keyword_text}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error creating keyword {keyword_text}: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def create_negative_keyword(self, campaign_id: str, keyword_text: str, 
                               match_type: str = "NEGATIVE_EXACT") -> bool:
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative: {keyword_text}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/campaignNegativeKeywords"
        payload = [{
            "campaignId": campaign_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED"
        }]
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Added negative keyword: {keyword_text}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to add negative {keyword_text}: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def update_keyword_bid(self, keyword_id: str, new_bid: float) -> bool:
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} to ${new_bid:.2f}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/keywords/{keyword_id}"
        payload = {"bid": new_bid}
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid:.2f}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("⚠️ Got 401, forcing token refresh...")
                self.token_manager.force_refresh()
                response = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
                response.raise_for_status()
                return True
            logger.error(f"❌ Failed to update keyword: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error updating keyword {keyword_id}: {e}")
            return False
PYEOF
