"""
Amazon Advertising API client with automatic token refresh
"""
import requests
import time
from google.cloud import secretmanager
from typing import List, Dict, Any, Optional, Union
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import settings
from .logger import get_logger

# Define logger at the module level, correctly using __name__
logger = get_logger(__name__)

class AmazonAdsClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, profile_id: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.profile_id = profile_id
        self.BASE_URL = "https://advertising-api.amazon.com"
        self.token_manager = self._init_token_manager()

    def _init_token_manager(self):
        # Placeholder for your TokenManager initialization
        raise NotImplementedError("TokenManager initialization not implemented.")

    def _get_headers(self) -> Dict[str, str]:
        # Placeholder for your header generation, including access token
        raise NotImplementedError("_get_headers method not implemented.")

    def _execute_request_once(self, method: str, url: str, payload: Optional[Union[List, Dict]] = None) -> requests.Response:
        # Placeholder for executing a single request
        raise NotImplementedError("_execute_request_once method not implemented.")

    def _make_request(self, method: str, endpoint: str, payload: Optional[Union[List, Dict]] = None) -> Optional[Any]:
        """
        Centralized request handler with 401 (Token Expiry) handling and error logging.
        Returns the JSON response data on success, or None on failure.
        """
        url = f"{self.BASE_URL}{endpoint}"

        # CRITICAL DEBUGGING: Log payload with types
        logger.debug(f"Sending {method} request to {url}")
        logger.debug(f"Payload type: {type(payload)}")
        logger.debug(f"Payload content: {payload}")
        
        # Deep inspection of payload structure
        if isinstance(payload, list) and len(payload) > 0:
            for idx, item in enumerate(payload):
                logger.debug(f"Payload[{idx}] type: {type(item)}")
                if isinstance(item, dict):
                    for key, value in item.items():
                        logger.debug(f"  {key}: {value} (type: {type(value).__name__})")

        try:
            response = self._execute_request_once(method, url, payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("⚠️ Got 401 Unauthorized, forcing token refresh and retrying...")
                self.token_manager.force_refresh()

                try:
                    retry_response = self._execute_request_once(method, url, payload)
                    retry_response.raise_for_status()
                    return retry_response.json()
                except Exception as retry_e:
                    logger.error(f"❌ Retry after token refresh failed for {url}. Error: {retry_e}. Response: {getattr(retry_e, 'response', None).text if getattr(retry_e, 'response', None) else 'N/A'}")
                    return None

            elif e.response.status_code == 429:
                logger.warning(f"⚠️ Got 429 Too Many Requests for {url}. (Tenacity will retry if configured)")
                raise e

            else:
                logger.error(f"❌ HTTP Error {e.response.status_code} for {url}: {e.response.text}. Request Method: {method}, Payload: {payload}")
                return None

        except Exception as e:
            logger.error(f"❌ Request to {url} failed. Error: {e}. Request Method: {method}, Payload: {payload}")
            return None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def update_keyword_bid(self, keyword_id: Union[str, int], new_bid: float) -> Optional[Dict]:
        """Update existing keyword bid"""
        
        # Log what we received
        logger.debug(f"update_keyword_bid called with keyword_id={keyword_id} (type: {type(keyword_id).__name__}), new_bid={new_bid} (type: {type(new_bid).__name__})")
        
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} bid to ${new_bid:.2f}")
            return {"status": "dry_run_success"}

        # Ensure keywordId is always a string
        processed_keyword_id = str(keyword_id)
        
        # Ensure bid is a float, not Decimal or other numeric type
        processed_bid = float(new_bid)

        payload = [{
            "keywordId": processed_keyword_id,
            "bid": processed_bid,
            "state": "ENABLED"
        }]

        endpoint = "/v2/sp/keywords"

        response_data = self._make_request("PUT", endpoint, payload)
        if response_data:
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid:.2f}. Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to update keyword {keyword_id} bid to ${new_bid:.2f}")
        return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.HTTPError)
    )
    def create_keyword(self, campaign_id: Union[str, int], ad_group_id: Union[str, int], keyword_text: str,
                       match_type: str, bid: float) -> Optional[Dict]:
        """Create a new keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: '{keyword_text}' ({match_type}) in Campaign {campaign_id}, AdGroup {ad_group_id} @ ${bid:.2f}")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "adGroupId": str(ad_group_id),
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED",
            "bid": float(bid)
        }]

        response_data = self._make_request("POST", "/v2/sp/keywords", payload)
        if response_data:
            logger.info(f"✅ Created keyword: '{keyword_text}' (Campaign: {campaign_id}, AdGroup: {ad_group_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to create keyword: '{keyword_text}' (Campaign: {campaign_id}, AdGroup: {ad_group_id})")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(self, campaign_id: Union[str, int], keyword_text: str,
                                match_type: str = "NEGATIVE_EXACT") -> Optional[Dict]:
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative keyword: '{keyword_text}' ({match_type}) to Campaign {campaign_id}")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED"
        }]

        response_data = self._make_request("POST", "/v2/sp/campaignNegativeKeywords", payload)
        if response_data:
            logger.info(f"✅ Added negative keyword: '{keyword_text}' (Campaign: {campaign_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to add negative keyword: '{keyword_text}' (Campaign: {campaign_id})")
        return None

    def batch_update_keyword_bids(self, bid_updates: List[Dict[str, Union[str, float]]]) -> Dict[str, int]:
        """
        Batches multiple keyword bid updates into a single API call if possible,
        or iterates if a batch endpoint is not available or desired.
        For Amazon SP keywords, the PUT /v2/sp/keywords endpoint accepts a list.
        """
        if not bid_updates:
            return {"success": 0, "failed": 0}

        success_count = 0
        failed_count = 0
        
        for update_item in bid_updates:
            keyword_id = update_item["keywordId"]
            new_bid = update_item["bid"]
            result = self.update_keyword_bid(keyword_id=keyword_id, new_bid=new_bid)
            if result:
                success_count += 1
            else:
                failed_count += 1
        
        return {"success": success_count, "failed": failed_count}
