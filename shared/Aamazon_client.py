"""
Amazon Advertising API client with automatic token refresh
"""
import requests
import time
import json
from decimal import Decimal
from google.cloud import secretmanager
from typing import List, Dict, Any, Optional, Union
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import settings
from .logger import get_logger

# Define logger at the module level, correctly using __name__
logger = get_logger(__name__)

def safe_serialize(obj):
    """Convert any numeric types to JSON-safe types"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (int, float)):
        return float(obj) if isinstance(obj, float) else obj
    elif isinstance(obj, dict):
        return {k: safe_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_serialize(item) for item in obj]
    return obj

class AmazonAdsClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, profile_id: str):
        self.client_id = str(client_id)  # Ensure string
        self.client_secret = str(client_secret)
        self.refresh_token = str(refresh_token)
        self.profile_id = str(profile_id)  # CRITICAL: profile_id must be string
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

        # Sanitize payload to ensure all values are JSON-serializable
        if payload:
            payload = safe_serialize(payload)

        # CRITICAL DEBUGGING: Log payload with types
        logger.info(f"🔍 API Request: {method} {endpoint}")
        logger.info(f"📦 Payload: {json.dumps(payload, indent=2) if payload else 'None'}")
        
        # Deep inspection of payload structure
        if isinstance(payload, list) and len(payload) > 0:
            for idx, item in enumerate(payload):
                if isinstance(item, dict):
                    logger.info(f"📋 Payload[{idx}] fields:")
                    for key, value in item.items():
                        logger.info(f"   • {key}: {value} (type: {type(value).__name__})")

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
                logger.error(f"❌ HTTP Error {e.response.status_code} for {url}: {e.response.text}")
                logger.error(f"❌ Request payload was: {json.dumps(payload, indent=2) if payload else 'None'}")
                return None

        except Exception as e:
            logger.error(f"❌ Request to {url} failed. Error: {e}")
            logger.error(f"❌ Request payload was: {json.dumps(payload, indent=2) if payload else 'None'}")
            return None

    def get_keyword_bid_recommendations(self, keyword_id: Union[str, int]) -> Optional[Dict]:
        """
        Get Amazon's suggested bid recommendations for a keyword.
        Returns suggested bids for different match types and ad formats.
        """
        keyword_id = str(keyword_id)
        endpoint = f"/v2/sp/keywords/{keyword_id}/bidRecommendations"
        
        logger.info(f"📊 Fetching bid recommendations for keyword {keyword_id}")
        
        response_data = self._make_request("GET", endpoint)
        if response_data:
            logger.info(f"✅ Got bid recommendations for keyword {keyword_id}: {response_data}")
            return response_data
        
        logger.warning(f"⚠️ Could not get bid recommendations for keyword {keyword_id}")
        return None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def update_keyword_bid(self, keyword_id: Union[str, int], new_bid: float, 
                          use_amazon_suggested: bool = False) -> Optional[Dict]:
        """
        Update existing keyword bid.
        
        Args:
            keyword_id: The keyword ID to update
            new_bid: The new bid amount (ignored if use_amazon_suggested=True)
            use_amazon_suggested: If True, fetch and use Amazon's suggested bid
        """
        keyword_id_str = str(keyword_id)
        
        logger.info(f"🎯 update_keyword_bid called:")
        logger.info(f"   keyword_id={keyword_id} (type: {type(keyword_id).__name__})")
        logger.info(f"   new_bid={new_bid} (type: {type(new_bid).__name__})")
        logger.info(f"   use_amazon_suggested={use_amazon_suggested}")
        
        # If using Amazon suggested bids, fetch the recommendation
        if use_amazon_suggested:
            recommendations = self.get_keyword_bid_recommendations(keyword_id_str)
            if recommendations and 'suggestedBid' in recommendations:
                new_bid = float(recommendations['suggestedBid'])
                logger.info(f"📊 Using Amazon suggested bid: ${new_bid:.2f}")
            else:
                logger.warning(f"⚠️ Could not get Amazon suggested bid, using provided bid: ${new_bid:.2f}")
        
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} bid to ${new_bid:.2f}")
            return {"status": "dry_run_success"}

        # Ensure all values are correct types
        processed_keyword_id = str(keyword_id)
        processed_bid = float(new_bid)

        # Build payload with explicit type enforcement
        payload = [{
            "keywordId": processed_keyword_id,  # Must be string
            "bid": processed_bid,               # Must be float
            "state": "ENABLED"                  # Must be string
        }]

        endpoint = "/v2/sp/keywords"

        logger.info(f"📤 Sending update request for keyword {processed_keyword_id}")
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
    def create_keyword(self, campaign_id: Union[str, int], ad_group_id: Union[str, int], 
                       keyword_text: str, match_type: str, bid: float) -> Optional[Dict]:
        """Create a new keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: '{keyword_text}' ({match_type}) in Campaign {campaign_id}, AdGroup {ad_group_id} @ ${bid:.2f}")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "adGroupId": str(ad_group_id),
            "keywordText": str(keyword_text),
            "matchType": str(match_type),
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
            "keywordText": str(keyword_text),
            "matchType": str(match_type),
            "state": "ENABLED"
        }]

        response_data = self._make_request("POST", "/v2/sp/campaignNegativeKeywords", payload)
        if response_data:
            logger.info(f"✅ Added negative keyword: '{keyword_text}' (Campaign: {campaign_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to add negative keyword: '{keyword_text}' (Campaign: {campaign_id})")
        return None

    def batch_update_keyword_bids(self, bid_updates: List[Dict[str, Union[str, float, int]]], 
                                  use_amazon_suggested: bool = False) -> Dict[str, int]:
        """
        Batches multiple keyword bid updates.
        
        Args:
            bid_updates: List of dicts with 'keywordId' and 'bid' keys
            use_amazon_suggested: If True, use Amazon's suggested bids instead of provided bids
        """
        if not bid_updates:
            logger.warning("⚠️ No bid updates provided to batch_update_keyword_bids")
            return {"success": 0, "failed": 0}

        logger.info(f"🔄 Processing {len(bid_updates)} bid updates (Amazon suggested: {use_amazon_suggested})")
        
        success_count = 0
        failed_count = 0
        
        for update_item in bid_updates:
            keyword_id = update_item["keywordId"]
            new_bid = update_item.get("bid", 0.0)
            
            result = self.update_keyword_bid(
                keyword_id=keyword_id, 
                new_bid=new_bid,
                use_amazon_suggested=use_amazon_suggested
            )
            
            if result:
                success_count += 1
            else:
                failed_count += 1
        
        logger.info(f"📊 Batch update complete: {success_count} success, {failed_count} failed")
        return {"success": success_count, "failed": failed_count}
