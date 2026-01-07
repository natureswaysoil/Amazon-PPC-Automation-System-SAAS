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

logger = get_logger(__name__)

def safe_serialize(obj):
    """Convert any numeric types to JSON-safe types"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (int, float)):
        return obj
    elif isinstance(obj, dict):
        return {str(k): safe_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_serialize(item) for item in obj]
    return obj

class AmazonAdsClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, profile_id: str):
        self.client_id = str(client_id)
        self.client_secret = str(client_secret)
        self.refresh_token = str(refresh_token)
        self.profile_id = str(profile_id)
        self.BASE_URL = "https://advertising-api.amazon.com"
        self.token_manager = self._init_token_manager()
        
        # Log initialization
        logger.info(f"🔧 AmazonAdsClient init - profile_id type: {type(self.profile_id).__name__}")

    def _init_token_manager(self):
        raise NotImplementedError("TokenManager initialization not implemented.")

    def _get_headers(self) -> Dict[str, str]:
        raise NotImplementedError("_get_headers method not implemented.")

    def _execute_request_once(self, method: str, url: str, payload: Optional[Union[List, Dict]] = None) -> requests.Response:
        """Execute a single HTTP request"""
        headers = self._get_headers()
        
        # CRITICAL: Log the actual headers being sent
        logger.info(f"📋 Request Headers:")
        for key, value in headers.items():
            if 'Authorization' not in key:  # Don't log auth token
                logger.info(f"   {key}: {value} (type: {type(value).__name__})")
        
        # Serialize and log the payload
        if payload:
            # Convert payload to JSON string to see exactly what will be sent
            try:
                json_payload = json.dumps(payload)
                logger.info(f"📦 JSON Payload to be sent:")
                logger.info(json_payload)
            except Exception as e:
                logger.error(f"❌ Failed to serialize payload to JSON: {e}")
                logger.error(f"Problematic payload: {payload}")
                raise
            
            response = requests.request(method, url, headers=headers, json=payload)
        else:
            response = requests.request(method, url, headers=headers)
        
        return response

    def _make_request(self, method: str, endpoint: str, payload: Optional[Union[List, Dict]] = None) -> Optional[Any]:
        """
        Centralized request handler with 401 (Token Expiry) handling and error logging.
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        logger.info(f"=" * 80)
        logger.info(f"🌐 Making API Request:")
        logger.info(f"   Method: {method}")
        logger.info(f"   URL: {url}")
        logger.info(f"   Endpoint: {endpoint}")

        # Sanitize payload
        if payload:
            original_payload = payload
            payload = safe_serialize(payload)
            logger.info(f"🔍 Payload Analysis:")
            logger.info(f"   Original type: {type(original_payload).__name__}")
            logger.info(f"   After sanitization: {type(payload).__name__}")
            
            if isinstance(payload, list):
                logger.info(f"   List length: {len(payload)}")
                for idx, item in enumerate(payload):
                    logger.info(f"   Item[{idx}]:")
                    if isinstance(item, dict):
                        for k, v in item.items():
                            logger.info(f"      {k}: '{v}' (type: {type(v).__name__})")

        try:
            response = self._execute_request_once(method, url, payload)
            response.raise_for_status()
            logger.info(f"✅ Request successful!")
            logger.info(f"=" * 80)
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("⚠️ Got 401 Unauthorized, forcing token refresh and retrying...")
                self.token_manager.force_refresh()

                try:
                    retry_response = self._execute_request_once(method, url, payload)
                    retry_response.raise_for_status()
                    logger.info(f"✅ Retry successful after token refresh!")
                    return retry_response.json()
                except Exception as retry_e:
                    logger.error(f"❌ Retry failed: {retry_e}")
                    if hasattr(retry_e, 'response') and retry_e.response:
                        logger.error(f"Response text: {retry_e.response.text}")
                    return None

            elif e.response.status_code == 429:
                logger.warning(f"⚠️ Rate limited - will retry")
                raise e

            else:
                logger.error(f"❌ HTTP Error {e.response.status_code}")
                logger.error(f"Response: {e.response.text}")
                logger.info(f"=" * 80)
                return None

        except Exception as e:
            logger.error(f"❌ Request failed: {e}")
            logger.info(f"=" * 80)
            return None

    def get_keyword_bid_recommendations(self, keyword_id: Union[str, int]) -> Optional[Dict]:
        """Get Amazon's suggested bid recommendations for a keyword"""
        keyword_id = str(keyword_id)
        endpoint = f"/v2/sp/keywords/{keyword_id}/bidRecommendations"
        
        logger.info(f"📊 Fetching bid recommendations for keyword {keyword_id}")
        response_data = self._make_request("GET", endpoint)
        
        if response_data:
            logger.info(f"✅ Got recommendations: {response_data}")
            return response_data
        
        return None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def update_keyword_bid(self, keyword_id: Union[str, int], new_bid: float, 
                          use_amazon_suggested: bool = False) -> Optional[Dict]:
        """Update existing keyword bid"""
        
        logger.info(f"🎯 update_keyword_bid called with:")
        logger.info(f"   keyword_id: {keyword_id} (type: {type(keyword_id).__name__})")
        logger.info(f"   new_bid: {new_bid} (type: {type(new_bid).__name__})")
        logger.info(f"   use_amazon_suggested: {use_amazon_suggested}")
        
        keyword_id_str = str(keyword_id)
        
        if use_amazon_suggested:
            recommendations = self.get_keyword_bid_recommendations(keyword_id_str)
            if recommendations and 'suggestedBid' in recommendations:
                new_bid = float(recommendations['suggestedBid'])
                logger.info(f"📊 Using Amazon suggested bid: ${new_bid:.2f}")
        
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} to ${new_bid:.2f}")
            return {"status": "dry_run_success"}

        # Build payload with EXPLICIT type conversion
        payload_item = {
            "keywordId": str(keyword_id),
            "bid": float(new_bid),
            "state": str("ENABLED")
        }
        
        # Double-check types before adding to list
        logger.info(f"🔍 Payload item before list wrap:")
        for k, v in payload_item.items():
            logger.info(f"   {k}: {v} (type: {type(v).__name__})")
        
        payload = [payload_item]
        endpoint = "/v2/sp/keywords"

        response_data = self._make_request("PUT", endpoint, payload)
        
        if response_data:
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid:.2f}")
            return response_data
        
        logger.error(f"❌ Failed to update keyword {keyword_id}")
        return None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def create_keyword(self, campaign_id: Union[str, int], ad_group_id: Union[str, int], 
                       keyword_text: str, match_type: str, bid: float) -> Optional[Dict]:
        """Create a new keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: '{keyword_text}'")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "adGroupId": str(ad_group_id),
            "keywordText": str(keyword_text),
            "matchType": str(match_type),
            "state": str("ENABLED"),
            "bid": float(bid)
        }]

        response_data = self._make_request("POST", "/v2/sp/keywords", payload)
        if response_data:
            logger.info(f"✅ Created keyword: '{keyword_text}'")
            return response_data
        
        logger.error(f"❌ Failed to create keyword: '{keyword_text}'")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(self, campaign_id: Union[str, int], keyword_text: str,
                                match_type: str = "NEGATIVE_EXACT") -> Optional[Dict]:
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative: '{keyword_text}'")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "keywordText": str(keyword_text),
            "matchType": str(match_type),
            "state": str("ENABLED")
        }]

        response_data = self._make_request("POST", "/v2/sp/campaignNegativeKeywords", payload)
        if response_data:
            logger.info(f"✅ Added negative keyword: '{keyword_text}'")
            return response_data
        
        logger.error(f"❌ Failed to add negative keyword")
        return None

    def batch_update_keyword_bids(self, bid_updates: List[Dict[str, Union[str, float, int]]], 
                                  use_amazon_suggested: bool = False) -> Dict[str, int]:
        """Batch update multiple keyword bids"""
        if not bid_updates:
            return {"success": 0, "failed": 0}

        logger.info(f"🔄 Batch updating {len(bid_updates)} keywords")
        
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
        
        logger.info(f"📊 Batch complete: {success_count} success, {failed_count} failed")
        return {"success": success_count, "failed": failed_count}
