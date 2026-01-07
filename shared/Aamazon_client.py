"""
Amazon Advertising API client with automatic token refresh
"""

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
        self.token_manager = self._init_token_manager() # Assuming you have a TokenManager class

    def _init_token_manager(self):
        # Placeholder for your TokenManager initialization
        # This part is crucial for handling access tokens
        # Example: return TokenManager(self.client_id, self.client_secret, self.refresh_token)
        raise NotImplementedError("TokenManager initialization not implemented.")

    def _get_headers(self) -> Dict[str, str]:
        # Placeholder for your header generation, including access token
        # Example:
        # access_token = self.token_manager.get_access_token()
        # return {
        #     "Content-Type": "application/json",
        #     "Authorization": f"Bearer {access_token}",
        #     "Amazon-Advertising-API-ClientId": self.client_id,
        #     "Amazon-Advertising-API-Scope": self.profile_id
        # }
        raise NotImplementedError("_get_headers method not implemented.")

    def _execute_request_once(self, method: str, url: str, payload: Optional[Union[List, Dict]] = None) -> requests.Response:
        # Placeholder for executing a single request
        # Example:
        # headers = self._get_headers()
        # if payload:
        #     return requests.request(method, url, headers=headers, json=payload)
        # else:
        #     return requests.request(method, url, headers=headers)
        raise NotImplementedError("_execute_request_once method not implemented.")

    def _make_request(self, method: str, endpoint: str, payload: Optional[Union[List, Dict]] = None) -> Optional[Any]:
        """
        Centralized request handler with 401 (Token Expiry) handling and error logging.
        Returns the JSON response data on success, or None on failure.
        """
        url = f"{self.BASE_URL}{endpoint}"

        # CRITICAL DEBUGGING STEP: Log the actual payload being sent
        logger.debug(f"Sending {method} request to {url} with payload: {payload}")

        try:
            response = self._execute_request_once(method, url, payload)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json() # Return JSON response body on success
        except requests.exceptions.HTTPError as e:
            # Handle Token Expiry (401)
            if e.response.status_code == 401:
                logger.warning("⚠️ Got 401 Unauthorized, forcing token refresh and retrying...")
                self.token_manager.force_refresh() # Force refresh token

                # Retry once with new token
                try:
                    retry_response = self._execute_request_once(method, url, payload)
                    retry_response.raise_for_status()
                    return retry_response.json() # Return JSON response body on retry success
                except Exception as retry_e:
                    logger.error(f"❌ Retry after token refresh failed for {url}. Error: {retry_e}. Response: {getattr(retry_e, 'response', None).text if getattr(retry_e, 'response', None) else 'N/A'}")
                    return None

            # Handle Rate Limiting (429) - Tenacity will catch this
            elif e.response.status_code == 429:
                logger.warning(f"⚠️ Got 429 Too Many Requests for {url}. (Tenacity will retry if configured)")
                raise e # Re-raise to let Tenacity handle backing off

            else:
                logger.error(f"❌ HTTP Error {e.response.status_code} for {url}: {e.response.text}. Request Method: {method}, Payload: {payload}")
                return None

        except Exception as e:
            logger.error(f"❌ Request to {url} failed. Error: {e}. Request Method: {method}, Payload: {payload}")
            return None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def update_keyword_bid(self, keyword_id: Union[str, int], new_bid: float) -> Optional[Dict]: # Return Optional[Dict]
        """Update existing keyword bid"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} bid to ${new_bid:.2f}")
            return {"status": "dry_run_success"} # Mock response for dry run

        # CRITICAL FIX: Ensure keywordId is always a string, even if it comes in as an int/float
        # The Amazon API for keyword IDs always expects them as strings in the JSON payload.
        processed_keyword_id = str(keyword_id) # This is the key fix for the reported error

        # Amazon Advertising API for SP Keywords expects a list of objects for PUT /v2/sp/keywords
        # Each object in the list must contain the keywordId and the fields to update.
        payload = [{
            "keywordId": processed_keyword_id, # Use the string-enforced ID
            "bid": new_bid,
            "state": "ENABLED" # Good practice to include, assuming you want it to remain enabled
        }]

        # The endpoint for batch updates of keywords is /v2/sp/keywords
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
                       match_type: str, bid: float) -> Optional[Dict]: # Return Optional[Dict] for the response
        """Create a new keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: '{keyword_text}' ({match_type}) in Campaign {campaign_id}, AdGroup {ad_group_id} @ ${bid:.2f}")
            return {"status": "dry_run_success"} # Mock response for dry run

        payload = [{
            "campaignId": str(campaign_id),  # Ensure string
            "adGroupId": str(ad_group_id),   # Ensure string
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED",
            "bid": bid
        }]

        response_data = self._make_request("POST", "/v2/sp/keywords", payload)
        if response_data:
            logger.info(f"✅ Created keyword: '{keyword_text}' (Campaign: {campaign_id}, AdGroup: {ad_group_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to create keyword: '{keyword_text}' (Campaign: {campaign_id}, AdGroup: {ad_group_id})")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(self, campaign_id: Union[str, int], keyword_text: str,
                                match_type: str = "NEGATIVE_EXACT") -> Optional[Dict]: # Return Optional[Dict]
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative keyword: '{keyword_text}' ({match_type}) to Campaign {campaign_id}")
            return {"status": "dry_run_success"} # Mock response for dry run

        payload = [{
            "campaignId": str(campaign_id),  # Ensure string
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
        
        # The update_keyword_bid method already handles a single keyword,
        # but the _make_request with a list payload handles the batch.
        # So we can just call _make_request directly here.
        
        # Format bid_updates for the batch endpoint if your existing update_keyword_bid
        # is meant for single updates. Given your original problem was about 'keywordId'
        # being a number, we'll assume `update_keyword_bid` is called for each one.
        # If the goal was to make this a single batch call, the logic would be slightly different.
        
        # For simplicity, assuming `update_keyword_bid` is designed to be called for each:
        for update_item in bid_updates:
            keyword_id = update_item["keywordId"]
            new_bid = update_item["bid"]
            result = self.update_keyword_bid(keyword_id=keyword_id, new_bid=new_bid)
            if result:
                success_count += 1
            else:
                failed_count += 1
        
        return {"success": success_count, "failed": failed_count}
