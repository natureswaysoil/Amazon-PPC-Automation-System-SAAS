import requests
import time
from google.cloud import secretmanager
from typing import List, Dict, Any, Optional, Union # Added Union for payload type
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import settings
from .logger import get_logger

# Define logger at the module level, correctly using __name__
logger = get_logger(__name__)

class AmazonAdsClient:
    # ... (rest of the class init, _get_headers, _execute_request_once) ...

    def _make_request(self, method: str, endpoint: str, payload: Optional[Union[List, Dict]] = None) -> Optional[Any]:
        """
        Centralized request handler with 401 (Token Expiry) handling and error logging.
        Returns the JSON response data on success, or None on failure.
        """
        url = f"{self.BASE_URL}{}"

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
                    logger.error(f"❌ Retry after token refresh failed for {url}. Error: {}. Response: {getattr(retry_e, 'response', None)}")
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

    # ... (create_keyword and create_negative_keyword methods) ...

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def update_keyword_bid(self, keyword_id: str, new_bid: float) -> Optional[Dict]: # Return Optional[Dict]
        """Update existing keyword bid"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {} bid to ${new_bid:.2f}")
            return {"status": "dry_run_success"} # Mock response for dry run

        # CRITICAL FIX: Ensure keywordId is always a string, even if it comes in as an int/float
        # The Amazon API for keyword IDs always expects them as strings in the JSON payload.
        processed_keyword_id = str(keyword_id)

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
            logger.info(f"✅ Updated keyword {} bid to ${new_bid:.2f}. Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to update keyword {} bid to ${new_bid:.2f}")
        return None

        except Exception as e:
            logger.error(f"❌ Request to {url} failed. Error: {e}. Request Method: {method}, Payload: {payload}")
            return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.HTTPError)
    )
    def create_keyword(self, campaign_id: str, ad_group_id: str, keyword_text: str,
                       match_type: str, bid: float) -> Optional[Dict]: # Return Optional[Dict] for the response
        """Create a new keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: '{}' ({}) in Campaign {campaign_id}, AdGroup {ad_group_id} @ ${bid:.2f}")
            return {"status": "dry_run_success"} # Mock response for dry run

        payload = [{
            "campaignId": campaign_id,
            "adGroupId": ad_group_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED",
            "bid": bid
        }]

        response_data = self._make_request("POST", "/v2/sp/keywords", payload)
        if response_data:
            logger.info(f"✅ Created keyword: '{}' (Campaign: {campaign_id}, AdGroup: {ad_group_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to create keyword: '{}' (Campaign: {campaign_id}, AdGroup: {ad_group_id})")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(self, campaign_id: str, keyword_text: str,
                                match_type: str = "NEGATIVE_EXACT") -> Optional[Dict]: # Return Optional[Dict]
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative keyword: '{}' ({}) to Campaign {campaign_id}")
            return {"status": "dry_run_success"} # Mock response for dry run

        payload = [{
            "campaignId": campaign_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED"
        }]

        response_data = self._make_request("POST", "/v2/sp/campaignNegativeKeywords", payload)
        if response_data:
            logger.info(f"✅ Added negative keyword: '{}' (Campaign: {campaign_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to add negative keyword: '{}' (Campaign: {campaign_id})")
        return None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def update_keyword_bid(self, keyword_id: str, new_bid: float) -> Optional[Dict]: # Return Optional[Dict]
        """Update existing keyword bid"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {} bid to ${new_bid:.2f}")
            return {"status": "dry_run_success"} # Mock response for dry run

        # Amazon Advertising API for SP Keywords expects a list of objects for PUT /v2/sp/keywords
        # Each object in the list must contain the keywordId and the fields to update.
        payload = [{
            "keywordId": keyword_id,
            "bid": new_bid,
            "state": "ENABLED" # Good practice to include, assuming you want it to remain enabled
        }]

        # The endpoint for batch updates of keywords is /v2/sp/keywords
        endpoint = "/v2/sp/keywords"

        response_data = self._make_request("PUT", endpoint, payload)
        if response_data:
            logger.info(f"✅ Updated keyword {} bid to ${new_bid:.2f}. Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to update keyword {} bid to ${new_bid:.2f}")
        return None




Evaluate

Compare

