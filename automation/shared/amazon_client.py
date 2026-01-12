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
        # Build full URL from base and endpoint
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
                    logger.error(f"❌ Retry after token refresh failed for {url}. Error: {retry_e}. Response: {getattr(retry_e, 'response', None)}")
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
    def update_keyword_bid(self, keyword_id: Union[str, int], new_bid: float) -> Optional[Dict]:
        """Update existing keyword bid"""
        # Honor dry-run mode
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} bid to ${new_bid:.2f}")
            return {"status": "dry_run_success"}

        # Ensure keywordId is a string (API expects string)
        processed_keyword_id = str(keyword_id)

        payload = [{
            "keywordId": processed_keyword_id,
            "bid": float(new_bid),
            "state": "ENABLED"
        }]

        endpoint = "/v2/sp/keywords"

        response_data = self._make_request("PUT", endpoint, payload)
        if response_data:
            logger.info(f"✅ Updated keyword {processed_keyword_id} bid to ${new_bid:.2f}. Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to update keyword {processed_keyword_id} bid to ${new_bid:.2f}")
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
            logger.info(f"[DRY RUN] Would create keyword: '{keyword_text}' ({match_type}) in Campaign {campaign_id}, AdGroup {ad_group_id} @ ${bid:.2f}")
            return {"status": "dry_run_success"}

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
            logger.info(f"✅ Created keyword: '{keyword_text}' (Campaign: {campaign_id}, AdGroup: {ad_group_id}). Response: {response_data}")
            return response_data
        logger.error(f"❌ Failed to create keyword: '{keyword_text}' (Campaign: {campaign_id}, AdGroup: {ad_group_id})")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(self, campaign_id: str, keyword_text: str,
                                match_type: str = "NEGATIVE_EXACT") -> Optional[Dict]: # Return Optional[Dict]
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative keyword: '{keyword_text}' ({match_type}) to Campaign {campaign_id}")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": campaign_id,
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

    def batch_update_keyword_bids(self, bid_updates: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch update keyword bids via /v2/sp/keywords.

        Expects a list of dicts like {"keywordId": <id>, "bid": <float>}. Returns counts.
        Honors DRY_RUN by logging and not calling the API.
        """
        if not bid_updates:
            return {"success": 0, "failed": 0}

        # Normalize payload
        payload = []
        for upd in bid_updates:
            kid = str(upd.get("keywordId")) if upd.get("keywordId") is not None else None
            bid = float(upd.get("bid", 0.0))
            if not kid:
                continue
            payload.append({
                "keywordId": kid,
                "bid": bid,
                "state": "ENABLED"
            })

        if settings.dry_run:
            logger.info(f"[DRY RUN] Would batch update {len(payload)} keyword bids")
            return {"success": len(payload), "failed": 0}

        response_data = self._make_request("PUT", "/v2/sp/keywords", payload)
        if response_data is not None:
            # Amazon returns a list of operation results; count success by status if present
            try:
                success = sum(1 for r in response_data if str(r.get("code", "")).startswith("SUCCESS") or r.get("status") == "SUCCESS")
                failed = len(payload) - success
            except Exception:
                # Fallback if response is not a list
                success = len(payload)
                failed = 0
            logger.info(f"✅ Batch updated keyword bids: {success} success, {failed} failed")
            return {"success": success, "failed": failed}
        logger.error("❌ Batch keyword bid update failed")
        return {"success": 0, "failed": len(payload)}

