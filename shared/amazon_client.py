"""
Amazon Advertising API client — multi-tenant.

Each instance is bound to ONE seller account via that account's own
credentials (client_id / client_secret / refresh_token / profile_id). The
client performs its own access-token refresh, so there is NO dependency on a
global, single-tenant Secret Manager entry. This is what lets the same
deployment manage many customers' ad accounts.

Build a client per tenant:

    client = AmazonAdsClient(
        client_id=tenant.client_id,
        client_secret=tenant.client_secret,
        refresh_token=tenant.refresh_token,
        profile_id=tenant.profile_id,
        region="NA",
    )
    client.update_keyword_bid("123456789", 1.25)

Honors settings.dry_run: in dry-run mode no mutating call hits Amazon.
"""

import json
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

# Amazon Ads regional API hosts
_REGION_HOSTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}
_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
_TOKEN_EXPIRY_BUFFER = 300  # refresh 5 min before expiry


def safe_serialize(obj):
    """Convert Decimal / nested numerics into JSON-safe primitives."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    elif isinstance(obj, dict):
        return {str(k): safe_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_serialize(item) for item in obj]
    return obj


class AmazonAdsClient:
    """Per-tenant Amazon Advertising API client with self-managed tokens."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        profile_id: Union[str, int],
        region: str = "NA",
    ):
        if not all([client_id, client_secret, refresh_token, profile_id]):
            raise ValueError(
                "AmazonAdsClient requires client_id, client_secret, "
                "refresh_token, and profile_id (multi-tenant credentials)."
            )
        self.client_id = str(client_id)
        self.client_secret = str(client_secret)
        self.refresh_token = str(refresh_token)
        self.profile_id = str(profile_id)
        self.BASE_URL = _REGION_HOSTS.get(region.upper(), _REGION_HOSTS["NA"])

        # Per-instance token state
        self._access_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

        # _make_request's 401 path calls self.token_manager.force_refresh();
        # the client IS its own token manager in the multi-tenant model.
        self.token_manager = self

        logger.info(
            f"🔧 AmazonAdsClient ready (profile={self.profile_id}, host={self.BASE_URL})"
        )

    # ------------------------------------------------------------------ #
    # Auth
    # ------------------------------------------------------------------ #
    def _needs_refresh(self) -> bool:
        if not self._access_token or not self._expires_at:
            return True
        return (self._expires_at - datetime.utcnow()).total_seconds() < _TOKEN_EXPIRY_BUFFER

    def _refresh_access_token(self) -> str:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = requests.post(_TOKEN_URL, data=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._expires_at = datetime.utcnow() + timedelta(
            seconds=data.get("expires_in", 3600)
        )
        # Amazon occasionally rotates the refresh token
        rotated = data.get("refresh_token")
        if rotated and rotated != self.refresh_token:
            logger.info("🔄 Refresh token rotated by Amazon for this tenant")
            self.refresh_token = rotated
        logger.info("✅ Access token refreshed for profile %s", self.profile_id)
        return self._access_token

    def get_valid_access_token(self) -> str:
        if self._needs_refresh():
            self._refresh_access_token()
        return self._access_token

    def force_refresh(self) -> str:
        """Force an immediate token refresh (used by the 401 retry path)."""
        self._access_token = None
        self._expires_at = None
        return self._refresh_access_token()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.get_valid_access_token()}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": self.profile_id,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------ #
    # Transport
    # ------------------------------------------------------------------ #
    def _execute_request_once(
        self, method: str, url: str, payload: Optional[Union[List, Dict]] = None
    ) -> requests.Response:
        headers = self._get_headers()
        if payload is not None:
            return requests.request(method, url, headers=headers, json=payload, timeout=30)
        return requests.request(method, url, headers=headers, timeout=30)

    def _make_request(
        self, method: str, endpoint: str, payload: Optional[Union[List, Dict]] = None
    ) -> Optional[Any]:
        """
        Centralized request handler:
          - sanitizes payloads (Decimal -> float)
          - on 401: forces a token refresh and retries once
          - on 429: re-raises so the @retry decorator backs off
          - other 4xx/5xx: logs and returns None
        """
        url = f"{self.BASE_URL}{endpoint}"
        if payload is not None:
            payload = safe_serialize(payload)

        try:
            response = self._execute_request_once(method, url, payload)
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 401:
                logger.warning("⚠️ 401 Unauthorized — refreshing token and retrying once")
                self.force_refresh()
                try:
                    retry_resp = self._execute_request_once(method, url, payload)
                    retry_resp.raise_for_status()
                    return retry_resp.json() if retry_resp.content else {}
                except Exception as retry_e:
                    logger.error(f"❌ Retry after refresh failed for {url}: {retry_e}")
                    return None
            elif status == 429:
                logger.warning(f"⚠️ 429 rate limited for {url} — backing off")
                raise
            else:
                body = getattr(e.response, "text", "")
                logger.error(f"❌ HTTP {status} for {url}: {body}")
                return None
        except Exception as e:
            logger.error(f"❌ Request to {url} failed: {e}")
            return None

    # ------------------------------------------------------------------ #
    # Sponsored Products operations
    # ------------------------------------------------------------------ #
    def get_keyword_bid_recommendations(self, keyword_id: Union[str, int]) -> Optional[Dict]:
        endpoint = f"/v2/sp/keywords/{str(keyword_id)}/bidRecommendations"
        return self._make_request("GET", endpoint)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.HTTPError),
    )
    def update_keyword_bid(
        self,
        keyword_id: Union[str, int],
        new_bid: float,
        use_amazon_suggested: bool = False,
    ) -> Optional[Dict]:
        """Update a single keyword's bid."""
        if use_amazon_suggested:
            rec = self.get_keyword_bid_recommendations(keyword_id)
            if rec and "suggestedBid" in rec:
                new_bid = float(rec["suggestedBid"])

        if settings.dry_run:
            logger.info(f"[DRY RUN] keyword {keyword_id} -> ${float(new_bid):.2f}")
            return {"status": "dry_run_success"}

        payload = [{
            "keywordId": str(keyword_id),
            "bid": round(float(new_bid), 2),
            "state": "ENABLED",
        }]
        result = self._make_request("PUT", "/v2/sp/keywords", payload)
        if result is not None:
            logger.info(f"✅ keyword {keyword_id} -> ${float(new_bid):.2f}")
        return result

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.HTTPError),
    )
    def create_keyword(
        self,
        campaign_id: Union[str, int],
        ad_group_id: Union[str, int],
        keyword_text: str,
        match_type: str,
        bid: float,
    ) -> Optional[Dict]:
        if settings.dry_run:
            logger.info(f"[DRY RUN] create keyword '{keyword_text}' ({match_type})")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "adGroupId": str(ad_group_id),
            "keywordText": str(keyword_text),
            "matchType": str(match_type),
            "state": "ENABLED",
            "bid": round(float(bid), 2),
        }]
        return self._make_request("POST", "/v2/sp/keywords", payload)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_negative_keyword(
        self,
        campaign_id: Union[str, int],
        keyword_text: str,
        match_type: str = "NEGATIVE_EXACT",
    ) -> Optional[Dict]:
        if settings.dry_run:
            logger.info(f"[DRY RUN] negative '{keyword_text}' ({match_type})")
            return {"status": "dry_run_success"}

        payload = [{
            "campaignId": str(campaign_id),
            "keywordText": str(keyword_text),
            "matchType": str(match_type),
            "state": "ENABLED",
        }]
        return self._make_request("POST", "/v2/sp/campaignNegativeKeywords", payload)

    def batch_update_keyword_bids(
        self,
        bid_updates: List[Dict[str, Any]],
        use_amazon_suggested: bool = False,
    ) -> Dict[str, int]:
        """
        Batch update keyword bids in one API call.
        Each item: {"keywordId": <id>, "bid": <float>}.
        """
        if not bid_updates:
            return {"success": 0, "failed": 0}

        payload = []
        for upd in bid_updates:
            kid = upd.get("keywordId")
            bid_value = round(float(upd.get("bid", 0.0)), 2)
            if kid is None or bid_value <= 0:
                continue
            payload.append({"keywordId": str(kid), "bid": bid_value, "state": "ENABLED"})

        if not payload:
            return {"success": 0, "failed": 0}

        if settings.dry_run:
            logger.info(f"[DRY RUN] batch update {len(payload)} keyword bids")
            return {"success": len(payload), "failed": 0}

        result = self._make_request("PUT", "/v2/sp/keywords", payload)
        if result is not None:
            try:
                success = sum(
                    1 for r in result
                    if str(r.get("code", "")).upper().startswith("SUCCESS")
                    or r.get("status") == "SUCCESS"
                )
                failed = len(payload) - success
            except Exception:
                success, failed = len(payload), 0
            logger.info(f"✅ batch: {success} ok, {failed} failed")
            return {"success": success, "failed": failed}
        return {"success": 0, "failed": len(payload)}


__all__ = ["AmazonAdsClient", "safe_serialize"]
