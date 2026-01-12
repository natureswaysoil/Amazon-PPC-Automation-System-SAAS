"""
Verify live connectivity and data from Amazon Ads API and BigQuery.

Checks:
- Amazon Ads API auth via Secret Manager + token refresh (default)
- Optional env-mode: uses AMAZON_* env vars to fetch access token
- Simple GET for profiles and one enabled campaign/keyword
- BigQuery freshness for core tables (optional)

Usage:
    PYTHONPATH=. python verify_live_data.py [--skip-bq] [--skip-amazon] [--from-env]
"""

import json
import sys
from datetime import datetime, timezone
from typing import Dict
import argparse
import os

import requests
from google.cloud import secretmanager, bigquery

try:
    from shared.config import settings
    from shared.token_manager import get_token_manager
    from shared.logger import get_logger
except Exception:
    print("Ensure PYTHONPATH includes repo root; run with PYTHONPATH=.")
    raise

logger = get_logger(__name__)


def _get_secret(sm: secretmanager.SecretManagerServiceClient, project_id: str, name: str) -> str:
    path = f"projects/{project_id}/secrets/{name}/versions/latest"
    resp = sm.access_secret_version(request={"name": path})
    return resp.payload.data.decode("UTF-8")


def _get_access_token_from_env(client_id: str, client_secret: str, refresh_token: str) -> str:
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    r = requests.post(url, data=payload, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def verify_amazon_api(from_env: bool = False) -> Dict:
    """Verify Amazon Ads API by fetching profiles and sampling data."""
    headers = {}
    if from_env:
        client_id = os.getenv("AMAZON_CLIENT_ID")
        client_secret = os.getenv("AMAZON_CLIENT_SECRET")
        refresh_token = os.getenv("AMAZON_REFRESH_TOKEN")
        profile_id = os.getenv("AMAZON_PROFILE_ID")
        if not all([client_id, client_secret, refresh_token, profile_id]):
            raise RuntimeError("Missing AMAZON_* env vars: AMAZON_CLIENT_ID, AMAZON_CLIENT_SECRET, AMAZON_REFRESH_TOKEN, AMAZON_PROFILE_ID")
        access_token = _get_access_token_from_env(client_id, client_secret, refresh_token)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": "application/json",
        }
    else:
        sm = secretmanager.SecretManagerServiceClient()
        project_id = settings.project_id
        profile_id = _get_secret(sm, project_id, "amazon_profile_id")
        tm = get_token_manager()
        access_token = tm.get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": tm.client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": "application/json",
        }

    base = "https://advertising-api.amazon.com"
    results = {"ok": False, "steps": []}

    def step(name: str, ok: bool, detail: str = ""):
        results["steps"].append({"name": name, "ok": ok, "detail": detail})

    # 1) Profiles
    try:
        r = requests.get(f"{base}/v2/profiles", headers=headers, timeout=30)
        r.raise_for_status()
        profiles = r.json()
        step("profiles", True, f"profiles={len(profiles)}")
    except Exception as e:
        step("profiles", False, str(e))

    # 2) Sample enabled campaigns (limit by count query param if supported)
    try:
        r = requests.get(
            f"{base}/v2/sp/campaigns?stateFilter=enabled",
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        campaigns = r.json()
        sample = campaigns[:1] if isinstance(campaigns, list) else []
        step("campaigns", True, f"sample_count={len(sample)}")
    except Exception as e:
        step("campaigns", False, str(e))

    # 3) Sample enabled keywords
    try:
        r = requests.get(
            f"{base}/v2/sp/keywords?stateFilter=enabled",
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        keywords = r.json()
        sample = keywords[:1] if isinstance(keywords, list) else []
        step("keywords", True, f"sample_count={len(sample)}")
    except Exception as e:
        step("keywords", False, str(e))

    results["ok"] = all(s["ok"] for s in results["steps"]) 
    return results


def verify_bigquery_freshness() -> Dict:
    """Check latest partition date for core tables in BigQuery."""
    client = bigquery.Client(project=settings.project_id)
    dataset = settings.dataset_id

    queries = {
        "sp_targeting_metrics": f"SELECT MAX(segments_date) AS max_date FROM `{settings.project_id}.{dataset}.sp_targeting_metrics`",
        "sp_advertised_product_metrics": f"SELECT MAX(segments_date) AS max_date FROM `{settings.project_id}.{dataset}.sp_advertised_product_metrics`",
    }

    results = {"ok": True, "tables": {}}
    today = datetime.now(timezone.utc).date()

    for table, q in queries.items():
        try:
            rows = client.query(q).result()
            row = next(iter(rows), None)
            max_date = row["max_date"].date() if row and row["max_date"] else None
            is_fresh = max_date in {today, today}  # allow same-day freshness
            results["tables"][table] = {"max_date": str(max_date), "fresh_today": bool(is_fresh)}
            if not is_fresh:
                results["ok"] = False
        except Exception as e:
            results["tables"][table] = {"error": str(e)}
            results["ok"] = False

    return results


def main():
    parser = argparse.ArgumentParser(description="Verify live data from Amazon Ads API and BigQuery")
    parser.add_argument("--skip-bq", action="store_true", help="Skip BigQuery freshness check")
    parser.add_argument("--skip-amazon", action="store_true", help="Skip Amazon Ads API checks")
    parser.add_argument("--from-env", action="store_true", help="Use AMAZON_* env vars for Amazon auth instead of Secret Manager")
    args = parser.parse_args()

    logger.info("🔎 Verifying live Amazon + BigQuery data...")

    api = {"ok": True, "steps": []}
    bq = {"ok": True}

    if not args.skip_amazon:
        try:
            api = verify_amazon_api(from_env=args.from_env)
        except Exception as e:
            api = {"ok": False, "error": str(e)}
        logger.info(json.dumps({"amazon_api": api}, ensure_ascii=False))

    if not args.skip_bq:
        try:
            bq = verify_bigquery_freshness()
        except Exception as e:
            bq = {"ok": False, "error": str(e)}
        logger.info(json.dumps({"bigquery": bq}, ensure_ascii=False))

    ok = (api.get("ok", True)) and (bq.get("ok", True))
    print(json.dumps({"ok": ok, "amazon_api": api, "bigquery": bq}, indent=2))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
