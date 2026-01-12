"""
Cloud Run-friendly live verification for Amazon Ads API and BigQuery.

Usage in Cloud Run Jobs:
  python -m automation.verify_live_data [--skip-bq] [--skip-amazon]
"""

import json
import sys
import argparse
from datetime import datetime, timezone

import requests
from google.cloud import secretmanager, bigquery

from .shared.config import settings
from .shared.token_manager import get_token_manager
from .shared.logger import get_logger

logger = get_logger(__name__)


def _get_secret(sm: secretmanager.SecretManagerServiceClient, project_id: str, name: str) -> str:
    path = f"projects/{project_id}/secrets/{name}/versions/latest"
    resp = sm.access_secret_version(request={"name": path})
    return resp.payload.data.decode("UTF-8")


def verify_amazon_api() -> dict:
    """Verify Amazon Ads API by fetching profiles, campaigns, and keywords."""
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

    # Profiles
    try:
        r = requests.get(f"{base}/v2/profiles", headers=headers, timeout=30)
        r.raise_for_status()
        profiles = r.json()
        step("profiles", True, f"profiles={len(profiles)}")
    except Exception as e:
        step("profiles", False, str(e))

    # Enabled campaigns
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

    # Enabled keywords
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


def verify_bigquery_freshness() -> dict:
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
            max_ts = row["max_date"] if row else None
            max_date = max_ts.date() if max_ts else None
            is_fresh = max_date == today
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
    args = parser.parse_args()

    logger.info("🔎 Verifying live Amazon + BigQuery data...")

    api = {"ok": True}
    bq = {"ok": True}

    if not args.skip_amazon:
        try:
            api = verify_amazon_api()
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
