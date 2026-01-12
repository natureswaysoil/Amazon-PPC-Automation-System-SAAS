"""
Minimal Winning Bid Finder

Find the minimum bid that yields impressions for a single keyword
by bisection search, using Amazon Ads Reporting v3 for measurement.

Safe defaults:
- Respects DRY_RUN for bid updates (reports still fetch).
- Small report windows for speed.

CLI:
  KEYWORD_ID=123456 python -m automation.min_winning_bid
Optional envs:
  CAMPAIGN_ID, ADGROUP_ID, OBSERVE_MIN, MAX_ITERS, MIN_BID, MAX_BID,
  REPORT_TYPE_ID, REPORT_COLUMNS, TIME_UNIT, REPORT_FORMAT
"""

import os
import time
import json
import math
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict

import requests
import pandas as pd

from .shared.config import settings
from .shared.amazon_client import AmazonAdsClient
from .shared.logger import get_logger

logger = get_logger(__name__)

# ---- CONFIG (env-friendly) ----
MIN_BID = float(os.getenv("MIN_BID", "0.35"))
MAX_BID = float(os.getenv("MAX_BID", "5.00"))
BID_PRECISION = int(os.getenv("BID_PRECISION", "2"))          # cents
OBSERVE_MIN = int(os.getenv("OBSERVE_MIN", "20"))             # minutes to wait after changing bid
MAX_ITERS = int(os.getenv("MAX_ITERS", "8"))                  # bisection iterations
MIN_IMPRESSIONS = int(os.getenv("MIN_IMPRESSIONS", "1"))      # "win" threshold
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1"))          # report window (keep small for speed)
USE_BATCH_UPDATE = os.getenv("USE_BATCH_UPDATE", "false").lower() == "true"

REPORT_TYPE_ID = os.getenv("REPORT_TYPE_ID", "spSearchTerm")
AD_PRODUCT = os.getenv("AD_PRODUCT", "SPONSORED_PRODUCTS")
TIME_UNIT = os.getenv("TIME_UNIT", "SUMMARY")
REPORT_FORMAT = os.getenv("REPORT_FORMAT", "GZIP_JSON")

REPORT_COLUMNS = os.getenv(
    "REPORT_COLUMNS",
    "campaignId,adGroupId,keywordId,clicks,impressions,cost,attributedSales14d,attributedConversions14d"
).split(",")


# ---- HELPERS ----
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def round_bid(x: float) -> float:
    p = 10 ** BID_PRECISION
    return math.floor(x * p + 0.5) / p


def download_report_to_df(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    # v3 often returns gzip JSON lines
    if r.content[:2] == b"\x1f\x8b":
        import gzip
        from io import BytesIO
        with gzip.GzipFile(fileobj=BytesIO(r.content)) as gz:
            lines = gz.read().splitlines()
        rows = [json.loads(line) for line in lines if line.strip()]
        return pd.DataFrame(rows)
    # fallback jsonl/csv
    try:
        from io import BytesIO
        return pd.read_json(BytesIO(r.content), lines=True)
    except Exception:
        from io import BytesIO
        return pd.read_csv(BytesIO(r.content))


@dataclass
class WinResult:
    bid: float
    impressions: int
    clicks: int


def utc_date_str(days_ago: int) -> str:
    d = pd.Timestamp.utcnow().date() - pd.Timedelta(days=days_ago)
    return d.strftime("%Y-%m-%d")


# ---- Reporting v3 helpers (API paths may vary; adapted generically) ----
def build_v3_report_payload(name: str, start_date: str, end_date: str) -> dict:
    return {
        "name": name,
        "startDate": start_date,
        "endDate": end_date,
        "configuration": {
            "adProduct": AD_PRODUCT,
            "reportTypeId": REPORT_TYPE_ID,
            "timeUnit": TIME_UNIT,
            "format": REPORT_FORMAT,
            "columns": REPORT_COLUMNS,
        },
    }


def create_v3_report(ads: AmazonAdsClient, payload: Dict) -> str:
    """POST to reporting v3 to create a report. Returns report id."""
    url = f"{ads.BASE_URL}/reporting/reports"
    resp = requests.post(url, headers=ads._get_headers(), json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("reportId") or data.get("id")


def wait_v3_report_url(ads: AmazonAdsClient, report_id: str, timeout_s: int = 900) -> str:
    """Poll report status until ready and return the download URL."""
    url = f"{ads.BASE_URL}/reporting/reports/{report_id}"
    deadline = time.time() + timeout_s
    last_status = None
    while time.time() < deadline:
        resp = requests.get(url, headers=ads._get_headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = (data.get("status") or data.get("processingStatus") or "").upper()
        last_status = status
        if status in {"SUCCESS", "COMPLETED", "DONE"}:
            # The location field name can vary
            dl = data.get("url") or data.get("location") or data.get("reportLocation")
            if not dl:
                raise RuntimeError(f"Report {report_id} ready but missing download URL: {data}")
            return dl
        if status in {"FAILURE", "FAILED", "ERROR"}:
            raise RuntimeError(f"Report {report_id} failed: {data}")
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for report {report_id}, last_status={last_status}")


def measure_keyword_win(
    ads: AmazonAdsClient,
    keyword_id: int,
    campaign_id: Optional[int] = None,
    ad_group_id: Optional[int] = None,
) -> WinResult:
    """Pull a small v3 report window and compute impressions/clicks for this keyword_id."""
    start_date = utc_date_str(LOOKBACK_DAYS)
    end_date = utc_date_str(1)

    payload = build_v3_report_payload(
        name=f"minwin_{keyword_id}_{int(time.time())}",
        start_date=start_date,
        end_date=end_date,
    )

    report_id = create_v3_report(ads, payload)
    url = wait_v3_report_url(ads, report_id, timeout_s=900)

    df = download_report_to_df(url)
    if df.empty:
        return WinResult(bid=float("nan"), impressions=0, clicks=0)

    # Normalize common column naming
    cols = {c.lower(): c for c in df.columns}

    def get_col(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    kid_col = get_col("keywordId", "keywordid")
    imp_col = get_col("impressions")
    clk_col = get_col("clicks")

    if not kid_col or not imp_col:
        raise RuntimeError(f"Report missing required columns. Have: {list(df.columns)[:50]}")

    df[kid_col] = pd.to_numeric(df[kid_col], errors="coerce").fillna(-1).astype(int)
    df[imp_col] = pd.to_numeric(df[imp_col], errors="coerce").fillna(0).astype(int)
    if clk_col:
        df[clk_col] = pd.to_numeric(df[clk_col], errors="coerce").fillna(0).astype(int)
    else:
        df["__clicks"] = 0
        clk_col = "__clicks"

    sub = df[df[kid_col] == int(keyword_id)]

    # Optional: also filter to campaign/adgroup if provided
    if campaign_id is not None:
        cid_col = get_col("campaignId", "campaignid")
        if cid_col:
            sub = sub[pd.to_numeric(sub[cid_col], errors="coerce").fillna(-1).astype(int) == int(campaign_id)]
    if ad_group_id is not None:
        ag_col = get_col("adGroupId", "adgroupid")
        if ag_col:
            sub = sub[pd.to_numeric(sub[ag_col], errors="coerce").fillna(-1).astype(int) == int(ad_group_id)]

    impressions = int(sub[imp_col].sum()) if not sub.empty else 0
    clicks = int(sub[clk_col].sum()) if not sub.empty else 0
    return WinResult(bid=float("nan"), impressions=impressions, clicks=clicks)


def set_keyword_bid(ads: AmazonAdsClient, keyword_id: int, bid: float) -> float:
    """Update keyword bid using the project's client. Respects DRY_RUN.

    If `USE_BATCH_UPDATE=true` and the client supports `batch_update_keyword_bids`,
    use a single-item batch; otherwise fall back to `update_keyword_bid`.
    """
    bid = round_bid(clamp(bid, MIN_BID, MAX_BID))
    if settings.dry_run:
        logger.info(f"[DRY RUN] Would update keyword {keyword_id} bid to {bid:.2f}")
        return bid

    update = {"keywordId": str(int(keyword_id)), "bid": float(bid)}
    if USE_BATCH_UPDATE and hasattr(ads, "batch_update_keyword_bids"):
        result = ads.batch_update_keyword_bids([update])
        if (result or {}).get("failed", 0) != 0:
            raise RuntimeError(f"Batch update failed for keyword {keyword_id}: {result}")
        return bid

    ok = ads.update_keyword_bid(str(int(keyword_id)), float(bid))
    if not ok:
        raise RuntimeError(f"Failed to update bid for keyword {keyword_id}")
    return bid


def find_min_winning_bid(
    ads: AmazonAdsClient,
    *,
    keyword_id: int,
    campaign_id: Optional[int] = None,
    ad_group_id: Optional[int] = None,
    lo: float = MIN_BID,
    hi: float = MAX_BID,
) -> Tuple[float, List[Dict]]:
    """
    Bisection search for minimal bid that yields >= MIN_IMPRESSIONS impressions
    in the observe window. Returns: (best_bid, history)
    """
    history: List[Dict] = []

    # Ensure hi actually wins; otherwise no winner will be found.
    hi = round_bid(hi)
    set_keyword_bid(ads, keyword_id, hi)
    logger.info(f"Observing at bid={hi:.2f} for {OBSERVE_MIN} min...")
    time.sleep(OBSERVE_MIN * 60)
    r_hi = measure_keyword_win(ads, keyword_id, campaign_id, ad_group_id)
    history.append({"bid": hi, "impressions": r_hi.impressions, "clicks": r_hi.clicks})
    if r_hi.impressions < MIN_IMPRESSIONS:
        raise RuntimeError(
            f"Even MAX_BID={hi:.2f} did not win (impressions={r_hi.impressions}). "
            f"Increase MAX_BID or OBSERVE_MIN, or check keyword/adgroup eligibility."
        )

    best = hi
    lo = round_bid(lo)

    for _ in range(MAX_ITERS):
        mid = round_bid((lo + best) / 2.0)
        if mid >= best or abs(best - lo) < (10 ** -BID_PRECISION):
            break

        set_keyword_bid(ads, keyword_id, mid)
        logger.info(f"Observing at bid={mid:.2f} for {OBSERVE_MIN} min...")
        time.sleep(OBSERVE_MIN * 60)
        r = measure_keyword_win(ads, keyword_id, campaign_id, ad_group_id)

        won = r.impressions >= MIN_IMPRESSIONS
        history.append({"bid": mid, "impressions": r.impressions, "clicks": r.clicks, "won": won})

        if won:
            best = mid
        else:
            lo = mid

    return best, history


def main():
    """CLI entry point."""
    keyword_id = int(os.environ["KEYWORD_ID"])  # required
    campaign_id = int(os.getenv("CAMPAIGN_ID")) if os.getenv("CAMPAIGN_ID") else None
    ad_group_id = int(os.getenv("ADGROUP_ID")) if os.getenv("ADGROUP_ID") else None

    ads = AmazonAdsClient()

    best, hist = find_min_winning_bid(
        ads,
        keyword_id=keyword_id,
        campaign_id=campaign_id,
        ad_group_id=ad_group_id,
        lo=MIN_BID,
        hi=MAX_BID,
    )

    print("\n=== RESULT ===")
    print("best_bid:", best)
    try:
        import pandas as _pd
        print(_pd.DataFrame(hist).to_string(index=False))
    except Exception:
        print(json.dumps(hist, indent=2))


if __name__ == "__main__":
    main()
