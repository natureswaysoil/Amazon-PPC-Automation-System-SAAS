"""
Main bid optimization job (runs hourly via Cloud Scheduler / Cloud Run Job).

Multi-tenant: the job optimizes ONE tenant per invocation. The tenant is
either passed in (programmatic) or loaded from the environment (tenant_from_env).
The Amazon client is built from that tenant's own credentials, and bid math
comes from the single canonical BidCalculator in shared/rules_engine.py.
"""

import sys
import logging
from datetime import datetime
from typing import List, Dict, Optional

import pytz

# Ensure the container root is importable
sys.path.insert(0, "/app")

try:
    from automation.shared.config import settings
    from automation.shared.rules_engine import BidCalculator
    from automation.shared.amazon_client import AmazonAdsClient  # noqa: F401
    from automation.shared.tenant import TenantConfig, tenant_from_env, build_amazon_client
    from shared.bigquery_client import BigQueryClient
    try:
        from aov_fetcher import aov_fetcher
    except Exception:  # optional dependency
        aov_fetcher = None
except ImportError as e:  # pragma: no cover - import guard for local syntax checks
    logging.warning(f"Import warning: {e}. Ensure PYTHONPATH is set correctly.")
    settings = type("obj", (object,), {"timezone": "America/New_York", "dry_run": True, "default_aov": 35.0})
    BidCalculator = object
    BigQueryClient = object
    TenantConfig = object
    aov_fetcher = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BidOptimizer:
    """Optimizes keyword bids for a single tenant."""

    def __init__(self, tenant: Optional["TenantConfig"] = None):
        # Resolve tenant (explicit arg wins; else load from environment)
        if tenant is None:
            try:
                tenant = tenant_from_env()
            except Exception as e:
                logger.warning(f"No tenant configured from env: {e}")
                tenant = None
        self.tenant = tenant

        # BigQuery client (per-tenant dataset when available)
        try:
            if tenant and tenant.bq_project and tenant.bq_dataset:
                self.bq_client = BigQueryClient(tenant.bq_project, tenant.bq_dataset)
            else:
                self.bq_client = BigQueryClient()
        except Exception as e:
            logger.warning(f"BigQuery client not initialized: {e}")
            self.bq_client = None

        # Amazon client (built from THIS tenant's credentials)
        try:
            self.amazon_client = build_amazon_client(tenant) if tenant else None
        except Exception as e:
            logger.warning(f"Amazon client not initialized: {e}")
            self.amazon_client = None

        self.bid_calculator = BidCalculator(
            target_acos=getattr(tenant, "target_acos", None) if tenant else None
        )

        tz_name = getattr(settings, "timezone", "America/New_York")
        self.tz = pytz.timezone(tz_name)

        self.stats = {
            "keywords_evaluated": 0,
            "bids_updated": 0,
            "bids_unchanged": 0,
            "errors": 0,
            "total_bid_increase": 0.0,
            "total_bid_decrease": 0.0,
        }

    def _aov_for(self, keyword: Dict) -> Optional[float]:
        """Best-effort AOV lookup for a keyword's ASIN."""
        if aov_fetcher is not None:
            try:
                asin = keyword.get("advertisedAsin") or keyword.get("asin")
                if asin:
                    data = aov_fetcher.get_aov(asin)
                    # aov_fetcher may return an object or a float
                    return float(getattr(data, "aov", data))
            except Exception:
                pass
        if keyword.get("aov") is not None:
            return float(keyword["aov"])
        return None

    def run(self):
        logger.info("=" * 60)
        logger.info("🚀 Starting Bid Optimization Job")
        if self.tenant:
            logger.info(f"Tenant: {self.tenant.masked()}")
        logger.info(f"Timestamp: {datetime.now(self.tz).isoformat()}")
        logger.info(f"Dry Run: {getattr(settings, 'dry_run', True)}")
        logger.info("=" * 60)

        try:
            if aov_fetcher is not None:
                logger.info("💰 Loading AOV data")
                try:
                    aov_fetcher.fetch_all()
                except Exception as e:
                    logger.warning(f"AOV preload skipped: {e}")

            logger.info("🔍 Loading keywords")
            if self.bq_client:
                keywords = self.bq_client.get_keywords_for_optimization(
                    min_clicks=5, days_lookback=14
                )
            else:
                keywords = []
                logger.warning("No BigQuery client available.")

            logger.info(f"Found {len(keywords)} keywords to evaluate")
            if not keywords:
                logger.warning("⚠️ No keywords to optimize")
                return

            current_hour = datetime.now(self.tz).hour
            bid_updates: List[Dict] = []

            for keyword in keywords:
                self.stats["keywords_evaluated"] += 1

                result = self.bid_calculator.calculate_optimal_bid_from_data(
                    keyword_data=keyword,
                    current_hour=current_hour,
                    asin_aov=self._aov_for(keyword),
                    user_override=keyword.get("user_override"),
                    override_expires_at=keyword.get("override_expires_at"),
                )

                if not result["should_update"]:
                    self.stats["bids_unchanged"] += 1
                    continue

                current_bid = float(keyword.get("current_bid", 0.0))
                optimal_bid = result["optimal_bid"]
                bid_change = optimal_bid - current_bid

                bid_updates.append(
                    {"keywordId": str(keyword["keywordId"]), "bid": optimal_bid}
                )

                if self.bq_client:
                    try:
                        self.bq_client.log_bid_change(
                            keyword_id=str(keyword["keywordId"]),
                            old_bid=current_bid,
                            new_bid=optimal_bid,
                            reason=result["reason"],
                        )
                    except Exception as e:
                        logger.warning(f"log_bid_change failed: {e}")

                self.stats["bids_updated"] += 1
                if bid_change > 0:
                    self.stats["total_bid_increase"] += bid_change
                else:
                    self.stats["total_bid_decrease"] += abs(bid_change)

                logger.info(
                    f"📈 {keyword.get('keywordText', 'Unknown')}: "
                    f"${current_bid:.2f} → ${optimal_bid:.2f} ({result['reason']})"
                )

            # Apply (the client itself honors dry_run)
            if bid_updates and self.amazon_client:
                logger.info(f"🔄 Applying {len(bid_updates)} bid updates")
                res = self.amazon_client.batch_update_keyword_bids(bid_updates)
                logger.info(f"✅ Updated: {res.get('success', 0)}, failed: {res.get('failed', 0)}")
            elif bid_updates:
                logger.warning("Amazon client missing — computed updates not pushed.")
            else:
                logger.info("✅ No bid updates needed")

            self._print_summary()

        except Exception as e:
            logger.error(f"❌ Bid optimization job failed: {e}", exc_info=True)
            self.stats["errors"] += 1
            sys.exit(1)

    def _print_summary(self):
        s = self.stats
        net = s["total_bid_increase"] - s["total_bid_decrease"]
        logger.info("=" * 60)
        logger.info("📊 JOB SUMMARY")
        logger.info(f"Keywords Evaluated: {s['keywords_evaluated']}")
        logger.info(f"Bids Updated:       {s['bids_updated']}")
        logger.info(f"Bids Unchanged:     {s['bids_unchanged']}")
        logger.info(f"Errors:             {s['errors']}")
        logger.info(f"Total Increase:     ${s['total_bid_increase']:.2f}")
        logger.info(f"Total Decrease:     ${s['total_bid_decrease']:.2f}")
        logger.info(f"Net Change:         ${net:.2f}")
        logger.info("=" * 60)


def main():
    """Entry point for the Cloud Run Job."""
    BidOptimizer().run()


if __name__ == "__main__":
    main()
