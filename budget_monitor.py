"""
Budget pacing monitor
Runs every 15 minutes to prevent budget burn before evening
"""

import sys
import os
from datetime import datetime
import pytz
import logging
from typing import List, Dict, Any
from google.cloud import bigquery

# Add project root to path
sys.path.insert(0, '/app')

try:
    from automation.shared.config import settings
    from shared.bigquery_client import BigQueryClient
    from automation.shared.amazon_client import AmazonAdsClient
except ImportError:
    # Fallback/Mock for syntax checking or standalone execution
    class MockSettings:
        timezone: str = 'America/Los_Angeles'
        dry_run: bool = False
        PROJECT_ID: str = 'amazon-ppc-474902'
        BIGQUERY_DATASET: str = 'amazon_ppc'
        budget_critical_threshold_3pm: float = 0.75
        budget_warning_threshold_3pm: float = 0.65
        min_bid: float = 0.20
    settings = MockSettings()

    class MockBigQueryClient:
        def get_campaign_budget_status(self) -> List[Dict[str, Any]]:
            # Mock data for testing
            return [
                {"campaignId": "101", "campaign_name": "Campaign A", "budget": 100.0, "spend_today": 60.0},
                {"campaignId": "102", "campaign_name": "Campaign B", "budget": 200.0, "spend_today": 160.0},
                {"campaignId": "103", "campaign_name": "Campaign C", "budget": 50.0, "spend_today": 20.0},
                {"campaignId": "104", "campaign_name": "Campaign D (Exhausted)", "budget": 100.0, "spend_today": 98.0},
            ]
    BigQueryClient = MockBigQueryClient

    class MockAmazonAdsClient:
        def batch_update_keyword_bids(self, bid_updates: List[Dict[str, Any]]) -> Dict[str, int]:
            logger.info(f"MockAmazonAdsClient: Simulating batch update for {len(bid_updates)} keywords.")
            return {"success": len(bid_updates), "failed": 0}
    AmazonAdsClient = MockAmazonAdsClient

# Setup Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BudgetMonitor:
    """
    Monitor campaign budgets and prevent early depletion
    Critical checkpoint: 3 PM must be under 65% spend
    """

    def __init__(self):
        # Initialize clients safely
        self.bq_client: Any = None
        self.amazon_client: Any = None
        self.raw_bq_client: bigquery.Client = None

        try:
            self.bq_client = BigQueryClient()
            self.amazon_client = AmazonAdsClient()
            # Ensure settings.PROJECT_ID is accessible even if main settings load fails
            # Use getattr for robustness if MockSettings doesn't have PROJECT_ID
            project_id = getattr(settings, 'PROJECT_ID', None)
            if project_id:
                self.raw_bq_client = bigquery.Client(project=project_id)
            else:
                logger.warning("PROJECT_ID not found in settings, raw BigQuery client will not be initialized.")

        except Exception as e:
            logger.warning(f"Clients failed to initialize. Ensure environment is set up. Error: {e}")
            if settings.dry_run:
                logger.info("Running in DRY RUN mode, using mock clients for `get_campaign_budget_status` and `batch_update_keyword_bids`.")
                # Define lightweight mocks for DRY RUN if imports or credentials failed
                class _LocalMockBigQueryClient:
                    def get_campaign_budget_status(self) -> List[Dict[str, Any]]:
                        return [
                            {"campaignId": "101", "campaign_name": "Campaign A", "budget": 100.0, "spend_today": 60.0},
                            {"campaignId": "102", "campaign_name": "Campaign B", "budget": 200.0, "spend_today": 160.0},
                            {"campaignId": "103", "campaign_name": "Campaign C", "budget": 50.0, "spend_today": 20.0},
                            {"campaignId": "104", "campaign_name": "Campaign D (Exhausted)", "budget": 100.0, "spend_today": 98.0},
                        ]
                class _LocalMockAmazonAdsClient:
                    def batch_update_keyword_bids(self, bid_updates: List[Dict[str, Any]]) -> Dict[str, int]:
                        logger.info(f"MockAmazonAdsClient: Simulating batch update for {len(bid_updates)} keywords.")
                        return {"success": len(bid_updates), "failed": 0}

                if not self.bq_client:
                    self.bq_client = _LocalMockBigQueryClient()
                if not self.amazon_client:
                    self.amazon_client = _LocalMockAmazonAdsClient()
            else:
                logger.error("Clients failed to initialize in non-dry-run mode. Exiting.")
                sys.exit(1)


        tz_name = settings.timezone
        self.tz = pytz.timezone(tz_name)

        self.alerts: List[Dict[str, Any]] = []
        self.emergency_actions: List[Dict[str, Any]] = []

    def run(self):
        """Main monitoring workflow"""
        current_time = datetime.now(self.tz)
        current_hour = current_time.hour # e.g., 15 for 3 PM
        # current_minute = current_time.minute # Not used, can be removed

        logger.info("=" * 60)
        logger.info("⏰ Budget Monitor Job Started")
        logger.info(f"Timestamp: {current_time.isoformat()}")
        logger.info(f"Current Hour: {current_hour}")
        logger.info(f"Dry Run Mode: {settings.dry_run}")
        logger.info("=" * 60)

        try:
            # Step 1: Get campaign budget status
            if self.bq_client is None:
                logger.error("BigQuery client not initialized. Cannot fetch campaigns.")
                return

            campaigns = self.bq_client.get_campaign_budget_status()

            if not campaigns:
                logger.warning("⚠️ No active campaigns found or client failed to retrieve data.")
                return

            logger.info(f"\n📊 Monitoring {len(campaigns)} campaigns")

            for campaign in campaigns:
                self._check_campaign_budget(campaign, current_hour)

            # Summary
            self._print_summary()

            logger.info("\n✅ Budget Monitor Job Completed Successfully")

        except Exception as e:
            logger.error(f"❌ Budget monitor job failed unexpectedly: {e}", exc_info=True)
            sys.exit(1)

    def _check_campaign_budget(self, campaign: Dict, current_hour: int):
        """Check individual campaign budget pacing"""
        campaign_id = campaign.get("campaignId")
        campaign_name = campaign.get("campaign_name", "Unknown Campaign")
        budget = float(campaign.get("budget", 0))
        spend_today = float(campaign.get("spend_today", 0))

        if budget <= 0:
            logger.warning(f"Skipping campaign {campaign_name} ({campaign_id}) due to invalid budget: {budget}")
            return

        spend_pct = spend_today / budget

        logger.info(f"  - Campaign '{campaign_name}' ({campaign_id}): Spent {spend_pct:.1%} (${spend_today:.2f} / ${budget:.2f})")

        # Thresholds from settings
        crit_thresh_3pm = settings.budget_critical_threshold_3pm
        warn_thresh_3pm = settings.budget_warning_threshold_3pm

        # Critical checkpoint: 3 PM (hour 15)
        if current_hour == 15: # 3 PM
            if spend_pct > crit_thresh_3pm:
                # CRITICAL: Over threshold at 3 PM
                message = f"Over {crit_thresh_3pm:.0%} at 3 PM"
                logger.error(f"🚨 CRITICAL: Campaign '{campaign_name}' {message} ({spend_pct:.1%})")

                self.alerts.append({
                    "severity": "CRITICAL",
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "spend_pct": spend_pct,
                    "spend": spend_today,
                    "budget": budget,
                    "message": message
                })

                # Emergency action: reduce bids significantly
                self._emergency_bid_reduction(campaign_id, campaign_name, reduction=0.25)

            elif spend_pct > warn_thresh_3pm:
                # WARNING: Over threshold at 3 PM
                message = f"Over {warn_thresh_3pm:.0%} at 3 PM"
                logger.warning(f"⚠️ WARNING: Campaign '{campaign_name}' {message} ({spend_pct:.1%})")

                self.alerts.append({
                    "severity": "WARNING",
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "spend_pct": spend_pct,
                    "spend": spend_today,
                    "budget": budget,
                    "message": message
                })

                # Moderate action: reduce bids slightly
                self._emergency_bid_reduction(campaign_id, campaign_name, reduction=0.15)
            else:
                logger.info(f"    Pacing at 3 PM is healthy: {spend_pct:.1%}")
        else:
            logger.info(f"    Current hour {current_hour} is not 3 PM checkpoint.")


        # Check for budget exhaustion at any hour
        if spend_pct >= 0.95:
            message = "Budget nearly exhausted (>=95%)"
            logger.error(f"🚨 CRITICAL: Campaign '{campaign_name}' {message} ({spend_pct:.1%})")

            # Add to alerts if not already added by 3 PM check for critical.
            # This ensures exhaustion alerts can trigger at any time.
            if not any(a["campaign_id"] == campaign_id and a["message"] == message for a in self.alerts):
                self.alerts.append({
                    "severity": "CRITICAL",
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "spend_pct": spend_pct,
                    "spend": spend_today,
                    "budget": budget,
                    "message": message
                })
            # Even if it was already critical at 3 PM, re-evaluate bid reduction if it's now exhausted
            # Or if it's exhausted at a different hour
            if spend_pct >= 0.95 and not any(a["campaign_id"] == campaign_id and a["reduction_pct"] == 0.50 for a in self.emergency_actions):
                self._emergency_bid_reduction(campaign_id, campaign_name, reduction=0.50) # More aggressive reduction for near exhaustion

    def _emergency_bid_reduction(self, campaign_id: str, campaign_name: str, reduction: float):
        """
        Emergency bid reduction to slow spend

        Args:
            campaign_id: Campaign to target
            campaign_name: For logging
            reduction: Percentage to reduce (0.15 = 15% reduction)
        """
        logger.warning(f"🔧 Applying {reduction:.0%} emergency bid reduction to campaign '{campaign_name}' ({campaign_id}).")

        if settings.dry_run:
            logger.info(f"[DRY RUN] Would reduce bids for campaign '{campaign_name}' by {reduction:.0%}.")
            self.emergency_actions.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "reduction_pct": reduction,
                "keywords_updated": 0, # In dry run, no actual update
                "keywords_failed": 0,
                "dry_run": True
            })
            return

        min_bid = settings.min_bid

        # Get keywords for this campaign
        # Use fully qualified table name
        table_path = f"{settings.PROJECT_ID}.{settings.BIGQUERY_DATASET}.keywords"
        query = f"""
        SELECT
            keywordId,
            bid as current_bid
        FROM `{table_path}`
        WHERE campaignId = @campaign_id
          AND state = 'ENABLED'
          AND bid > @min_bid
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                bigquery.ScalarQueryParameter("min_bid", "FLOAT64", min_bid),
            ]
        )

        try:
            if not self.raw_bq_client:
                logger.error(f"Raw BigQuery client not initialized for campaign '{campaign_name}'. Cannot fetch keywords.")
                return

            query_job = self.raw_bq_client.query(query, job_config=job_config)
            keywords = list(query_job.result())

            if not keywords:
                logger.warning(f"No enabled keywords with bid > {min_bid} found for campaign '{campaign_name}' ({campaign_id}). No bids to reduce.")
                return

            # Calculate reduced bids
            bid_updates = []
            for keyword in keywords:
                current_bid = keyword.get("current_bid", 0.0) # Use .get() for safety
                if current_bid == 0.0: continue # Skip if bid is unexpectedly zero

                new_bid = max(
                    min_bid,
                    current_bid * (1 - reduction)
                )
                new_bid = round(new_bid, 2)

                if new_bid < current_bid: # Only update if bid is actually reduced
                    bid_updates.append({
                        "keywordId": keyword.get("keywordId"),
                        "bid": new_bid
                    })

            if not bid_updates:
                logger.info(f"No keywords eligible for bid reduction in campaign '{campaign_name}' (bids already at min or lower).")
                return

            # Apply via Amazon API
            if self.amazon_client:
                logger.info(f"Attempting to update {len(bid_updates)} keyword bids for campaign '{campaign_name}'.")
                results = self.amazon_client.batch_update_keyword_bids(bid_updates)
                success_count = results.get("success", 0)
                failed_count = results.get("failed", 0)
            else:
                logger.error(f"Amazon Ads client not initialized for campaign '{campaign_name}'. Cannot update bids.")
                success_count = 0
                failed_count = len(bid_updates)

            self.emergency_actions.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "reduction_pct": reduction,
                "keywords_updated": success_count,
                "keywords_failed": failed_count,
                "dry_run": False
            })

            logger.info(
                f"✅ Emergency bid reduction applied for campaign '{campaign_name}': "
                f"{success_count} keywords updated, {failed_count} failed."
            )

        except Exception as e:
            logger.error(f"❌ Emergency bid reduction failed for campaign '{campaign_name}' ({campaign_id}): {e}", exc_info=True)

    def _print_summary(self):
        """Print monitoring summary"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 BUDGET MONITOR SUMMARY")
        logger.info("=" * 60)

        if not self.alerts and not self.emergency_actions:
            logger.info("✅ All campaigns within budget targets and no actions taken.")
        else:
            if self.alerts:
                logger.info(f"⚠️ Alerts: {len(self.alerts)} found")

                critical = [a for a in self.alerts if a["severity"] == "CRITICAL"]
                warnings = [a for a in self.alerts if a["severity"] == "WARNING"]

                if critical:
                    logger.info(f"  🚨 Critical Alerts: {len(critical)}")
                    for alert in critical:
                        logger.info(f"    - [CRITICAL] Campaign '{alert['campaign_name']}' ({alert['campaign_id']}): {alert['message']} (Spent {alert['spend_pct']:.1%})")
                if warnings:
                    logger.info(f"  ⚠️ Warning Alerts: {len(warnings)}")
                    for alert in warnings:
                        logger.info(f"    - [WARNING] Campaign '{alert['campaign_name']}' ({alert['campaign_id']}): {alert['message']} (Spent {alert['spend_pct']:.1%})")

            if self.emergency_actions:
                logger.info(f"\n🔧 Emergency Actions Taken: {len(self.emergency_actions)}")
                for action in self.emergency_actions:
                    status = "[DRY RUN]" if action.get("dry_run") else "[APPLIED]"
                    logger.info(
                        f"  {status} Campaign '{action['campaign_name']}' ({action['campaign_id']}): "
                        f"{action['keywords_updated']} bids reduced by {action['reduction_pct']:.0%}"
                        f" ({action['keywords_failed']} failed)"
                    )

        logger.info("=" * 60)


def main():
    """Entry point for Cloud Run Job"""
    monitor = BudgetMonitor()
    monitor.run()

if __name__ == "__main__":
    main()
