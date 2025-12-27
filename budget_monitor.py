"""
Budget pacing monitor
Runs every 15 minutes to prevent budget burn before evening
"""

import sys
import os
from datetime import datetime
import pytz
import logging
from typing import List, Dict
from google.cloud import bigquery # FIX: Import at top level

# Add project root to path
sys.path.insert(0, '/app')

try:
    from backend.core.config import settings
    # Mock classes for import safety if running standalone
    from backend.shared.bigquery_client import BigQueryClient
    from backend.shared.amazon_client import AmazonAdsClient
except ImportError:
    # Fallback/Mock for syntax checking
    settings = type('obj', (object,), {
        'timezone': 'America/Los_Angeles', 
        'dry_run': True, 
        'PROJECT_ID': 'amazon-ppc-474902',
        'BIGQUERY_DATASET': 'amazon_ppc',
        'budget_critical_threshold_3pm': 0.75,
        'budget_warning_threshold_3pm': 0.65,
        'min_bid': 0.20
    })
    BigQueryClient = object
    AmazonAdsClient = object

# Setup Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BudgetMonitor:
    """
    Monitor campaign budgets and prevent early depletion
    Critical checkpoint: 3 PM must be under 65% spend
    """
    
    def __init__(self):
        # Initialize clients safely
        try:
            self.bq_client = BigQueryClient()
            self.amazon_client = AmazonAdsClient()
            self.raw_bq_client = bigquery.Client(project=getattr(settings, 'PROJECT_ID', 'amazon-ppc-474902'))
        except Exception as e:
            logger.warning(f"Clients failed to init: {e}")
            self.bq_client = None
            self.amazon_client = None
            self.raw_bq_client = None

        tz_name = getattr(settings, 'timezone', 'America/Los_Angeles')
        self.tz = pytz.timezone(tz_name)
        
        self.alerts = []
        self.emergency_actions = []
    
    def run(self):
        """Main monitoring workflow"""
        current_time = datetime.now(self.tz)
        current_hour = current_time.hour
        
        logger.info("=" * 60)
        logger.info("⏰ Budget Monitor Job")
        logger.info(f"Timestamp: {current_time.isoformat()}")
        logger.info(f"Current Hour: {current_hour}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Get campaign budget status
            if self.bq_client:
                campaigns = self.bq_client.get_campaign_budget_status()
            else:
                logger.warning("No BQ Client available.")
                campaigns = []
            
            if not campaigns:
                logger.warning("⚠️ No active campaigns found (or client failed)")
                return
            
            logger.info(f"\n📊 Monitoring {len(campaigns)} campaigns")
            
            for campaign in campaigns:
                self._check_campaign_budget(campaign, current_hour, current_time)
            
            # Summary
            self._print_summary()
            
            logger.info("\n✅ Budget Monitor Completed")
            
        except Exception as e:
            logger.error(f"❌ Budget monitor failed: {e}", exc_info=True)
            sys.exit(1)
    
    def _check_campaign_budget(self, campaign: Dict, current_hour: int, current_time: datetime):
        """Check individual campaign budget pacing"""
        campaign_id = campaign.get("campaignId")
        campaign_name = campaign.get("campaign_name", "Unknown")
        budget = float(campaign.get("budget", 0))
        spend_today = float(campaign.get("spend_today", 0))
        
        # Calculate spend percentage
        spend_pct = spend_today / budget if budget > 0 else 0
        
        # Thresholds
        crit_thresh = getattr(settings, 'budget_critical_threshold_3pm', 0.75)
        warn_thresh = getattr(settings, 'budget_warning_threshold_3pm', 0.65)
        
        # Critical checkpoint: 3 PM
        if current_hour == 15: # 3 PM
            if spend_pct > crit_thresh:
                # CRITICAL: Over 75% at 3 PM
                logger.error(
                    f"🚨 CRITICAL: {campaign_name} at {spend_pct:.1%} "
                    f"(${spend_today:.2f} / ${budget:.2f})"
                )
                
                self.alerts.append({
                    "severity": "CRITICAL",
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "spend_pct": spend_pct,
                    "spend": spend_today,
                    "budget": budget,
                    "message": f"Over {crit_thresh:.0%} at 3 PM"
                })
                
                # Emergency action: reduce bids
                self._emergency_bid_reduction(campaign_id, campaign_name, reduction=0.25)
                
            elif spend_pct > warn_thresh:
                # WARNING: Over 65% at 3 PM
                logger.warning(
                    f"⚠️ WARNING: {campaign_name} at {spend_pct:.1%} "
                    f"(${spend_today:.2f} / ${budget:.2f})"
                )
                
                self.alerts.append({
                    "severity": "WARNING",
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "spend_pct": spend_pct,
                    "spend": spend_today,
                    "budget": budget,
                    "message": f"Over {warn_thresh:.0%} at 3 PM"
                })
                
                # Moderate action: reduce bids slightly
                self._emergency_bid_reduction(campaign_id, campaign_name, reduction=0.15)
            else:
                # Good pacing
                logger.info(f"✅ {campaign_name}: {spend_pct:.1%} spent (healthy)")
        
        # Check for budget exhaustion at any hour
        if spend_pct >= 0.95:
            logger.error(
                f"🚨 {campaign_name} budget nearly exhausted: {spend_pct:.1%}"
            )
            
            self.alerts.append({
                "severity": "CRITICAL",
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "spend_pct": spend_pct,
                "message": "Budget nearly exhausted"
            })
    
    def _emergency_bid_reduction(self, campaign_id: str, campaign_name: str, reduction: float):
        """
        Emergency bid reduction to slow spend
        
        Args:
            campaign_id: Campaign to target
            campaign_name: For logging
            reduction: Percentage to reduce (0.15 = 15% reduction)
        """
        logger.warning(f"🔧 Applying {reduction:.0%} emergency bid reduction to {campaign_name}")
        
        if getattr(settings, 'dry_run', True):
            logger.info(f"[DRY RUN] Would reduce bids by {reduction:.0%}")
            return
        
        # Safe access to config vars
        project_id = getattr(settings, 'PROJECT_ID', 'amazon-ppc-474902')
        dataset_id = getattr(settings, 'BIGQUERY_DATASET', 'amazon_ppc')
        min_bid = getattr(settings, 'min_bid', 0.20)

        # Get keywords for this campaign
        query = f"""
        SELECT
            keywordId,
            bid as current_bid
        FROM `{project_id}.{dataset_id}.keywords`
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
                 logger.error("Raw BigQuery client missing.")
                 return

            keywords = list(self.raw_bq_client.query(query, job_config=job_config).result())
            
            if not keywords:
                logger.warning(f"No keywords found for campaign {campaign_name}")
                return
            
            # Calculate reduced bids
            bid_updates = []
            for keyword in keywords:
                # Access row items safely
                current_bid = keyword.get("current_bid") or 0.0
                new_bid = max(
                    min_bid,
                    current_bid * (1 - reduction)
                )
                new_bid = round(new_bid, 2)
                
                bid_updates.append({
                    "keywordId": keyword.get("keywordId"),
                    "bid": new_bid
                })
            
            # Apply via Amazon API
            if self.amazon_client:
                results = self.amazon_client.batch_update_keyword_bids(bid_updates)
                success_count = results.get("success", 0)
                failed_count = results.get("failed", 0)
            else:
                logger.error("Amazon Client missing.")
                success_count = 0
                failed_count = len(bid_updates)
            
            self.emergency_actions.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "reduction_pct": reduction,
                "keywords_updated": success_count,
                "keywords_failed": failed_count
            })
            
            logger.info(
                f"✅ Emergency bid reduction applied: "
                f"{success_count} keywords updated"
            )
            
        except Exception as e:
            logger.error(f"❌ Emergency bid reduction failed: {e}")
    
    def _print_summary(self):
        """Print monitoring summary"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 BUDGET MONITOR SUMMARY")
        logger.info("=" * 60)
        
        if not self.alerts:
            logger.info("✅ All campaigns within budget targets")
        else:
            logger.info(f"⚠️ Alerts: {len(self.alerts)}")
            
            critical = [a for a in self.alerts if a["severity"] == "CRITICAL"]
            warnings = [a for a in self.alerts if a["severity"] == "WARNING"]
            
            if critical:
                logger.info(f"  🚨 Critical: {len(critical)}")
            if warnings:
                logger.info(f"  ⚠️ Warnings: {len(warnings)}")
        
        if self.emergency_actions:
            logger.info(f"\n🔧 Emergency Actions: {len(self.emergency_actions)}")
            for action in self.emergency_actions:
                logger.info(
                    f"  - {action['campaign_name']}: "
                    f"{action['keywords_updated']} bids reduced by {action['reduction_pct']:.0%}"
                )
        
        logger.info("=" * 60)


def main():
    """Entry point for Cloud Run Job"""
    monitor = BudgetMonitor()
    monitor.run()
