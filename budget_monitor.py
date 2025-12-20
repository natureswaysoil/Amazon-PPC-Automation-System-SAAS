"""
Budget pacing monitor
Runs every 15 minutes to prevent budget burn before evening
"""

import sys
from datetime import datetime
import pytz
from typing import List, Dict

from shared.config import settings
from shared.logger import get_logger
from shared.bigquery_client import BigQueryClient
from shared.amazon_client import AmazonAdsClient

logger = get_logger(__name__)

class BudgetMonitor:
    """
    Monitor campaign budgets and prevent early depletion
    Critical checkpoint: 3 PM must be under 65% spend
    """
    
    def __init__(self):
        self.bq_client = BigQueryClient()
        self.amazon_client = AmazonAdsClient()
        self.tz = pytz.timezone(settings.timezone)
        
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
            # Get campaign budget status
            campaigns = self.bq_client.get_campaign_budget_status()
            
            if not campaigns:
                logger.warning("⚠️ No active campaigns found")
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
        campaign_id = campaign["campaignId"]
        campaign_name = campaign["campaign_name"]
        budget = float(campaign["budget"])
        spend_today = float(campaign["spend_today"])
        
        # Calculate spend percentage
        spend_pct = spend_today / budget if budget > 0 else 0
        
        # Critical checkpoint: 3 PM
        if current_hour == 15:  # 3 PM
            if spend_pct > settings.budget_critical_threshold_3pm:
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
                    "message": f"Over {settings.budget_critical_threshold_3pm:.0%} at 3 PM"
                })
                
                # Emergency action: reduce bids
                self._emergency_bid_reduction(campaign_id, campaign_name, reduction=0.25)
                
            elif spend_pct > settings.budget_warning_threshold_3pm:
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
                    "message": f"Over {settings.budget_warning_threshold_3pm:.0%} at 3 PM"
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
        
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would reduce bids by {reduction:.0%}")
            return
        
        # Get keywords for this campaign
        query = f"""
        SELECT
            keywordId,
            bid as current_bid
        FROM `{settings.project_id}.{settings.dataset_id}.sp_keywords`
        WHERE campaignId = @campaign_id
            AND state = 'ENABLED'
            AND bid > @min_bid
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                bigquery.ScalarQueryParameter("min_bid", "FLOAT64", settings.min_bid),
            ]
        )
        
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=settings.project_id)
            
            keywords = list(client.query(query, job_config=job_config).result())
            
            if not keywords:
                logger.warning(f"No keywords found for campaign {campaign_name}")
                return
            
            # Calculate reduced bids
            bid_updates = []
            for keyword in keywords:
                new_bid = max(
                    settings.min_bid,
                    keyword["current_bid"] * (1 - reduction)
                )
                new_bid = round(new_bid, 2)
                
                bid_updates.append({
                    "keywordId": keyword["keywordId"],
                    "bid": new_bid
                })
            
            # Apply via Amazon API
            results = self.amazon_client.batch_update_keyword_bids(bid_updates)
            
            self.emergency_actions.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "reduction_pct": reduction,
                "keywords_updated": results["success"],
                "keywords_failed": results["failed"]
            })
            
            logger.info(
                f"✅ Emergency bid reduction applied: "
                f"{results['success']} keywords updated"
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
                logger.info(f"   🚨 Critical: {len(critical)}")
            if warnings:
                logger.info(f"   ⚠️ Warnings: {len(warnings)}")
        
        if self.emergency_actions:
            logger.info(f"\n🔧 Emergency Actions: {len(self.emergency_actions)}")
            for action in self.emergency_actions:
                logger.info(
                    f"   - {action['campaign_name']}: "
                    f"{action['keywords_updated']} bids reduced by {action['reduction_pct']:.0%}"
                )
        
        logger.info("=" * 60)


def main():
    """Entry point for Cloud Run Job"""
    monitor = BudgetMonitor()
    monitor.run()


if __name__ == "__main__":
    main()
