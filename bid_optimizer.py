"""
Main bid optimization job
Runs hourly via Cloud Scheduler
"""

import sys
from datetime import datetime
import pytz
from typing import List, Dict

from shared.config import settings
from shared.logger import get_logger
from shared.bigquery_client import BigQueryClient
from shared.amazon_client import AmazonAdsClient
from shared.rules_engine import BidCalculator
from data_verification import verify_data_or_exit

logger = get_logger(__name__)

class BidOptimizer:
    def __init__(self):
        self.bq_client = BigQueryClient()
        self.amazon_client = AmazonAdsClient()
        self.bid_calculator = BidCalculator()
        self.tz = pytz.timezone(settings.timezone)
        
        self.stats = {
            "keywords_evaluated": 0,
            "bids_updated": 0,
            "bids_unchanged": 0,
            "errors": 0,
            "total_bid_increase": 0.0,
            "total_bid_decrease": 0.0
        }
    
    def run(self):
        """Main optimization workflow"""
        logger.info("=" * 60)
        logger.info("🚀 Starting Bid Optimization Job")
        logger.info(f"Timestamp: {datetime.now(self.tz).isoformat()}")
        logger.info(f"Dry Run: {settings.dry_run}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Verify data
            logger.info("\n📊 Step 1: Data Verification")
            verification_results = verify_data_or_exit()
            logger.info(f"Verification status: {verification_results['status']}")
            
            # Step 2: Load AOV data
            logger.info("\n💰 Step 2: Loading AOV Data")
            aov_map_14d = self.bq_client.get_asin_aov_map(days=14, min_orders=2)
            aov_map_30d = self.bq_client.get_asin_aov_map(days=30, min_orders=2)
            logger.info(f"Loaded AOV for {len(aov_map_14d)} ASINs (14d), {len(aov_map_30d)} ASINs (30d)")
            
            # Step 3: Get keywords needing optimization
            logger.info("\n🔍 Step 3: Loading Keywords")
            keywords = self.bq_client.get_keywords_for_optimization(
                min_clicks=5,
                days_lookback=14
            )
            logger.info(f"Found {len(keywords)} keywords to evaluate")
            
            if not keywords:
                logger.warning("⚠️ No keywords to optimize")
                return
            
            # Step 4: Calculate optimal bids
            logger.info("\n🧮 Step 4: Calculating Optimal Bids")
            current_hour = datetime.now(self.tz).hour
            logger.info(f"Current hour: {current_hour}")
            
            bid_updates = []
            
            for keyword in keywords:
                self.stats["keywords_evaluated"] += 1
                
                # Get ASIN AOV
                asin = keyword.get("advertisedAsin")
                aov = aov_map_14d.get(asin) or aov_map_30d.get(asin) or settings.default_aov
                
                # Classify performance tier
                tier = self.bid_calculator.classify_performance_tier(
                    conversions=keyword["conversions"],
                    clicks=keyword["clicks"],
                    acos=keyword.get("acos", 0) or 0,
                    cvr=keyword.get("cvr", 0) or 0
                )
                
                # Calculate optimal bid
                result = self.bid_calculator.calculate_optimal_bid(
                    keyword_id=keyword["keywordId"],
                    asin_aov=aov,
                    performance_tier=tier,
                    match_type=keyword["matchType"],
                    current_bid=keyword["current_bid"],
                    conversions=keyword["conversions"],
                    clicks=keyword["clicks"],
                    acos=keyword.get("acos", 0) or 0,
                    cvr=keyword.get("cvr", 0) or 0,
                    current_hour=current_hour
                )
                
                if result["should_update"]:
                    bid_change = result["optimal_bid"] - keyword["current_bid"]
                    
                    bid_updates.append({
                        "keywordId": keyword["keywordId"],
                        "bid": result["optimal_bid"]
                    })
                    
                    # Log to BigQuery
                    self.bq_client.log_bid_change(
                        keyword_id=keyword["keywordId"],
                        old_bid=keyword["current_bid"],
                        new_bid=result["optimal_bid"],
                        reason=result["reason"],
                        changed_by="system",
                        components=result["components"]
                    )
                    
                    self.stats["bids_updated"] += 1
                    if bid_change > 0:
                        self.stats["total_bid_increase"] += bid_change
                    else:
                        self.stats["total_bid_decrease"] += abs(bid_change)
                    
                    logger.info(
                        f"📈 {keyword['keywordText']}: "
                        f"${keyword['current_bid']:.2f} → ${result['optimal_bid']:.2f} "
                        f"({result['reason']})"
                    )
                else:
                    self.stats["bids_unchanged"] += 1
            
            # Step 5: Apply updates via Amazon API
            if bid_updates:
                logger.info(f"\n🔄 Step 5: Applying {len(bid_updates)} Bid Updates")
                
                update_results = self.amazon_client.batch_update_keyword_bids(bid_updates)
                
                logger.info(f"✅ Successfully updated: {update_results['success']}")
                if update_results['failed'] > 0:
                    logger.error(f"❌ Failed updates: {update_results['failed']}")
                    self.stats["errors"] = update_results['failed']
            else:
                logger.info("\n✅ No bid updates needed")
            
            # Step 6: Summary
            self._print_summary()
            
            logger.info("\n✅ Bid Optimization Job Completed Successfully")
            
        except Exception as e:
            logger.error(f"❌ Bid optimization job failed: {e}", exc_info=True)
            self.stats["errors"] += 1
            sys.exit(1)
    
    def _print_summary(self):
        """Print job summary statistics"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 JOB SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Keywords Evaluated:  {self.stats['keywords_evaluated']}")
        logger.info(f"Bids Updated:        {self.stats['bids_updated']}")
        logger.info(f"Bids Unchanged:      {self.stats['bids_unchanged']}")
        logger.info(f"Errors:              {self.stats['errors']}")
        logger.info(f"Total Bid Increase:  ${self.stats['total_bid_increase']:.2f}")
        logger.info(f"Total Bid Decrease:  ${self.stats['total_bid_decrease']:.2f}")
        logger.info(f"Net Change:          ${self.stats['total_bid_increase'] - self.stats['total_bid_decrease']:.2f}")
        logger.info("=" * 60)


def main():
    """Entry point for Cloud Run Job"""
    optimizer = BidOptimizer()
    optimizer.run()


if __name__ == "__main__":
    main()
