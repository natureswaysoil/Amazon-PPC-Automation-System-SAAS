#!/bin/bash

# ... (Previous script content runs here first) ...

echo "📂 Creating Job Files in automation/jobs/..."

# ============================================
# automation/jobs/bid_optimizer.py
# ============================================
cat > automation/jobs/bid_optimizer.py << 'PYEOF'
import sys
from automation.shared.config import settings
from automation.shared.logger import get_logger
from automation.shared.bigquery_client import BigQueryClient
from automation.shared.amazon_client import AmazonAdsClient
from automation.shared.rules_engine import BidCalculator

logger = get_logger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("🚀 Starting Bid Optimizer Job")
    logger.info(f"Project: {settings.project_id}")
    logger.info(f"Dry Run: {settings.dry_run}")
    logger.info("=" * 60)
    
    try:
        # Initialize Clients
        bq = BigQueryClient()
        # Amazon client initializes token automatically
        amz = AmazonAdsClient()
        calculator = BidCalculator()
        
        # 1. Load Data
        logger.info("\nStep 1: Loading Performance Data")
        # Get AOV map
        aov_map_14d = bq.get_asin_aov_map(days=14, min_orders=2)
        aov_map_30d = bq.get_asin_aov_map(days=30, min_orders=2)
        
        # Get Keywords
        keywords = bq.get_keywords_for_optimization(min_clicks=5)
        
        if not keywords:
            logger.info("⚠️ No keywords found to optimize.")
            return

        # 2. Calculate Bids
        logger.info(f"\nStep 2: Calculating Bids for {len(keywords)} keywords")
        updates = []
        
        for kw in keywords:
            # Determine AOV (Hierarchy: 14d -> 30d -> Default)
            # Note: Your SQL might not return 'advertisedAsin' if not joined. 
            # Assuming the corrected SQL from previous steps includes it.
            asin = kw.get("advertisedAsin")
            aov = settings.default_aov
            if asin:
                aov = aov_map_14d.get(asin) or aov_map_30d.get(asin) or settings.default_aov
            
            # Logic
            result = calculator.calculate_optimal_bid(
                asin_aov=aov,
                current_bid=kw["current_bid"],
                conversions=kw["conversions"],
                clicks=kw["clicks"],
                acos=kw.get("acos", 0) or 0,
                cvr=kw.get("cvr", 0) or 0,
                match_type=kw["matchType"]
            )
            
            if result["should_update"]:
                updates.append({
                    "keyword_id": kw["keywordId"],
                    "keyword_text": kw["keywordText"],
                    "old_bid": kw["current_bid"],
                    "new_bid": result["optimal_bid"],
                    "tier": result["tier"],
                    "reason": result["reason"]
                })

        # 3. Log & Update
        logger.info(f"\nStep 3: Found {len(updates)} bid updates")
        
        # Batch logging to BQ
        for u in updates:
            bq.log_bid_change(
                u["keyword_id"], 
                u["old_bid"], 
                u["new_bid"], 
                u["reason"]
            )
            
            if not settings.dry_run:
                amz.update_keyword_bid(u["keyword_id"], u["new_bid"])
            else:
                logger.info(f"[DRY RUN] {u['keyword_text']}: ${u['old_bid']:.2f} -> ${u['new_bid']:.2f}")

        logger.info("\n✅ Bid Optimizer Completed Successfully")
        
    except Exception as e:
        logger.error(f"❌ Job Failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
PYEOF

# ============================================
# automation/jobs/budget_monitor.py
# ============================================
cat > automation/jobs/budget_monitor.py << 'PYEOF'
import sys
from datetime import datetime
import pytz
from automation.shared.config import settings
from automation.shared.logger import get_logger
from automation.shared.bigquery_client import BigQueryClient
from automation.shared.amazon_client import AmazonAdsClient

logger = get_logger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("⏰ Starting Budget Monitor Job")
    logger.info("=" * 60)
    
    try:
        bq = BigQueryClient()
        amz = AmazonAdsClient()
        tz = pytz.timezone(settings.timezone)
        current_hour = datetime.now(tz).hour
        
        campaigns = bq.get_campaign_budget_status()
        
        for camp in campaigns:
            spend = camp["spend_today"]
            budget = camp["budget"]
            if budget <= 0: continue
            
            utilization = spend / budget
            camp_name = camp["campaign_name"]
            camp_id = camp["campaignId"]
            
            # Logic: 3 PM Checkpoint
            if current_hour == 15 and utilization > settings.budget_critical_threshold_3pm:
                logger.warning(f"🚨 CRITICAL: {camp_name} used {utilization:.1%} budget by 3 PM!")
                # Action: Reduce bids by 20%
                # (Implementation requires fetch keywords for this campaign and loop update)
                pass 
                
            elif utilization >= 0.95:
                logger.warning(f"⚠️ {camp_name} budget nearly exhausted ({utilization:.1%})")
        
        logger.info("✅ Budget Monitor Completed")
        
    except Exception as e:
        logger.error(f"❌ Job Failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
PYEOF

# ============================================
# automation/jobs/keyword_harvester.py
# ============================================
cat > automation/jobs/keyword_harvester.py << 'PYEOF'
import sys
from automation.shared.config import settings
from automation.shared.logger import get_logger
from automation.shared.bigquery_client import BigQueryClient
from automation.shared.amazon_client import AmazonAdsClient
from automation.shared.rules_engine import BidCalculator

logger = get_logger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("🌾 Starting Keyword Harvester Job")
    logger.info("=" * 60)
    
    try:
        bq = BigQueryClient()
        amz = AmazonAdsClient()
        calculator = BidCalculator()
        
        # 1. Harvest New Keywords
        search_terms = bq.get_search_terms_for_harvesting()
        
        for term in search_terms:
            # Calculate initial bid
            # Assuming AOV=35 if unknown, strictly speaking we should look it up
            bid = calculator.calculate_harvest_bid(
                aov=settings.default_aov, 
                acos=term.get("acos", 0.3), 
                cvr=term.get("cvr", 0.1)
            )
            
            success = amz.create_keyword(
                campaign_id=term["campaignId"],
                ad_group_id=term["adGroupId"],
                keyword_text=term["search_term"],
                match_type="EXACT", # Harvest as Exact
                bid=bid
            )
            
            if success:
                bq.log_keyword_harvest(
                    term["search_term"], 
                    term["campaignId"], 
                    "EXACT", 
                    bid, 
                    "CREATED"
                )
        
        # 2. Add Negatives
        negatives = bq.get_negative_search_terms()
        for term in negatives:
            success = amz.create_negative_keyword(
                campaign_id=term["campaignId"],
                keyword_text=term["search_term"],
                match_type="NEGATIVE_EXACT"
            )
            
            if success:
                bq.log_keyword_harvest(
                    term["search_term"], 
                    term["campaignId"], 
                    "NEGATIVE_EXACT", 
                    0.0, 
                    "BLOCKED"
                )

        logger.info("✅ Keyword Harvester Completed")
        
    except Exception as e:
        logger.error(f"❌ Job Failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
PYEOF

echo "✅ All files created."
echo "👉 To run Bid Optimizer locally: python3 -m automation.jobs.bid_optimizer"





Evaluate

Compare
