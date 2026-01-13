"""
Main bid optimization job
Runs hourly via Cloud Scheduler
"""

import sys
import os
from datetime import datetime
import pytz
import logging
from typing import List, Dict, Optional

# Add project root to path
sys.path.insert(0, '/app')

# --- IMPORTS ---
try:
    # Use automation/shared re-exports and local aov_fetcher
    from automation.shared.config import settings
    from shared.bigquery_client import BigQueryClient
    from automation.shared.amazon_client import AmazonAdsClient
    from aov_fetcher import aov_fetcher
except ImportError as e:
    logging.warning(f"Import warning: {e}. Ensure PYTHONPATH is set correctly.")
    # Fallbacks for syntax checking
    settings = type('obj', (object,), {'timezone': 'America/Los_Angeles', 'dry_run': True, 'default_aov': 35.0})
    BigQueryClient = object
    AmazonAdsClient = object

# Setup Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- BID CALCULATOR LOGIC (Moved from bottom of file) ---
class BidCalculator:
    """Core logic for determining bid prices"""
    
    def __init__(self):
        # AOV-based bid ceiling definitions
        self.AOV_CEILINGS = {
            "L": {"base": 1.05, "max": 1.15}, # Low Ticket
            "M": {"base": 1.40, "max": 1.60}, # Mid Ticket
            "H": {"base": 1.95, "max": 2.20}, # High Ticket
            "X": {"base": 2.50, "max": 2.75}, # Luxury / Extra High
        }

    def classify_performance_tier(self, conversions: int, clicks: int, acos: float, cvr: float) -> str:
        """Classify keyword performance"""
        if conversions >= 5 and acos < 0.30: return "A"
        if conversions >= 2 and acos < 0.40: return "B"
        if conversions >= 1: return "C"
        if clicks > 15 and conversions == 0: return "D" # Bleeder
        return "C" # Default

    def calculate_bid_ceiling(self, asin: str, performance_tier: str, match_type: str) -> float:
        """
        Calculate dynamic bid ceiling based on:
        - ASIN AOV (real-time from aov_fetcher)
        - Keyword performance tier
        - Match type
        """
        # Get AOV tier dynamically from fetcher
        aov_tier = aov_fetcher.get_aov_tier(asin)
        aov_data = aov_fetcher.get_aov(asin)
        
        # Base ceiling from AOV configuration
        # Fallback to 'L' if tier not found
        ceiling_config = self.AOV_CEILINGS.get(aov_tier, self.AOV_CEILINGS["L"])
        base_ceiling = ceiling_config["base"]
        
        # Performance tier modifier
        tier_modifiers = {
            "A": 1.20, # Boost winners
            "B": 1.00, # Base
            "C": 0.80, # Conservative
            "D": 0.40, # Aggressive Cut
        }
        
        # Match type modifier
        match_modifiers = {
            "EXACT": 1.00,
            "PHRASE": 0.75,
            "BROAD": 0.50,
        }
        
        # Normalize match type string
        match_type_key = match_type.upper() if match_type else "BROAD"
        
        ceiling = (
            base_ceiling
            * tier_modifiers.get(performance_tier, 0.65)
            * match_modifiers.get(match_type_key, 0.50)
        )
        
        # Apply confidence penalty for default AOV
        if aov_data.confidence == "default":
            ceiling *= 0.85
            
        return round(ceiling, 2)

    def calculate_optimal_bid(self, keyword_data: dict, current_hour: int) -> dict:
        """
        Main calculation wrapper
        """
        asin = keyword_data.get('advertisedAsin', '')
        current_bid = float(keyword_data.get('current_bid', 0.50))
        
        perf_tier = self.classify_performance_tier(
            keyword_data.get('conversions', 0),
            keyword_data.get('clicks', 0),
            keyword_data.get('acos', 0.0),
            keyword_data.get('cvr', 0.0)
        )
        
        ceiling = self.calculate_bid_ceiling(
            asin, 
            perf_tier, 
            keyword_data.get('matchType', 'BROAD')
        )
        
        # -- Simplified Logic for updates --
        # (Real logic should match the detailed AOVBidOptimizer from previous file)
        # Here we just ensure we don't exceed the calculated ceiling
        
        if current_bid > ceiling:
            new_bid = ceiling
            reason = "Bid exceeded AOV ceiling"
        elif perf_tier == "A" and current_bid < ceiling:
            new_bid = min(current_bid * 1.1, ceiling)
            reason = "Scale winner"
        elif perf_tier == "D":
            new_bid = min(current_bid * 0.75, ceiling)
            reason = "Cut bleeder"
        else:
            new_bid = current_bid
            reason = "Hold"

        # Stability check
        should_update = abs(new_bid - current_bid) > 0.02

        return {
            "optimal_bid": round(new_bid, 2),
            "should_update": should_update,
            "reason": reason,
            "components": {"ceiling": ceiling, "tier": perf_tier}
        }

# --- MAIN OPTIMIZER CLASS ---
class BidOptimizer:
    def __init__(self):
        # Initialize clients
        # Note: Using mocked objects if imports failed above
        try:
            self.bq_client = BigQueryClient()
            self.amazon_client = AmazonAdsClient()
        except:
            logger.warning("Clients not initialized (Import/Config error)")
            self.bq_client = None
            self.amazon_client = None
            
        self.bid_calculator = BidCalculator()
        
        # Timezone setup
        tz_name = getattr(settings, 'timezone', 'America/Los_Angeles')
        self.tz = pytz.timezone(tz_name)
        
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
        logger.info(f"Dry Run: {getattr(settings, 'dry_run', True)}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Pre-load AOV data into memory
            logger.info("\n💰 Step 1: Loading AOV Data")
            aov_fetcher.fetch_all() # <--- CORRECTED: Using the fetcher we built
            
            # Step 2: Get keywords (Mocking the BQ call for structure)
            logger.info("\n🔍 Step 2: Loading Keywords")
            if self.bq_client:
                keywords = self.bq_client.get_keywords_for_optimization(min_clicks=5, days_lookback=14)
            else:
                keywords = [] # Empty if no client
                logger.warning("No BigQuery client available.")

            logger.info(f"Found {len(keywords)} keywords to evaluate")
            
            if not keywords:
                logger.warning("⚠️ No keywords to optimize")
                return
            
            # Step 3: Calculate optimal bids
            logger.info("\n🧮 Step 3: Calculating Optimal Bids")
            current_hour = datetime.now(self.tz).hour
            
            bid_updates = []
            
            for keyword in keywords:
                self.stats["keywords_evaluated"] += 1
                
                # Logic calculation
                result = self.bid_calculator.calculate_optimal_bid(
                    keyword_data=keyword,
                    current_hour=current_hour
                )
                
                if result["should_update"]:
                    current_bid = keyword.get("current_bid", 0.0)
                    optimal_bid = result["optimal_bid"]
                    bid_change = optimal_bid - current_bid
                    
                    # CRITICAL: Convert keywordId to string (Amazon API requirement)
                    bid_updates.append({
                        "keywordId": str(keyword["keywordId"]),
                        "bid": optimal_bid
                    })
                    
                    # Log to BQ
                    if self.bq_client:
                        self.bq_client.log_bid_change(
                            keyword_id=str(keyword["keywordId"]),
                            old_bid=current_bid,
                            new_bid=optimal_bid,
                            reason=result["reason"]
                        )
                    
                    self.stats["bids_updated"] += 1
                    if bid_change > 0:
                        self.stats["total_bid_increase"] += bid_change
                    else:
                        self.stats["total_bid_decrease"] += abs(bid_change)
                    
                    logger.info(
                        f"📈 {keyword.get('keywordText', 'Unknown')}: "
                        f"${current_bid:.2f} → ${optimal_bid:.2f} "
                        f"({result['reason']})"
                    )
                else:
                    self.stats["bids_unchanged"] += 1
            
            # Step 4: Apply updates via Amazon API
            if bid_updates and not getattr(settings, 'dry_run', True):
                logger.info(f"\n🔄 Step 4: Applying {len(bid_updates)} Bid Updates")
                if self.amazon_client:
                    update_results = self.amazon_client.batch_update_keyword_bids(bid_updates)
                    logger.info(f"✅ Successfully updated: {update_results.get('success', 0)}")
                else:
                    logger.error("Amazon Client missing, cannot push updates.")
            elif getattr(settings, 'dry_run', True):
                 logger.info(f"\n✋ DRY RUN ENABLED: Skipping Amazon API update for {len(bid_updates)} bids.")
            else:
                logger.info("\n✅ No bid updates needed")
            
            # Step 5: Summary
            self._print_summary()
            
        except Exception as e:
            logger.error(f"❌ Bid optimization job failed: {e}", exc_info=True)
            self.stats["errors"] += 1
            sys.exit(1)
    
    def _print_summary(self):
        """Print job summary statistics"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 JOB SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Keywords Evaluated:   {self.stats['keywords_evaluated']}")
        logger.info(f"Bids Updated:         {self.stats['bids_updated']}")
        logger.info(f"Bids Unchanged:       {self.stats['bids_unchanged']}")
        logger.info(f"Errors:               {self.stats['errors']}")
        logger.info(f"Total Bid Increase:   ${self.stats['total_bid_increase']:.2f}")
        logger.info(f"Total Bid Decrease:   ${self.stats['total_bid_decrease']:.2f}")
        net = self.stats['total_bid_increase'] - self.stats['total_bid_decrease']
        logger.info(f"Net Change:           ${net:.2f}")
        logger.info("=" * 60)


def main():
    """Entry point for Cloud Run Job"""
    optimizer = BidOptimizer()
    optimizer.run()

# Ensure this block is AT THE END
if __name__ == "__main__":
    main()
