"""
Bid calculation rules engine
Implements all the optimization logic
"""

from typing import Optional, Dict
from datetime import datetime
import pytz
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class BidCalculator:
    """
    Calculate optimal bids based on:
    - AOV
    - Performance tier
    - Match type
    - Time of day
    """
    
    def __init__(self, target_acos: float = None):
        self.target_acos = target_acos or settings.default_target_acos
        self.tz = pytz.timezone(settings.timezone)
    
    def calculate_optimal_bid(
        self,
        asin_aov: float,
        current_bid: float,
        conversions: int,
        clicks: int,
        acos: float,
        cvr: float,
        match_type: str,
        keyword_id: str = None,
        current_hour: Optional[int] = None
    ) -> dict:
        """
        Main entry point for bid calculation.
        Automatically classifies the performance tier internally.
        """
        
        # 1. Determine Context
        if current_hour is None:
            current_hour = datetime.now(self.tz).hour
            
        # 2. Classify Performance Tier
        tier = self.classify_performance_tier(conversions, clicks, acos, cvr)
        
        # 3. Get Multipliers
        aov_base_ceiling = self._get_aov_base_ceiling(asin_aov)
        perf_mult = self._get_performance_multiplier(tier)
        match_mult = self._get_match_type_modifier(match_type)
        time_mult = self._get_time_of_day_modifier(current_hour)
        
        # 4. Calculate Logic
        # Formula: Base (AOV) * Performance * Match Type * Time
        optimal_bid = (
            aov_base_ceiling 
            * perf_mult 
            * match_mult 
            * time_mult
        )
        
        # 5. Logic Checks
        # If it's a "Winner" (Tier A), never drop the bid below current (Scale it)
        if tier == "A" and optimal_bid < current_bid:
             optimal_bid = current_bid * 1.10

        # Apply Hard Limits
        optimal_bid = max(settings.min_bid, min(optimal_bid, settings.max_bid))
        optimal_bid = round(optimal_bid, 2)
        
        # Stability Check (only update if > $0.05 change)
        should_update = abs(optimal_bid - current_bid) >= 0.05
        
        reason = self._generate_reason(should_update, tier, time_mult, current_hour)
        
        return {
            "optimal_bid": optimal_bid,
            "reason": reason,
            "tier": tier,
            "should_update": should_update,
            "components": {
                "base": aov_base_ceiling,
                "perf_mult": perf_mult,
                "time_mult": time_mult,
                "match_mult": match_mult
            }
        }

    def calculate_harvest_bid(self, aov: float, acos: float, cvr: float) -> float:
        """
        Calculate initial bid for a newly harvested keyword.
        Used by keyword_harvester.py
        """
        # Start conservative: AOV * TargetACoS * CVR gives "Break Even Bid"
        # We take 85% of break-even to be safe.
        break_even_bid = aov * self.target_acos * cvr
        harvest_bid = break_even_bid * 0.85
        
        # Hard limits
        harvest_bid = max(settings.min_bid, min(harvest_bid, settings.max_bid))
        return round(harvest_bid, 2)

    def _get_aov_base_ceiling(self, aov: float) -> float:
        if aov < 18: return 0.90
        elif aov < 30: return 1.05
        elif aov < 46: return 1.40
        elif aov < 70: return 1.95
        else: return 2.50
    
    def classify_performance_tier(self, conversions: int, clicks: int, acos: float, cvr: float) -> str:
        """
        Tier A: Winners (High CVR, Low ACoS)
        Tier B: Good (Profitable)
        Tier C: Testing (Low data)
        Tier D: Warning (Clicks, no sales)
        Tier E: Bleeders (Many clicks, no sales)
        """
        if conversions >= 2 and cvr >= 0.18 and acos <= 0.25:
            return "A"
        elif conversions >= 1 and 0.10 <= cvr < 0.18 and acos <= 0.40:
            return "B"
        elif clicks >= 30 and conversions == 0:
            return "E"
        elif clicks >= 20 and conversions == 0:
            return "D"
        else:
            return "C" # Default / Testing
    
    def _get_performance_multiplier(self, tier: str) -> float:
        return {
            "A": 1.20, # Boost winners
            "B": 1.00, # Maintain
            "C": 0.75, # Conservative
            "D": 0.40, # Cut aggressively
            "E": 0.15  # Kill
        }.get(tier, 0.75)
    
    def _get_match_type_modifier(self, match_type: str) -> float:
        return {
            "EXACT": 1.00,
            "PHRASE": 0.80,
            "BROAD": 0.60,
            "AUTO": 0.50
        }.get(match_type.upper(), 0.60)
    
    def _get_time_of_day_modifier(self, hour: int) -> float:
        # Eastern Time logic
        if 18 <= hour < 22: return 1.20   # Prime Time (6pm-10pm)
        elif 7 <= hour < 10: return 0.95  # Morning Commute
        elif 0 <= hour < 6: return 0.70   # Overnight
        else: return 1.00                 # Day
    
    def _generate_reason(self, should_update: bool, tier: str, time_mult: float, hour: int) -> str:
        if not should_update: return "hold"
        if time_mult > 1.1: return f"time_boost_h{hour}"
        if time_mult < 0.9: return f"time_cut_h{hour}"
        return f"tier_{tier}_opt"





Evaluate

Compare
