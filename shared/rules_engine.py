"""
Bid calculation rules engine
Implements all the optimization logic we defined
"""

from typing import Optional
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
    - User overrides
    """
    
    def __init__(self, target_acos: float = None):
        self.target_acos = target_acos or settings.default_target_acos
        self.tz = pytz.timezone(settings.timezone)
    
    def calculate_optimal_bid(
        self,
        keyword_id: str,
        asin_aov: float,
        performance_tier: str,
        match_type: str,
        current_bid: float,
        conversions: int = 0,
        clicks: int = 0,
        acos: float = 0,
        cvr: float = 0,
        user_override: Optional[float] = None,
        override_expires_at: Optional[datetime] = None,
        current_hour: Optional[int] = None
    ) -> dict:
        """
        Calculate optimal bid for a keyword
        
        Returns:
            {
                "optimal_bid": float,
                "reason": str,
                "components": dict,
                "should_update": bool
            }
        """
        
        # Check for active user override
        if user_override is not None and override_expires_at:
            if datetime.now(self.tz) < override_expires_at:
                return {
                    "optimal_bid": user_override,
                    "reason": "user_override_active",
                    "components": {"user_override": user_override},
                    "should_update": False  # Don't auto-update overridden bids
                }
        
        # Get current hour if not provided
        if current_hour is None:
            current_hour = datetime.now(self.tz).hour
        
        # Step 1: Get base ceiling from AOV
        aov_base_ceiling = self._get_aov_base_ceiling(asin_aov)
        
        # Step 2: Apply performance tier multiplier
        performance_multiplier = self._get_performance_multiplier(
            performance_tier, conversions, clicks, acos, cvr
        )
        
        # Step 3: Apply match type modifier
        match_modifier = self._get_match_type_modifier(match_type)
        
        # Step 4: Apply time-of-day modifier
        time_modifier = self._get_time_of_day_modifier(current_hour)
        
        # Calculate optimal bid
        optimal_bid = (
            aov_base_ceiling 
            * performance_multiplier 
            * match_modifier 
            * time_modifier
        )
        
        # Apply min/max constraints
        optimal_bid = max(settings.min_bid, min(optimal_bid, settings.max_bid))
        
        # Round to 2 decimals
        optimal_bid = round(optimal_bid, 2)
        
        # Determine if update is needed (>$0.05 difference)
        should_update = abs(optimal_bid - current_bid) >= 0.05
        
        components = {
            "aov_base_ceiling": round(aov_base_ceiling, 2),
            "performance_multiplier": round(performance_multiplier, 3),
            "match_modifier": round(match_modifier, 2),
            "time_modifier": round(time_modifier, 2),
            "performance_tier": performance_tier,
            "current_hour": current_hour
        }
        
        reason = self._generate_reason(
            should_update, performance_tier, time_modifier, current_hour
        )
        
        return {
            "optimal_bid": optimal_bid,
            "reason": reason,
            "components": components,
            "should_update": should_update
        }
    
    def _get_aov_base_ceiling(self, aov: float) -> float:
        """
        Get base bid ceiling based on AOV tier
        
        AOV Tier L ($18-29):  $0.90-$1.15  → use $1.05
        AOV Tier M ($30-45):  $1.25-$1.60  → use $1.40
        AOV Tier H ($46-70):  $1.70-$2.20  → use $1.95
        AOV Tier X ($70+):    $2.25-$2.75  → use $2.50
        """
        if aov < 18:
            return 0.90
        elif aov < 30:
            return 1.05
        elif aov < 46:
            return 1.40
        elif aov < 70:
            return 1.95
        else:
            return 2.50
    
    def _get_performance_multiplier(
        self,
        tier: str,
        conversions: int,
        clicks: int,
        acos: float,
        cvr: float
    ) -> float:
        """
        Performance tier multipliers
        
        Tier A (Winners):     1.00×
        Tier B (Solid):       0.85×
        Tier C (Testing):     0.65×
        Tier D (Bleeding):    0.40×
        Tier E (Kill):        0.15×
        """
        multipliers = {
            "A": 1.00,
            "B": 0.85,
            "C": 0.65,
            "D": 0.40,
            "E": 0.15
        }
        return multipliers.get(tier, 0.65)  # Default to C if unknown
    
    def _get_match_type_modifier(self, match_type: str) -> float:
        """
        Match type modifiers
        
        Exact:  1.00×
        Phrase: 0.75×
        Broad:  0.50×
        Auto:   0.45×
        """
        modifiers = {
            "EXACT": 1.00,
            "PHRASE": 0.75,
            "BROAD": 0.50,
            "AUTO": 0.45
        }
        return modifiers.get(match_type.upper(), 0.75)
    
    def _get_time_of_day_modifier(self, hour: int) -> float:
        """
        Time-of-day multipliers (Eastern Time)
        
        6-10 PM:  1.15-1.25× (use 1.20×)
        7-10 AM:  0.95×
        4-6 PM:   1.00×
        11 AM-3 PM: 0.80×
        12-6 AM:  0.70×
        10 PM-12 AM: 1.00×
        """
        if 18 <= hour < 22:  # 6 PM - 10 PM (GOLD WINDOW)
            return 1.20
        elif 7 <= hour < 10:  # 7 AM - 10 AM
            return 0.95
        elif 16 <= hour < 18:  # 4 PM - 6 PM
            return 1.00
        elif 11 <= hour < 15:  # 11 AM - 3 PM
            return 0.80
        elif 0 <= hour < 6:  # 12 AM - 6 AM
            return 0.70
        else:  # 10 PM - 12 AM and 10 AM - 11 AM
            return 1.00
    
    def _generate_reason(
        self,
        should_update: bool,
        tier: str,
        time_modifier: float,
        hour: int
    ) -> str:
        """Generate human-readable reason for bid change"""
        if not should_update:
            return "no_change_needed"
        
        if time_modifier > 1.1:
            return f"time_of_day_boost_hour_{hour}"
        elif time_modifier < 0.9:
            return f"time_of_day_reduce_hour_{hour}"
        elif tier in ["A", "B"]:
            return f"performance_tier_{tier}_optimization"
        else:
            return f"standard_optimization_tier_{tier}"

    def classify_performance_tier(
        self,
        conversions: int,
        clicks: int,
        acos: float,
        cvr: float
    ) -> str:
        """
        Classify keyword into performance tier
        
        Tier A: ≥2 conv, CVR≥18%, ACOS≤25%
        Tier B: ≥1 conv, CVR 10-17%, ACOS 25-40%
        Tier C: ≥15 clicks, no conv yet
        Tier D: ≥20 clicks, 0 conv
        Tier E: ≥30 clicks, 0 conv, no hope
        """
        if conversions >= 2 and cvr >= 0.18 and acos <= 0.25:
            return "A"
        elif conversions >= 1 and 0.10 <= cvr < 0.18 and acos <= 0.40:
            return "B"
        elif clicks >= 30 and conversions == 0:
            return "E"
        elif clicks >= 20 and conversions == 0:
            return "D"
        elif clicks >= 15:
            return "C"
        else:
            return "C"  # Default for new keywords
