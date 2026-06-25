"""
Bid calculation rules engine (canonical, single source of truth).

This is the ONE BidCalculator for the whole system. Both the hourly
optimizer job and the unit tests import from here. The previous build had
three competing copies (one here, one inline in bid_optimizer.py, and a
third implied by the test suite); they are now collapsed into this class.

Design:
    optimal_bid = AOV_base_ceiling
                  * performance_multiplier
                  * match_type_modifier
                  * time_of_day_modifier

Multi-tenant note:
    calculate_optimal_bid() accepts an optional per-keyword `user_override`
    (with an `override_expires_at`). When a customer has manually pinned a
    bid, the engine honors it and refuses to move the bid until the override
    expires. This is the hook a SaaS dashboard writes to.
"""

from typing import Optional, Dict, Any
from datetime import datetime
import pytz

from .config import settings
from .logger import get_logger

logger = get_logger(__name__)


class BidCalculator:
    """Calculate optimal bids from AOV, performance tier, match type, and hour."""

    def __init__(self, target_acos: Optional[float] = None):
        self.target_acos = target_acos if target_acos is not None else settings.default_target_acos
        self.tz = pytz.timezone(settings.timezone)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def calculate_optimal_bid(
        self,
        asin_aov: float = None,
        current_bid: float = 0.0,
        keyword_id: Optional[str] = None,
        performance_tier: Optional[str] = None,
        match_type: str = "BROAD",
        conversions: int = 0,
        clicks: int = 0,
        acos: float = 0.0,
        cvr: float = 0.0,
        current_hour: Optional[int] = None,
        user_override: Optional[float] = None,
        override_expires_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point for bid calculation.

        If `performance_tier` is None it is derived from the performance
        signals. If a live `user_override` is supplied, it wins outright.
        """
        # 0. Honor a manual override (per-tenant pin written by the dashboard)
        if user_override is not None and (
            override_expires_at is None or self._override_active(override_expires_at)
        ):
            bid = round(float(user_override), 2)
            return {
                "optimal_bid": bid,
                "reason": "user_override_active",
                "tier": performance_tier or "OVERRIDE",
                "should_update": False,
                "components": {"override": bid},
            }

        # 1. Context
        if current_hour is None:
            current_hour = datetime.now(self.tz).hour

        # 2. Tier (use caller's if provided, else classify)
        tier = performance_tier or self.classify_performance_tier(
            conversions, clicks, acos, cvr
        )

        # 3. Multipliers
        base = self._get_aov_base_ceiling(
            asin_aov if asin_aov is not None else settings.default_aov
        )
        perf_mult = self._get_performance_multiplier(tier, conversions, clicks, acos, cvr)
        match_mult = self._get_match_type_modifier(match_type)
        time_mult = self._get_time_of_day_modifier(current_hour)

        # 4. Compose
        optimal_bid = base * perf_mult * match_mult * time_mult

        # 5. Never cut a proven winner below its current bid — scale it instead
        if tier == "A" and optimal_bid < current_bid:
            optimal_bid = current_bid * 1.10

        # 6. Hard limits + rounding
        optimal_bid = max(settings.min_bid, min(optimal_bid, settings.max_bid))
        optimal_bid = round(optimal_bid, 2)

        # 7. Stability gate
        should_update = abs(optimal_bid - float(current_bid)) >= 0.05

        return {
            "optimal_bid": optimal_bid,
            "reason": self._generate_reason(should_update, tier, time_mult, current_hour),
            "tier": tier,
            "should_update": should_update,
            "components": {
                "base": base,
                "perf_mult": perf_mult,
                "match_mult": match_mult,
                "time_mult": time_mult,
            },
        }

    def calculate_optimal_bid_from_data(
        self,
        keyword_data: Dict[str, Any],
        current_hour: int,
        asin_aov: Optional[float] = None,
        user_override: Optional[float] = None,
        override_expires_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Convenience wrapper used by the optimizer job, which works with raw
        BigQuery row dicts. Maps a keyword row onto calculate_optimal_bid().
        """
        aov = asin_aov if asin_aov is not None else float(
            keyword_data.get("aov", settings.default_aov)
        )
        return self.calculate_optimal_bid(
            asin_aov=aov,
            current_bid=float(keyword_data.get("current_bid", 0.0)),
            keyword_id=keyword_data.get("keywordId"),
            performance_tier=None,
            match_type=keyword_data.get("matchType", "BROAD"),
            conversions=int(keyword_data.get("conversions", 0)),
            clicks=int(keyword_data.get("clicks", 0)),
            acos=float(keyword_data.get("acos", 0.0)),
            cvr=float(keyword_data.get("cvr", 0.0)),
            current_hour=current_hour,
            user_override=user_override,
            override_expires_at=override_expires_at,
        )

    def calculate_harvest_bid(self, aov: float, acos: float, cvr: float) -> float:
        """Initial bid for a newly harvested search term (85% of break-even)."""
        break_even_bid = aov * self.target_acos * cvr
        harvest_bid = break_even_bid * 0.85
        harvest_bid = max(settings.min_bid, min(harvest_bid, settings.max_bid))
        return round(harvest_bid, 2)

    # ------------------------------------------------------------------ #
    # Tier classification
    # ------------------------------------------------------------------ #
    def classify_performance_tier(
        self, conversions: int, clicks: int, acos: float, cvr: float
    ) -> str:
        """
        A: Winners (>=2 conv, strong CVR, low ACoS)
        B: Solid   (>=1 conv, decent CVR, acceptable ACoS)
        E: Bleeders (>=30 clicks, 0 conversions) -> kill
        D: Warning  (>=20 clicks, 0 conversions) -> cut hard
        C: Testing / insufficient data (default)
        """
        if conversions >= 2 and cvr >= 0.15 and acos <= 0.25:
            return "A"
        if conversions >= 1 and cvr >= 0.10 and acos <= 0.40:
            return "B"
        if clicks >= 30 and conversions == 0:
            return "E"
        if clicks >= 20 and conversions == 0:
            return "D"
        return "C"

    # ------------------------------------------------------------------ #
    # Multiplier tables
    # ------------------------------------------------------------------ #
    def _get_aov_base_ceiling(self, aov: float) -> float:
        if aov is None:
            aov = settings.default_aov
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
        conversions: int = 0,
        clicks: int = 0,
        acos: float = 0.0,
        cvr: float = 0.0,
    ) -> float:
        """
        Tier scales the AOV ceiling. Winners hold at the ceiling (1.00),
        everything below is progressively discounted; bleeders are starved.
        Extra performance args are accepted to keep one stable call signature
        across the codebase and allow future tie-breaking.
        """
        return {
            "A": 1.00,  # hold winners at ceiling
            "B": 0.85,  # solid
            "C": 0.65,  # testing / conservative
            "D": 0.40,  # cut hard
            "E": 0.15,  # starve bleeders
        }.get(tier, 0.65)

    def _get_match_type_modifier(self, match_type: str) -> float:
        return {
            "EXACT": 1.00,
            "PHRASE": 0.75,
            "BROAD": 0.50,
            "AUTO": 0.40,
        }.get((match_type or "BROAD").upper(), 0.50)

    def _get_time_of_day_modifier(self, hour: int) -> float:
        # Eastern-time day-parting
        if 18 <= hour < 22:
            return 1.20   # prime time 6pm-10pm
        elif 7 <= hour < 10:
            return 0.95   # morning
        elif 0 <= hour < 6:
            return 0.70   # overnight
        else:
            return 0.80   # daytime / late-evening default

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _override_active(override_expires_at: Optional[datetime]) -> bool:
        if override_expires_at is None:
            return False
        exp = override_expires_at
        if exp.tzinfo is None:
            exp = pytz.UTC.localize(exp)
        return exp > datetime.now(pytz.UTC)

    def _generate_reason(self, should_update: bool, tier: str, time_mult: float, hour: int) -> str:
        if not should_update:
            return "hold"
        if time_mult > 1.1:
            return f"time_of_day_boost_h{hour}"
        if time_mult < 0.75:
            return f"time_of_day_cut_h{hour}"
        return f"tier_{tier}_opt"


__all__ = ["BidCalculator"]
