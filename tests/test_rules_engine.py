"""
Unit tests for bid calculation rules
"""

import pytest
from automation.shared.rules_engine import BidCalculator

class TestBidCalculator:
    def setup_method(self):
        self.calculator = BidCalculator(target_acos=0.30)
    
    def test_aov_base_ceiling(self):
        """Test AOV-based base ceilings"""
        assert self.calculator._get_aov_base_ceiling(25) == 1.05  # Tier L
        assert self.calculator._get_aov_base_ceiling(40) == 1.40  # Tier M
        assert self.calculator._get_aov_base_ceiling(60) == 1.95  # Tier H
        assert self.calculator._get_aov_base_ceiling(80) == 2.50  # Tier X
    
    def test_performance_multiplier(self):
        """Test performance tier multipliers"""
        assert self.calculator._get_performance_multiplier("A", 5, 50, 0.20, 0.20) == 1.00
        assert self.calculator._get_performance_multiplier("B", 2, 30, 0.30, 0.15) == 0.85
        assert self.calculator._get_performance_multiplier("C", 0, 15, 0, 0) == 0.65
        assert self.calculator._get_performance_multiplier("D", 0, 25, 0, 0) == 0.40
    
    def test_match_type_modifier(self):
        """Test match type modifiers"""
        assert self.calculator._get_match_type_modifier("EXACT") == 1.00
        assert self.calculator._get_match_type_modifier("PHRASE") == 0.75
        assert self.calculator._get_match_type_modifier("BROAD") == 0.50
    
    def test_time_of_day_modifier(self):
        """Test time-of-day multipliers"""
        assert self.calculator._get_time_of_day_modifier(19) == 1.20  # 7 PM (peak)
        assert self.calculator._get_time_of_day_modifier(8) == 0.95   # 8 AM
        assert self.calculator._get_time_of_day_modifier(12) == 0.80  # Noon
        assert self.calculator._get_time_of_day_modifier(3) == 0.70   # 3 AM
    
    def test_performance_tier_classification(self):
        """Test keyword tier classification"""
        # Tier A: Winners
        assert self.calculator.classify_performance_tier(
            conversions=3, clicks=50, acos=0.20, cvr=0.20
        ) == "A"
        
        # Tier B: Solid
        assert self.calculator.classify_performance_tier(
            conversions=1, clicks=30, acos=0.35, cvr=0.12
        ) == "B"
        
        # Tier C: Testing
        assert self.calculator.classify_performance_tier(
            conversions=0, clicks=15, acos=0, cvr=0
        ) == "C"
        
        # Tier E: Kill
        assert self.calculator.classify_performance_tier(
            conversions=0, clicks=35, acos=0, cvr=0
        ) == "E"
    
    def test_optimal_bid_calculation(self):
        """Test full optimal bid calculation"""
        result = self.calculator.calculate_optimal_bid(
            keyword_id="test123",
            asin_aov=45.0,  # Tier M: $1.40 base
            performance_tier="A",  # 1.00× multiplier
            match_type="EXACT",  # 1.00× multiplier
            current_bid=1.20,
            conversions=3,
            clicks=50,
            acos=0.25,
            cvr=0.18,
            current_hour=19  # 7 PM: 1.20× multiplier
        )
        
        # Expected: 1.40 × 1.00 × 1.00 × 1.20 = 1.68
        assert result["optimal_bid"] == 1.68
        assert result["should_update"] == True  # Difference > $0.05
        assert "time_of_day_boost" in result["reason"]
    
    def test_user_override_respected(self):
        """Test that user overrides are respected"""
        from datetime import datetime, timedelta
        import pytz
        
        future_time = datetime.now(pytz.UTC) + timedelta(hours=1)
        
        result = self.calculator.calculate_optimal_bid(
            keyword_id="test123",
            asin_aov=45.0,
            performance_tier="A",
            match_type="EXACT",
            current_bid=2.50,
            conversions=3,
            clicks=50,
            user_override=2.50,
            override_expires_at=future_time
        )
        
        assert result["optimal_bid"] == 2.50
        assert result["should_update"] == False
        assert result["reason"] == "user_override_active"
