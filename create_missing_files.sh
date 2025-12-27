#!/bin/bash
# automated_setup.sh

# 1. Create Directory Structure
mkdir -p automation/shared
touch automation/__init__.py
touch automation/shared/__init__.py

echo "📂 Created directory structure."

# ============================================
# automation/shared/config.py
# ============================================
cat > automation/shared/config.py << 'PYEOF'
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Cloud Run provides these automatically, but defaults help local dev
    project_id: str = os.getenv("GCP_PROJECT", "amazon-ppc-bid-optimizer")
    dataset_id: str = os.getenv("BQ_DATASET", "amazon_ppc")
    region: str = os.getenv("GCP_REGION", "us-central1")
    
    # Bidding Rules
    default_target_acos: float = 0.30
    default_aov: float = 35.0
    min_bid: float = 0.20  # Increased floor to prevent "dead" keywords
    max_bid: float = 6.00
    
    # System
    timezone: str = "America/New_York"
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"
    
    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
PYEOF

# ============================================
# automation/shared/logger.py
# ============================================
cat > automation/shared/logger.py << 'PYEOF'
import logging
import sys

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
PYEOF

# ============================================
# automation/shared/token_manager.py
# ============================================
cat > automation/shared/token_manager.py << 'PYEOF'
import requests
from datetime import datetime, timedelta
from google.cloud import secretmanager
from typing import Optional
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class TokenManager:
    TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    TOKEN_EXPIRY_BUFFER = 300
    
    def __init__(self):
        self.project_id = settings.project_id
        self.sm_client = secretmanager.SecretManagerServiceClient()
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
        # Load initial secrets
        try:
            self.client_id = self._get_secret("amazon_client_id")
            self.client_secret = self._get_secret("amazon_client_secret")
            self.refresh_token = self._get_secret("amazon_refresh_token")
        except Exception as e:
            logger.warning(f"Could not load secrets (local dev?): {e}")

    def _get_secret(self, secret_name: str) -> str:
        try:
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
            response = self.sm_client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Error fetching secret {secret_name}: {e}")
            raise
    
    def _update_secret(self, secret_name: str, new_value: str):
        try:
            parent = f"projects/{self.project_id}/secrets/{secret_name}"
            self.sm_client.add_secret_version(
                request={"parent": parent, "payload": {"data": new_value.encode("UTF-8")}}
            )
            logger.info(f"Updated secret: {secret_name}")
            return True
        except Exception as e:
            logger.error(f"Error updating secret: {e}")
            return False
    
    def get_valid_access_token(self) -> str:
        if self._needs_refresh():
            self._refresh_access_token()
        return self.access_token
    
    def _needs_refresh(self) -> bool:
        if not self.access_token or not self.token_expires_at:
            return True
        time_until_expiry = (self.token_expires_at - datetime.utcnow()).total_seconds()
        return time_until_expiry < self.TOKEN_EXPIRY_BUFFER
    
    def _refresh_access_token(self):
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        try:
            logger.info("Refreshing Amazon access token...")
            response = requests.post(self.TOKEN_URL, data=payload, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            logger.info(f"✅ Access token refreshed (expires in {expires_in}s)")
            
            new_refresh_token = token_data.get("refresh_token")
            if new_refresh_token and new_refresh_token != self.refresh_token:
                logger.info("Refresh token rotated, updating Secret Manager...")
                self.refresh_token = new_refresh_token
                self._update_secret("amazon_refresh_token", new_refresh_token)
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            raise

_token_manager = None

def get_token_manager() -> TokenManager:
    global _token_manager
    if _token_manager is None:
        _token_manager = TokenManager()
    return _token_manager
PYEOF

# ============================================
# automation/shared/bigquery_client.py
# ============================================
cat > automation/shared/bigquery_client.py << 'PYEOF'
from google.cloud import bigquery
from datetime import datetime
from typing import List, Dict
import pytz
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class BigQueryClient:
    def __init__(self):
        self.project_id = settings.project_id
        self.dataset_id = settings.dataset_id
        self.client = bigquery.Client(project=self.project_id)
        self.tz = pytz.timezone(settings.timezone)
    
    def get_asin_aov_map(self, days: int = 14, min_orders: int = 2) -> Dict[str, float]:
        # FIX: Exclude last 3 days to avoid attribution lag
        query = f"""
        SELECT
            advertisedAsin AS asin,
            SAFE_DIVIDE(SUM(sales), NULLIF(SUM(purchases), 0)) AS aov
        FROM `{self.project_id}.{self.dataset_id}.sp_advertised_product_metrics`
        WHERE segments_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL @days + 3 DAY) 
                                AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        GROUP BY asin
        HAVING SUM(purchases) >= @min_orders
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days),
                bigquery.ScalarQueryParameter("min_orders", "INT64", min_orders),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            aov_map = {row["asin"]: float(row["aov"]) for row in rows if row["aov"]}
            logger.info(f"Loaded AOV for {len(aov_map)} ASINs")
            return aov_map
        except Exception as e:
            logger.error(f"Error fetching AOV: {e}")
            return {}
    
    def get_keywords_for_optimization(self, min_clicks: int = 5) -> List[Dict]:
        # FIX: Added `advertisedAsin` to SELECT via Join or Assumptions
        # FIX: Excluded last 3 days for attribution
        query = f"""
        SELECT
            k.keywordId,
            k.keywordText,
            k.matchType,
            k.bid as current_bid,
            -- Try to get ASIN from metrics or Campaign mapping if available. 
            -- Assuming sp_targeting_metrics has advertisedAsin in your schema:
            m.advertisedAsin, 
            COALESCE(SUM(m.clicks), 0) as clicks,
            COALESCE(SUM(m.purchases), 0) as conversions,
            COALESCE(SUM(m.cost), 0) as spend,
            COALESCE(SUM(m.sales), 0) as sales,
            SAFE_DIVIDE(SUM(m.purchases), NULLIF(SUM(m.clicks), 0)) as cvr,
            SAFE_DIVIDE(SUM(m.cost), NULLIF(SUM(m.sales), 0)) as acos
        FROM `{self.project_id}.{self.dataset_id}.sp_keywords` k
        LEFT JOIN `{self.project_id}.{self.dataset_id}.sp_targeting_metrics` m
            ON k.keywordId = m.targetId
            -- 3 Day Exclusion Window
            AND m.segments_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 17 DAY) 
                                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        WHERE k.state = 'ENABLED'
        GROUP BY 1,2,3,4,5
        HAVING clicks >= @min_clicks OR conversions > 0
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_clicks", "INT64", min_clicks),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            keywords = [dict(row) for row in rows]
            logger.info(f"Loaded {len(keywords)} keywords")
            return keywords
        except Exception as e:
            logger.error(f"Error fetching keywords: {e}")
            return []
    
    def log_bid_change(self, keyword_id: str, old_bid: float, new_bid: float, reason: str):
        table_id = f"{self.project_id}.{self.dataset_id}.bid_optimizations"
        
        row = {
            "keyword_id": keyword_id,
            "old_bid": old_bid,
            "new_bid": new_bid,
            "reason": reason,
            "changed_at": datetime.now(self.tz).isoformat(),
            "dry_run": settings.dry_run
        }
        
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            if errors:
                logger.error(f"BQ Insert Errors: {errors}")
            else:
                logger.info(f"Logged bid change: {keyword_id}")
        except Exception as e:
            logger.error(f"Error logging to BigQuery: {e}")
PYEOF

# ============================================
# automation/shared/rules_engine.py
# ============================================
cat > automation/shared/rules_engine.py << 'PYEOF'
from datetime import datetime
import pytz
from .config import settings

class BidCalculator:
    def __init__(self):
        self.target_acos = settings.default_target_acos
        self.tz = pytz.timezone(settings.timezone)
    
    def calculate_optimal_bid(
        self, asin_aov: float, current_bid: float, conversions: int,
        clicks: int, acos: float, cvr: float, match_type: str
    ) -> dict:
        current_hour = datetime.now(self.tz).hour
        
        # Calculate dynamic ceiling
        aov_base = self._get_aov_base_ceiling(asin_aov)
        tier = self._classify_tier(conversions, clicks, acos, cvr)
        
        perf_mult = self._get_performance_multiplier(tier)
        match_mult = self._get_match_type_modifier(match_type)
        time_mult = self._get_time_of_day_modifier(current_hour)
        
        # Formula: Ceiling * Modifiers
        optimal_bid = aov_base * perf_mult * match_mult * time_mult
        
        # Logic Check: Don't just blindly use formula if we have data
        # If performing well (Tier A), ensure we are at least increasing bid
        if tier == "A" and optimal_bid < current_bid:
             optimal_bid = current_bid * 1.10

        # Hard Caps
        optimal_bid = max(settings.min_bid, min(optimal_bid, settings.max_bid))
        optimal_bid = round(optimal_bid, 2)
        
        should_update = abs(optimal_bid - current_bid) >= 0.05
        
        return {
            "optimal_bid": optimal_bid,
            "should_update": should_update,
            "tier": tier,
            "reason": f"tier_{tier}_h{current_hour}_aov{int(asin_aov)}"
        }
    
    def _get_aov_base_ceiling(self, aov: float) -> float:
        if aov < 30: return 1.05
        elif aov < 46: return 1.40
        elif aov < 70: return 1.95
        else: return 2.50
    
    def _classify_tier(self, conv: int, clicks: int, acos: float, cvr: float) -> str:
        if conv >= 2 and cvr >= 0.18 and acos <= 0.25: return "A"
        elif conv >= 1 and 0.10 <= cvr < 0.18 and acos <= 0.40: return "B"
        elif clicks >= 30 and conv == 0: return "E" # Heavy Bleeder
        elif clicks >= 15 and conv == 0: return "D" # Warning
        else: return "C" # Testing
    
    def _get_performance_multiplier(self, tier: str) -> float:
        # A=Winner, B=Good, C=Test, D=Cut, E=Kill
        return {"A": 1.20, "B": 1.00, "C": 0.75, "D": 0.40, "E": 0.15}.get(tier, 0.75)
    
    def _get_match_type_modifier(self, match_type: str) -> float:
        return {"EXACT": 1.00, "PHRASE": 0.80, "BROAD": 0.60}.get(match_type.upper(), 0.60)
    
    def _get_time_of_day_modifier(self, hour: int) -> float:
        # Dayparting: Boost evening, cut overnight
        if 18 <= hour < 22: return 1.20
        elif 7 <= hour < 10: return 0.95
        elif 16 <= hour < 18: return 1.00
        elif 11 <= hour < 15: return 0.85
        elif 0 <= hour < 6: return 0.60
        else: return 1.00
PYEOF

# ============================================
# automation/bid_optimizer.py (MAIN)
# ============================================
cat > automation/bid_optimizer.py << 'PYEOF'
import sys
# Logic to handle running as script vs module
try:
    from automation.shared.config import settings
    from automation.shared.logger import get_logger
    from automation.shared.bigquery_client import BigQueryClient
    from automation.shared.token_manager import get_token_manager
    from automation.shared.rules_engine import BidCalculator
except ImportError:
    # Fallback if running from inside automation folder
    from shared.config import settings
    from shared.logger import get_logger
    from shared.bigquery_client import BigQueryClient
    from shared.token_manager import get_token_manager
    from shared.rules_engine import BidCalculator

logger = get_logger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("🚀 Starting Bid Optimizer")
    logger.info(f"Project: {settings.project_id}")
    logger.info(f"Dry Run: {settings.dry_run}")
    logger.info("=" * 60)
    
    try:
        logger.info("\nStep 1: Refreshing Amazon API token...")
        # Note: In a dry run, we still check the token to ensure connectivity
        token_manager = get_token_manager()
        token_manager.get_valid_access_token()
        logger.info("✅ Token ready")
        
        logger.info("\nStep 2: Loading data from BigQuery...")
        bq = BigQueryClient()
        
        aov_map_14d = bq.get_asin_aov_map(days=14, min_orders=2)
        aov_map_30d = bq.get_asin_aov_map(days=30, min_orders=2)
        
        keywords = bq.get_keywords_for_optimization(min_clicks=5)
        logger.info(f"Found {len(keywords)} keywords to optimize")
        
        if not keywords:
            logger.info("No keywords to optimize")
            return
        
        logger.info("\nStep 3: Calculating optimal bids...")
        calculator = BidCalculator()
        
        updates = []
        for kw in keywords:
            # FIX: Use correct key 'advertisedAsin' from SQL
            asin = kw.get("advertisedAsin")
            
            # Fallback logic for AOV lookup
            aov = settings.default_aov
            if asin:
                aov = aov_map_14d.get(asin) or aov_map_30d.get(asin) or settings.default_aov
            
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
                    "tier": result["tier"]
                })
                
                # In Dry Run, we still log to BQ with dry_run=True flag
                bq.log_bid_change(
                    kw["keywordId"],
                    kw["current_bid"],
                    result["optimal_bid"],
                    result["reason"]
                )
        
        logger.info(f"\n✅ Found {len(updates)} bids to update")
        
        for u in updates[:10]:
            logger.info(f"  {u['keyword_text']}: ${u['old_bid']:.2f} -> ${u['new_bid']:.2f} (Tier {u['tier']})")
        
        if settings.dry_run:
            logger.info("\n[DRY RUN] No actual bid updates sent to Amazon API")
        else:
            # TODO: Call Amazon API Batch Update here
            pass
        
        logger.info("\n✅ Bid Optimizer Completed Successfully")
        
    except Exception as e:
        logger.error(f"❌ Bid optimizer failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
PYEOF

echo "✅ All files created successfully in 'automation/' folder."
echo "👉 To run: python3 -m automation.bid_optimizer"





Evaluate

Compare



