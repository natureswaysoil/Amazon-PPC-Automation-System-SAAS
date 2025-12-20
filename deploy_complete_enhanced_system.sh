#!/bin/bash
set -e

PROJECT_ID="amazon-ppc-bid-optimizer"
REGION="us-central1"

echo "🚀 Creating Complete Enhanced Amazon PPC System"
echo "================================================"

cd ~
rm -rf amazon-ppc-automation-enhanced
mkdir -p amazon-ppc-automation-enhanced
cd amazon-ppc-automation-enhanced

mkdir -p automation/shared
mkdir -p automation/jobs
touch automation/__init__.py
touch automation/shared/__init__.py
touch automation/jobs/__init__.py

# ============================================
# requirements.txt
# ============================================
cat > requirements.txt << 'EOF'
google-cloud-bigquery==3.14.0
google-cloud-secret-manager==2.16.4
google-cloud-logging==3.8.0
requests==2.31.0
python-dateutil==2.8.2
pytz==2023.3
pydantic==2.5.0
pydantic-settings==2.1.0
tenacity==8.2.3
EOF

# ============================================
# Dockerfile
# ============================================
cat > Dockerfile << 'EOF'
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY automation/ ./automation/

ENV PYTHONPATH=/app

CMD ["python", "-m", "automation.jobs.bid_optimizer"]
EOF

cat > .dockerignore << 'EOF'
__pycache__/
*.pyc
.git/
.env
venv/
EOF

# ============================================
# automation/shared/config.py
# ============================================
cat > automation/shared/config.py << 'PYEOF'
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    project_id: str = os.getenv("GCP_PROJECT", "amazon-ppc-bid-optimizer")
    dataset_id: str = os.getenv("BQ_DATASET", "amazon_ppc")
    region: str = os.getenv("GCP_REGION", "us-central1")
    
    # Optimization settings
    default_target_acos: float = 0.30
    default_aov: float = 35.0
    min_bid: float = 0.10
    max_bid: float = 5.00
    
    # Budget pacing
    budget_warning_threshold_3pm: float = 0.65
    budget_critical_threshold_3pm: float = 0.75
    
    # Keyword harvesting
    harvest_min_clicks: int = 10
    harvest_min_orders: int = 2
    harvest_max_acos: float = 0.35
    harvest_days_lookback: int = 30
    
    # System
    max_data_age_hours: int = 48
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

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
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
        
        self.client_id = self._get_secret("amazon_client_id")
        self.client_secret = self._get_secret("amazon_client_secret")
        self.refresh_token = self._get_secret("amazon_refresh_token")
    
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
            logger.info(f"✅ Updated secret: {secret_name}")
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
            logger.info("🔄 Refreshing Amazon access token...")
            response = requests.post(self.TOKEN_URL, data=payload, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            logger.info(f"✅ Token refreshed (expires in {expires_in}s)")
            
            new_refresh_token = token_data.get("refresh_token")
            if new_refresh_token and new_refresh_token != self.refresh_token:
                logger.info("🔄 Refresh token rotated, updating Secret Manager...")
                self.refresh_token = new_refresh_token
                self._update_secret("amazon_refresh_token", new_refresh_token)
        except Exception as e:
            logger.error(f"❌ Token refresh failed: {e}")
            raise

_token_manager = None

def get_token_manager() -> TokenManager:
    global _token_manager
    if _token_manager is None:
        _token_manager = TokenManager()
    return _token_manager
PYEOF

# ============================================
# automation/shared/amazon_client.py
# ============================================
cat > automation/shared/amazon_client.py << 'PYEOF'
import requests
from google.cloud import secretmanager
from typing import List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import settings
from .logger import get_logger
from .token_manager import get_token_manager

logger = get_logger(__name__)

class AmazonAdsClient:
    BASE_URL = "https://advertising-api.amazon.com"
    
    def __init__(self):
        self.token_manager = get_token_manager()
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{settings.project_id}/secrets/amazon_profile_id/versions/latest"
        response = client.access_secret_version(request={"name": name})
        self.profile_id = response.payload.data.decode("UTF-8")
        self.access_token = self.token_manager.get_valid_access_token()
        logger.info("✅ AmazonAdsClient initialized")
    
    def _get_headers(self) -> Dict[str, str]:
        self.access_token = self.token_manager.get_valid_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.token_manager.client_id,
            "Amazon-Advertising-API-Scope": self.profile_id,
            "Content-Type": "application/json"
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def create_keyword(self, campaign_id: str, ad_group_id: str, keyword_text: str, 
                      match_type: str, bid: float) -> bool:
        """Create a new keyword from harvested search term"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would create keyword: {keyword_text} ({match_type}) @ ${bid:.2f}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/keywords"
        payload = [{
            "campaignId": campaign_id,
            "adGroupId": ad_group_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED",
            "bid": bid
        }]
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Created keyword: {keyword_text}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create keyword {keyword_text}: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def create_negative_keyword(self, campaign_id: str, keyword_text: str, 
                               match_type: str = "NEGATIVE_EXACT") -> bool:
        """Add negative keyword"""
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would add negative: {keyword_text}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/campaignNegativeKeywords"
        payload = [{
            "campaignId": campaign_id,
            "keywordText": keyword_text,
            "matchType": match_type,
            "state": "ENABLED"
        }]
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Added negative keyword: {keyword_text}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to add negative {keyword_text}: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def update_keyword_bid(self, keyword_id: str, new_bid: float) -> bool:
        if settings.dry_run:
            logger.info(f"[DRY RUN] Would update keyword {keyword_id} to ${new_bid:.2f}")
            return True
        
        url = f"{self.BASE_URL}/v2/sp/keywords/{keyword_id}"
        payload = {"bid": new_bid}
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Updated keyword {keyword_id} bid to ${new_bid:.2f}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update keyword: {e}")
            return False
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
        query = f"""
        SELECT
          advertisedAsin AS asin,
          SAFE_DIVIDE(SUM(sales), NULLIF(SUM(purchases), 0)) AS aov
        FROM `{self.project_id}.{self.dataset_id}.sp_advertised_product_metrics`
        WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
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
            logger.info(f"✅ Loaded AOV for {len(aov_map)} ASINs")
            return aov_map
        except Exception as e:
            logger.error(f"Error fetching AOV: {e}")
            return {}
    
    def get_keywords_for_optimization(self, min_clicks: int = 5) -> List[Dict]:
        query = f"""
        SELECT
          k.keywordId,
          k.keywordText,
          k.matchType,
          k.campaignId,
          k.adGroupId,
          k.bid as current_bid,
          COALESCE(SUM(m.clicks), 0) as clicks,
          COALESCE(SUM(m.purchases), 0) as conversions,
          COALESCE(SUM(m.cost), 0) as spend,
          COALESCE(SUM(m.sales), 0) as sales,
          SAFE_DIVIDE(SUM(m.purchases), NULLIF(SUM(m.clicks), 0)) as cvr,
          SAFE_DIVIDE(SUM(m.cost), NULLIF(SUM(m.sales), 0)) as acos
        FROM `{self.project_id}.{self.dataset_id}.sp_keywords` k
        LEFT JOIN `{self.project_id}.{self.dataset_id}.sp_targeting_metrics` m
          ON k.keywordId = m.targetId
          AND m.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        WHERE k.state = 'ENABLED'
        GROUP BY 1,2,3,4,5,6
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
            logger.info(f"✅ Loaded {len(keywords)} keywords")
            return keywords
        except Exception as e:
            logger.error(f"Error fetching keywords: {e}")
            return []
    
    def get_search_terms_for_harvesting(self) -> List[Dict]:
        """
        Find high-performing search terms to add as keywords
        """
        query = f"""
        WITH search_term_performance AS (
          SELECT
            st.query AS search_term,
            st.campaignId,
            st.adGroupId,
            st.matchType as triggering_match_type,
            SUM(st.clicks) as clicks,
            SUM(st.purchases) as orders,
            SUM(st.sales) as sales,
            SUM(st.cost) as spend,
            SAFE_DIVIDE(SUM(st.cost), NULLIF(SUM(st.sales), 0)) as acos,
            SAFE_DIVIDE(SUM(st.purchases), NULLIF(SUM(st.clicks), 0)) as cvr
          FROM `{self.project_id}.{self.dataset_id}.sp_search_term_metrics` st
          WHERE st.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
            AND st.query IS NOT NULL
            AND st.query != ''
          GROUP BY 1,2,3,4
        ),
        existing_keywords AS (
          SELECT LOWER(keywordText) as keyword_text
          FROM `{self.project_id}.{self.dataset_id}.sp_keywords`
          WHERE state = 'ENABLED'
        )
        SELECT
          stp.*
        FROM search_term_performance stp
        LEFT JOIN existing_keywords ek 
          ON LOWER(stp.search_term) = ek.keyword_text
        WHERE ek.keyword_text IS NULL
          AND stp.clicks >= @min_clicks
          AND stp.orders >= @min_orders
          AND stp.acos <= @max_acos
        ORDER BY stp.orders DESC, stp.clicks DESC
        LIMIT 50
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", settings.harvest_days_lookback),
                bigquery.ScalarQueryParameter("min_clicks", "INT64", settings.harvest_min_clicks),
                bigquery.ScalarQueryParameter("min_orders", "INT64", settings.harvest_min_orders),
                bigquery.ScalarQueryParameter("max_acos", "FLOAT64", settings.harvest_max_acos),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            search_terms = [dict(row) for row in rows]
            logger.info(f"✅ Found {len(search_terms)} search terms to harvest")
            return search_terms
        except Exception as e:
            logger.error(f"Error fetching search terms: {e}")
            return []
    
    def get_negative_search_terms(self) -> List[Dict]:
        """
        Find poor-performing search terms to add as negatives
        """
        query = f"""
        WITH search_term_performance AS (
          SELECT
            st.query AS search_term,
            st.campaignId,
            SUM(st.clicks) as clicks,
            SUM(st.purchases) as orders,
            SUM(st.cost) as spend
          FROM `{self.project_id}.{self.dataset_id}.sp_search_term_metrics` st
          WHERE st.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
            AND st.query IS NOT NULL
          GROUP BY 1,2
        )
        SELECT *
        FROM search_term_performance
        WHERE clicks >= 15
          AND orders = 0
          AND spend > 5.00
        ORDER BY spend DESC
        LIMIT 50
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", 30),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            negative_terms = [dict(row) for row in rows]
            logger.info(f"✅ Found {len(negative_terms)} negative search terms")
            return negative_terms
        except Exception as e:
            logger.error(f"Error fetching negative terms: {e}")
            return []
    
    def get_campaign_budget_status(self) -> List[Dict]:
        """Get today's spend vs budget for active campaigns"""
        query = f"""
        SELECT
          c.campaignId,
          c.name as campaign_name,
          c.budget,
          c.budgetType,
          COALESCE(SUM(m.cost), 0) as spend_today
        FROM `{self.project_id}.{self.dataset_id}.sp_campaigns` c
        LEFT JOIN `{self.project_id}.{self.dataset_id}.sp_campaign_metrics` m
          ON c.campaignId = m.campaignId
          AND m.segments_date = CURRENT_DATE()
        WHERE c.state = 'ENABLED'
          AND c.budgetType = 'DAILY'
        GROUP BY 1,2,3,4
        """
        
        try:
            rows = self.client.query(query).result()
            campaigns = [dict(row) for row in rows]
            logger.info(f"✅ Loaded {len(campaigns)} campaigns")
            return campaigns
        except Exception as e:
            logger.error(f"Error fetching campaigns: {e}")
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
            if not errors:
                logger.info(f"✅ Logged bid change: {keyword_id}")
        except Exception as e:
            logger.error(f"Error logging: {e}")
    
    def log_keyword_harvest(self, search_term: str, campaign_id: str, match_type: str, 
                           bid: float, action: str):
        table_id = f"{self.project_id}.{self.dataset_id}.keyword_harvest_log"
        row = {
            "search_term": search_term,
            "campaign_id": campaign_id,
            "match_type": match_type,
            "bid": bid,
            "action": action,
            "harvested_at": datetime.now(self.tz).isoformat(),
            "dry_run": settings.dry_run
        }
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            if not errors:
                logger.info(f"✅ Logged harvest: {search_term}")
        except Exception as e:
            logger.error(f"Error logging harvest: {e}")
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
        
        aov_base = self._get_aov_base_ceiling(asin_aov)
        tier = self._classify_tier(conversions, clicks, acos, cvr)
        perf_mult = self._get_performance_multiplier(tier)
        match_mult = self._get_match_type_modifier(match_type)
        time_mult = self._get_time_of_day_modifier(current_hour)
        
        optimal_bid = aov_base * perf_mult * match_mult * time_mult
        optimal_bid = max(settings.min_bid, min(optimal_bid, settings.max_bid))
        optimal_bid = round(optimal_bid, 2)
        
        should_update = abs(optimal_bid - current_bid) >= 0.05
        
        return {
            "optimal_bid": optimal_bid,
            "should_update": should_update,
            "tier": tier,
            "reason": f"tier_{tier}_hour_{current_hour}"
        }
    
    def calculate_harvest_bid(self, aov: float, acos: float, cvr: float) -> float:
        """Calculate initial bid for harvested keyword"""
        base_bid = aov * self.target_acos * cvr
        base_bid = max(settings.min_bid, min(base_bid, settings.max_bid))
        
        # Start conservative (85% of calculated)
        harvest_bid = base_bid * 0.85
        return round(harvest_bid, 2)
    
    def _get_aov_base_ceiling(self, aov: float) -> float:
        if aov < 30: return 1.05
        elif aov < 46: return 1.40
        elif aov < 70: return 1.95
        else: return 2.50
    
    def _classify_tier(self, conv: int, clicks: int, acos: float, cvr: float) -> str:
        if conv >= 2 and cvr >= 0.18 and acos <= 0.25: return "A"
        elif conv >= 1 and 0.10 <= cvr < 0.18 and acos <= 0.40: return "B"
        elif clicks >= 30 and conv == 0: return "E"
        elif clicks >= 20 and conv == 0: return "D"
        else: return "C"
    
    def _get_performance_multiplier(self, tier: str) -> float:
        return {"A": 1.00, "B": 0.85, "C": 0.65, "D": 0.40, "E": 0.15}.get(tier, 0.65)
    
    def _get_match_type_modifier(self, match_type: str) -> float:
        return {"EXACT": 1.00, "PHRASE": 0.75, "BROAD": 0.50}.get(match_type.upper(), 0.75)
    
    def _get_time_of_day_modifier(self, hour: int) -> float:
        if 18 <= hour < 22: return 1.20
        elif 7 <= hour < 10: return 0.95
        elif 16 <= hour < 18: return 1.00
        elif 11 <= hour < 15: return 0.80
        elif 0 <= hour < 6: return 0.70
        else: return 1.00
PYEOF

echo "✅ All shared modules created!"
echo ""
echo "Now creating job files..."

# Continue in next part...
