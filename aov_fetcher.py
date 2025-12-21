"""
Real-time AOV fetcher for Amazon PPC optimization
Pulls per-ASIN AOV from BigQuery with intelligent fallbacks
"""

import os
import logging
from typing import Dict, Tuple
from dataclasses import dataclass
from google.cloud import bigquery
from functools import lru_cache

logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT", "amazon-ppc-474902")
DATASET = os.getenv("BQ_DATASET", "amazon_ppc")
TABLE = os.getenv("BQ_ASIN_METRICS_TABLE", "sp_advertised_product_metrics")
DEFAULT_AOV = float(os.getenv("DEFAULT_AOV", "35.0"))

@dataclass
class AsinAOV:
    asin: str
    aov: float
    orders: int
    confidence: str  # 'high', 'medium', 'low', 'default'
    source: str  # '14d', '30d', 'default'


class AOVFetcher:
    """Fetches and caches ASIN-level AOV data from BigQuery"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        self._aov_14d: Dict[str, AsinAOV] = {}
        self._aov_30d: Dict[str, AsinAOV] = {}
        
    def fetch_all(self) -> None:
        """Fetch both 14d and 30d AOV maps (call once per job run)"""
        logger.info("Fetching ASIN AOV data from BigQuery...")
        self._aov_14d = self._fetch_aov_window(days=14, min_orders=2)
        self._aov_30d = self._fetch_aov_window(days=30, min_orders=2)
        logger.info(f"✓ Loaded AOV for {len(self._aov_14d)} ASINs (14d), "
                   f"{len(self._aov_30d)} ASINs (30d)")
    
    def _fetch_aov_window(self, days: int, min_orders: int) -> Dict[str, AsinAOV]:
        """Fetch AOV for a specific time window"""
        query = f"""
        SELECT
          advertisedAsin AS asin,
          SAFE_DIVIDE(SUM(sales), NULLIF(SUM(orders), 0)) AS aov,
          SUM(orders) AS orders,
          SUM(sales) AS sales,
          COUNT(DISTINCT segments_date) AS active_days
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE 
          segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
          AND sales > 0
        GROUP BY asin
        HAVING 
          orders >= @min_orders
          AND aov > 10  -- Sanity check
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days),
                bigquery.ScalarQueryParameter("min_orders", "INT64", min_orders),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            result = {}
            
            for row in rows:
                asin = row["asin"]
                aov = float(row["aov"])
                orders = int(row["orders"])
                active_days = int(row["active_days"])
                
                # Confidence scoring
                if orders >= 10 and active_days >= 7:
                    confidence = "high"
                elif orders >= 5:
                    confidence = "medium"
                else:
                    confidence = "low"
                
                result[asin] = AsinAOV(
                    asin=asin,
                    aov=aov,
                    orders=orders,
                    confidence=confidence,
                    source=f"{days}d"
                )
            
            return result
            
        except Exception as e:
            logger.error(f"BigQuery AOV fetch failed: {e}")
            return {}
    
    def get_aov(self, asin: str) -> AsinAOV:
        """
        Get AOV for ASIN with intelligent fallback:
        1. Try 14-day window (most recent)
        2. Fall back to 30-day window
        3. Fall back to default
        """
        if asin in self._aov_14d:
            return self._aov_14d[asin]
        
        if asin in self._aov_30d:
            aov_data = self._aov_30d[asin]
            # Downgrade confidence for stale data
            if aov_data.confidence == "high":
                aov_data.confidence = "medium"
            return aov_data
        
        # Default fallback
        logger.debug(f"Using default AOV for {asin}")
        return AsinAOV(
            asin=asin,
            aov=DEFAULT_AOV,
            orders=0,
            confidence="default",
            source="default"
        )
    
    def get_aov_tier(self, asin: str) -> str:
        """
        Classify ASIN into AOV tier for bid ceiling lookup
        Returns: 'L', 'M', 'H', 'X'
        """
        aov_data = self.get_aov(asin)
        aov = aov_data.aov
        
        if aov < 30:
            return "L"
        elif aov < 46:
            return "M"
        elif aov < 70:
            return "H"
        else:
            return "X"


# Global instance (initialized once per job run)
aov_fetcher = AOVFetcher()
