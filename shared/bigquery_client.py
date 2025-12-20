"""
BigQuery operations for automation
"""

from google.cloud import bigquery
from datetime import datetime, date
from typing import List, Dict, Optional
import pytz
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class BigQueryClient:
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id or settings.project_id
        self.dataset_id = dataset_id or settings.dataset_id
        self.client = bigquery.Client(project=self.project_id)
        self.tz = pytz.timezone(settings.timezone)
    
    def get_asin_aov_map(self, days: int = 14, min_orders: int = 2) -> Dict[str, float]:
        """
        Get AOV per ASIN from last N days
        Returns dict: {asin: aov}
        """
        query = f"""
        SELECT
          advertisedAsin AS asin,
          SAFE_DIVIDE(SUM(sales), NULLIF(SUM(purchases), 0)) AS aov,
          SUM(purchases) AS orders
        FROM `{self.project_id}.{self.dataset_id}.sp_advertised_product_metrics`
        WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        GROUP BY asin
        HAVING orders >= @min_orders
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
            logger.error(f"Error fetching AOV data: {e}")
            return {}
    
    def get_keywords_for_optimization(
        self,
        min_clicks: int = 5,
        days_lookback: int = 14
    ) -> List[Dict]:
        """
        Get all enabled keywords with performance data
        """
        query = f"""
        WITH keyword_metrics AS (
          SELECT
            k.keywordId,
            k.adGroupId,
            k.campaignId,
            k.keywordText,
            k.matchType,
            k.bid as current_bid,
            k.state,
            
            -- Performance metrics
            COALESCE(SUM(m.clicks), 0) as clicks,
            COALESCE(SUM(m.impressions), 0) as impressions,
            COALESCE(SUM(m.cost), 0) as spend,
            COALESCE(SUM(m.purchases), 0) as conversions,
            COALESCE(SUM(m.sales), 0) as sales,
            
            -- Calculated metrics
            SAFE_DIVIDE(SUM(m.purchases), NULLIF(SUM(m.clicks), 0)) as cvr,
            SAFE_DIVIDE(SUM(m.cost), NULLIF(SUM(m.sales), 0)) as acos
            
          FROM `{self.project_id}.{self.dataset_id}.sp_keywords` k
          LEFT JOIN `{self.project_id}.{self.dataset_id}.sp_targeting_metrics` m
            ON k.keywordId = m.targetId
            AND m.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days_lookback DAY)
          WHERE k.state = 'ENABLED'
          GROUP BY 1,2,3,4,5,6,7
        ),
        
        keyword_asins AS (
          SELECT DISTINCT
            t.targetId as keywordId,
            a.advertisedAsin
          FROM `{self.project_id}.{self.dataset_id}.sp_targeting_metrics` t
          INNER JOIN `{self.project_id}.{self.dataset_id}.sp_advertised_product_metrics` a
            ON t.campaignId = a.campaignId
            AND t.segments_date = a.segments_date
          WHERE t.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days_lookback DAY)
        )
        
        SELECT
          km.*,
          ka.advertisedAsin
        FROM keyword_metrics km
        LEFT JOIN keyword_asins ka ON km.keywordId = ka.keywordId
        WHERE km.clicks >= @min_clicks
          OR km.conversions > 0
        ORDER BY km.spend DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days_lookback", "INT64", days_lookback),
                bigquery.ScalarQueryParameter("min_clicks", "INT64", min_clicks),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            keywords = [dict(row) for row in rows]
            logger.info(f"Loaded {len(keywords)} keywords for optimization")
            return keywords
        except Exception as e:
            logger.error(f"Error fetching keywords: {e}")
            return []
    
    def get_campaign_budget_status(self) -> List[Dict]:
        """
        Get today's spend vs budget for all active campaigns
        """
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
            logger.info(f"Loaded budget status for {len(campaigns)} campaigns")
            return campaigns
        except Exception as e:
            logger.error(f"Error fetching campaign budgets: {e}")
            return []
    
    def log_bid_change(
        self,
        keyword_id: str,
        old_bid: float,
        new_bid: float,
        reason: str,
        changed_by: str = "system",
        components: Dict = None
    ):
        """
        Log bid changes to BigQuery for audit trail
        """
        table_id = f"{self.project_id}.{self.dataset_id}.bid_change_log"
        
        row = {
            "change_id": f"{keyword_id}_{datetime.now(self.tz).isoformat()}",
            "keyword_id": keyword_id,
            "old_bid": old_bid,
            "new_bid": new_bid,
            "bid_change": new_bid - old_bid,
            "reason": reason,
            "changed_by": changed_by,
            "components": str(components) if components else None,
            "changed_at": datetime.now(self.tz).isoformat(),
            "dry_run": settings.dry_run
        }
        
        try:
            # Ensure table exists
            self._ensure_bid_log_table_exists()
            
            errors = self.client.insert_rows_json(table_id, [row])
            if errors:
                logger.error(f"Error logging bid change: {errors}")
            else:
                logger.info(f"✅ Logged bid change: {keyword_id} {old_bid} → {new_bid}")
        except Exception as e:
            logger.error(f"Error logging to BigQuery: {e}")
    
    def _ensure_bid_log_table_exists(self):
        """Create bid_change_log table if it doesn't exist"""
        table_id = f"{self.project_id}.{self.dataset_id}.bid_change_log"
        
        schema = [
            bigquery.SchemaField("change_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("keyword_id", "STRING"),
            bigquery.SchemaField("old_bid", "FLOAT64"),
            bigquery.SchemaField("new_bid", "FLOAT64"),
            bigquery.SchemaField("bid_change", "FLOAT64"),
            bigquery.SchemaField("reason", "STRING"),
            bigquery.SchemaField("changed_by", "STRING"),
            bigquery.SchemaField("components", "STRING"),
            bigquery.SchemaField("changed_at", "TIMESTAMP"),
            bigquery.SchemaField("dry_run", "BOOL"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            self.client.create_table(table, exists_ok=True)
        except Exception as e:
            logger.warning(f"Could not create bid_change_log table: {e}")
