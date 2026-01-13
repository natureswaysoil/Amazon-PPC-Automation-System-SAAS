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
        self._table_checked = False

    def get_asin_aov_map(self, days: int = 14, min_orders: int = 2) -> Dict[str, float]:
        """
        Get AOV per ASIN.
        NOTE: Excludes last 3 days to account for Attribution Lag.
        """
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
            logger.error(f"Error fetching AOV data: {e}")
            return {}
    
    def get_keywords_for_optimization(self, min_clicks: int = 5, days_lookback: int = 14) -> List[Dict]:
        """
        Get all enabled keywords with performance data.
        NOTE: Excludes last 3 days.
        """
        query = f"""
        WITH keyword_metrics AS (
          SELECT
            k.keyword_id as keywordId,
            k.ad_group_id as adGroupId,
            k.campaign_id as campaignId,
            k.keyword_text as keywordText,
            k.match_type as matchType,
            k.bid as current_bid,
            k.state,
            
            -- Performance metrics
            COALESCE(SUM(m.clicks), 0) as clicks,
            COALESCE(SUM(m.cost), 0) as spend,
            COALESCE(SUM(m.attributedConversions14d), 0) as conversions,
            COALESCE(SUM(m.attributedSales14d), 0) as sales,
            
            -- Calculated metrics
            SAFE_DIVIDE(SUM(m.attributedConversions14d), NULLIF(SUM(m.clicks), 0)) as cvr,
            SAFE_DIVIDE(SUM(m.cost), NULLIF(SUM(m.attributedSales14d), 0)) as acos
            
          FROM `{self.project_id}.{self.dataset_id}.keywords` k
          LEFT JOIN `{self.project_id}.{self.dataset_id}.keyword_performance` m
            ON k.keyword_id = m.keywordId
            AND m.date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL @days_lookback + 3 DAY) 
                                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
          WHERE k.state = 'enabled'
          GROUP BY 1,2,3,4,5,6,7
        ),
        
        keyword_asins AS (
          SELECT DISTINCT
            m.keywordId,
            'PLACEHOLDER' as advertisedAsin
          FROM `{self.project_id}.{self.dataset_id}.keyword_performance` m
          WHERE m.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        )
        
        SELECT
          km.*,
          COALESCE(ka.advertisedAsin, 'UNKNOWN') as advertisedAsin
        FROM keyword_metrics km
        LEFT JOIN keyword_asins ka ON km.keywordId = ka.keywordId
        WHERE km.clicks >= @min_clicks OR km.conversions > 0
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
            SAFE_DIVIDE(SUM(st.cost), NULLIF(SUM(st.sales), 0)) as acos,
            SAFE_DIVIDE(SUM(st.purchases), NULLIF(SUM(st.clicks), 0)) as cvr
          FROM `{self.project_id}.{self.dataset_id}.sp_search_term_metrics` st
          WHERE st.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
          GROUP BY 1,2,3,4
        ),
        existing_keywords AS (
          SELECT LOWER(keywordText) as keyword_text
          FROM `{self.project_id}.{self.dataset_id}.sp_keywords`
          WHERE state = 'ENABLED'
        )
        SELECT stp.*
        FROM search_term_performance stp
        LEFT JOIN existing_keywords ek ON LOWER(stp.search_term) = ek.keyword_text
        WHERE ek.keyword_text IS NULL
          AND stp.clicks >= @min_clicks
          AND stp.orders >= @min_orders
          AND stp.acos <= @max_acos
        LIMIT 50
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", getattr(settings, 'harvest_days_lookback', 30)),
                bigquery.ScalarQueryParameter("min_clicks", "INT64", getattr(settings, 'harvest_min_clicks', 10)),
                bigquery.ScalarQueryParameter("min_orders", "INT64", getattr(settings, 'harvest_min_orders', 2)),
                bigquery.ScalarQueryParameter("max_acos", "FLOAT64", getattr(settings, 'harvest_max_acos', 0.35)),
            ]
        )
        
        try:
            rows = self.client.query(query, job_config=job_config).result()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching harvest terms: {e}")
            return []

    def get_negative_search_terms(self) -> List[Dict]:
        """Find bleeding search terms"""
        query = f"""
        SELECT
          st.query AS search_term,
          st.campaignId,
          SUM(st.clicks) as clicks,
          SUM(st.cost) as spend
        FROM `{self.project_id}.{self.dataset_id}.sp_search_term_metrics` st
        WHERE st.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY 1,2
        HAVING clicks >= 15 AND SUM(st.purchases) = 0
        LIMIT 50
        """
        try:
            rows = self.client.query(query).result()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching negative terms: {e}")
            return []
    
    def get_campaign_budget_status(self) -> List[Dict]:
        """Get today's spend vs budget"""
        query = f"""
        SELECT
          c.campaign_id as campaignId,
          c.name as campaign_name,
          c.budget,
          COALESCE(SUM(m.cost), 0) as spend_today
        FROM `{self.project_id}.{self.dataset_id}.campaigns` c
        LEFT JOIN `{self.project_id}.{self.dataset_id}.campaign_performance` m
          ON c.campaign_id = m.campaignId AND m.date = CURRENT_DATE()
        WHERE c.state = 'enabled' AND c.budget_type = 'daily'
        GROUP BY 1,2,3
        """
        try:
            rows = self.client.query(query).result()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching budgets: {e}")
            return []
    
    def log_bid_change(self, keyword_id: str, old_bid: float, new_bid: float, reason: str):
        """Log bid changes to BigQuery"""
        if not self._table_checked:
            self._ensure_bid_log_table_exists()
            self._table_checked = True

        table_id = f"{self.project_id}.{self.dataset_id}.bid_change_log"
        row = {
            "change_id": f"{keyword_id}_{datetime.now(self.tz).timestamp()}",
            "keyword_id": keyword_id,
            "old_bid": float(old_bid),
            "new_bid": float(new_bid),
            "bid_change": float(new_bid - old_bid),
            "reason": reason,
            "changed_at": datetime.now(self.tz).isoformat(),
            "dry_run": settings.dry_run
        }
        
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            if errors:
                logger.error(f"Error logging bid change: {errors}")
        except Exception as e:
            logger.error(f"Error logging to BigQuery: {e}")

    def log_keyword_harvest(self, search_term: str, campaign_id: str, match_type: str, bid: float, action: str):
        """Log harvested keywords"""
        table_id = f"{self.project_id}.{self.dataset_id}.keyword_harvest_log"
        row = {
            "search_term": search_term,
            "campaign_id": campaign_id,
            "match_type": match_type,
            "bid": float(bid),
            "action": action,
            "harvested_at": datetime.now(self.tz).isoformat(),
            "dry_run": settings.dry_run
        }
        try:
            self.client.insert_rows_json(table_id, [row])
        except Exception as e:
            logger.error(f"Error logging harvest: {e}")
    
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
            bigquery.SchemaField("changed_at", "TIMESTAMP"),
            bigquery.SchemaField("dry_run", "BOOL"),
        ]
        try:
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table, exists_ok=True)
        except Exception as e:
            logger.warning(f"Could not create table: {e}")
