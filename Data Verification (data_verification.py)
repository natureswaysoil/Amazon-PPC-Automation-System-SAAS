"""
Verify live data is present and fresh before running automation
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Tuple
from .config import settings
from .logger import get_logger

logger = get_logger(__name__)

class DataVerificationError(Exception):
    """Raised when data verification fails"""
    pass

class DataVerifier:
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id or settings.project_id
        self.dataset_id = dataset_id or settings.dataset_id
        self.client = bigquery.Client(project=self.project_id)
        self.tz = pytz.timezone(settings.timezone)
        
    def verify_all(self) -> Dict[str, any]:
        """
        Run all verification checks
        Returns dict with verification results
        Raises DataVerificationError if critical checks fail
        """
        logger.info("Starting data verification...")
        
        results = {
            "timestamp": datetime.now(self.tz).isoformat(),
            "checks": {}
        }
        
        # Check 1: Dataset exists
        results["checks"]["dataset_exists"] = self._verify_dataset_exists()
        
        # Check 2: Required tables exist
        results["checks"]["tables_exist"] = self._verify_tables_exist()
        
        # Check 3: Data freshness
        results["checks"]["data_freshness"] = self._verify_data_freshness()
        
        # Check 4: Data volume (minimum rows)
        results["checks"]["data_volume"] = self._verify_data_volume()
        
        # Check 5: Active campaigns
        results["checks"]["active_campaigns"] = self._verify_active_campaigns()
        
        # Check 6: Recent conversions
        results["checks"]["recent_conversions"] = self._verify_recent_conversions()
        
        # Determine overall status
        critical_checks = [
            "dataset_exists",
            "tables_exist",
            "data_freshness",
            "active_campaigns"
        ]
        
        failed_critical = [
            check for check in critical_checks 
            if not results["checks"][check]["passed"]
        ]
        
        if failed_critical:
            results["status"] = "FAILED"
            results["failed_critical_checks"] = failed_critical
            error_msg = f"Critical verification checks failed: {', '.join(failed_critical)}"
            logger.error(error_msg)
            raise DataVerificationError(error_msg)
        else:
            results["status"] = "PASSED"
            logger.info("✅ All data verification checks passed")
        
        return results
    
    def _verify_dataset_exists(self) -> Dict:
        """Check if BigQuery dataset exists"""
        try:
            dataset_ref = f"{self.project_id}.{self.dataset_id}"
            self.client.get_dataset(dataset_ref)
            logger.info(f"✅ Dataset exists: {dataset_ref}")
            return {"passed": True, "dataset": dataset_ref}
        except Exception as e:
            logger.error(f"❌ Dataset not found: {e}")
            return {"passed": False, "error": str(e)}
    
    def _verify_tables_exist(self) -> Dict:
        """Check if required tables exist"""
        required_tables = [
            "sp_campaigns",
            "sp_ad_groups", 
            "sp_keywords",
            "sp_campaign_metrics",
            "sp_advertised_product_metrics"
        ]
        
        results = {"passed": True, "tables": {}}
        
        for table_name in required_tables:
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
            try:
                self.client.get_table(table_ref)
                results["tables"][table_name] = True
                logger.info(f"✅ Table exists: {table_name}")
            except Exception as e:
                results["tables"][table_name] = False
                results["passed"] = False
                logger.error(f"❌ Table missing: {table_name}")
        
        return results
    
    def _verify_data_freshness(self) -> Dict:
        """Check if data is recent (within max_data_age_hours)"""
        query = f"""
        SELECT 
            MAX(segments_date) as latest_date,
            DATE_DIFF(CURRENT_DATE(), MAX(segments_date), DAY) as days_old
        FROM `{self.project_id}.{self.dataset_id}.sp_campaign_metrics`
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            latest_date = result.latest_date
            days_old = result.days_old
            
            max_days_old = settings.max_data_age_hours / 24
            passed = days_old <= max_days_old
            
            if passed:
                logger.info(f"✅ Data is fresh: latest_date={latest_date} ({days_old} days old)")
            else:
                logger.warning(f"⚠️ Data is stale: latest_date={latest_date} ({days_old} days old)")
            
            return {
                "passed": passed,
                "latest_date": str(latest_date),
                "days_old": days_old,
                "max_days_allowed": max_days_old
            }
        except Exception as e:
            logger.error(f"❌ Data freshness check failed: {e}")
            return {"passed": False, "error": str(e)}
    
    def _verify_data_volume(self) -> Dict:
        """Check minimum data volume"""
        checks = {}
        
        # Check campaigns
        query = f"""
        SELECT COUNT(DISTINCT campaignId) as campaign_count
        FROM `{self.project_id}.{self.dataset_id}.sp_campaigns`
        WHERE state = 'ENABLED'
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            campaign_count = result.campaign_count
            checks["campaigns"] = {
                "count": campaign_count,
                "passed": campaign_count > 0
            }
            logger.info(f"Active campaigns: {campaign_count}")
        except Exception as e:
            logger.error(f"Error checking campaign count: {e}")
            checks["campaigns"] = {"passed": False, "error": str(e)}
        
        # Check keywords
        query = f"""
        SELECT COUNT(DISTINCT keywordId) as keyword_count
        FROM `{self.project_id}.{self.dataset_id}.sp_keywords`
        WHERE state = 'ENABLED'
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            keyword_count = result.keyword_count
            checks["keywords"] = {
                "count": keyword_count,
                "passed": keyword_count > 0
            }
            logger.info(f"Active keywords: {keyword_count}")
        except Exception as e:
            logger.error(f"Error checking keyword count: {e}")
            checks["keywords"] = {"passed": False, "error": str(e)}
        
        passed = all(check["passed"] for check in checks.values())
        return {"passed": passed, "checks": checks}
    
    def _verify_active_campaigns(self) -> Dict:
        """Check for campaigns with recent spend"""
        query = f"""
        SELECT 
            COUNT(DISTINCT campaignId) as active_spending_campaigns
        FROM `{self.project_id}.{self.dataset_id}.sp_campaign_metrics`
        WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            AND cost > 0
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            count = result.active_spending_campaigns
            passed = count > 0
            
            if passed:
                logger.info(f"✅ Active spending campaigns: {count}")
            else:
                logger.warning(f"⚠️ No campaigns with recent spend")
            
            return {
                "passed": passed,
                "active_spending_campaigns": count
            }
        except Exception as e:
            logger.error(f"❌ Active campaigns check failed: {e}")
            return {"passed": False, "error": str(e)}
    
    def _verify_recent_conversions(self) -> Dict:
        """Check for recent conversion data (warning only, not critical)"""
        query = f"""
        SELECT 
            SUM(purchases) as total_conversions,
            SUM(sales) as total_sales
        FROM `{self.project_id}.{self.dataset_id}.sp_campaign_metrics`
        WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            conversions = result.total_conversions or 0
            sales = result.total_sales or 0
            
            # This is a warning check, not critical
            passed = conversions > 0
            
            if passed:
                logger.info(f"✅ Recent conversions: {conversions} (${sales:.2f} sales)")
            else:
                logger.warning(f"⚠️ No conversions in last 30 days")
            
            return {
                "passed": passed,
                "total_conversions": int(conversions),
                "total_sales": float(sales),
                "warning_only": True  # Not a critical check
            }
        except Exception as e:
            logger.error(f"Error checking conversions: {e}")
            return {
                "passed": False, 
                "error": str(e),
                "warning_only": True
            }

def verify_data_or_exit():
    """
    Convenience function to run verification and exit if failed
    Use at start of automation jobs
    """
    verifier = DataVerifier()
    try:
        results = verifier.verify_all()
        return results
    except DataVerificationError as e:
        logger.error(f"Data verification failed: {e}")
        logger.error("Exiting without running automation")
        exit(1)
