"""
Central configuration for the automation system.
Uses pydantic-settings to load from environment with sane defaults.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Core identifiers (lowercase for shared modules)
    project_id: str = Field(default="amazon-ppc-474902", alias="GCP_PROJECT")
    dataset_id: str = Field(default="amazon_ppc", alias="BQ_DATASET")

    # Convenience uppercase aliases for legacy scripts
    PROJECT_ID: str = Field(default="amazon-ppc-474902", alias="GCP_PROJECT")
    BIGQUERY_DATASET: str = Field(default="amazon_ppc", alias="BQ_DATASET")

    # Behavior
    dry_run: bool = Field(default=True, alias="DRY_RUN")
    timezone: str = Field(default="America/New_York", alias="TIMEZONE")

    # Bid rules
    min_bid: float = Field(default=0.2, alias="MIN_BID")
    max_bid: float = Field(default=3.0, alias="MAX_BID")
    default_target_acos: float = Field(default=0.30, alias="DEFAULT_TARGET_ACOS")

    # Budget monitor thresholds
    budget_critical_threshold_3pm: float = Field(default=0.75, alias="BUDGET_CRIT_3PM")
    budget_warning_threshold_3pm: float = Field(default=0.65, alias="BUDGET_WARN_3PM")

    # Optional defaults
    default_aov: float = Field(default=35.0, alias="DEFAULT_AOV")

    # Harvest settings
    harvest_days_lookback: int = Field(default=30, alias="HARVEST_DAYS")
    harvest_min_clicks: int = Field(default=10, alias="HARVEST_MIN_CLICKS")
    harvest_min_orders: int = Field(default=2, alias="HARVEST_MIN_ORDERS")
    harvest_max_acos: float = Field(default=0.35, alias="HARVEST_MAX_ACOS")

    class Config:
        populate_by_name = True
        case_sensitive = False


# Single settings instance
settings = Settings()

__all__ = ["settings", "Settings"]
