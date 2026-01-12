import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    project_id: str = os.getenv("GCP_PROJECT", "amazon-ppc-474902")
    dataset_id: str = os.getenv("BQ_DATASET", "amazon_ppc")
    region: str = os.getenv("GCP_REGION", "us-central1")

    # Optimization settings
    default_target_acos: float = 0.30
    default_aov: float = float(os.getenv("DEFAULT_AOV", "35.0"))
    min_bid: float = 0.10
    max_bid: float = 5.00

    # Budget pacing
    budget_warning_threshold_3pm: float = 0.65
    budget_critical_threshold_3pm: float = 0.75

    # System
    max_data_age_hours: int = 48
    timezone: str = "America/New_York"
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False
    )

settings = Settings()
