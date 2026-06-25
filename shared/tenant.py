"""
Tenant model for the multi-tenant PPC system.

Single-tenant -> SaaS: every per-customer thing the system needs to act on
one seller's account is captured here. A TenantConfig carries the Amazon Ads
credentials for that seller plus their isolation keys (BigQuery dataset) and
their policy knobs (target ACoS, bid floor/ceiling).

Today these are loaded from environment / Secret Manager for a single active
tenant (back-compat). The next step toward SaaS is a `tenants` table that
yields one TenantConfig per customer; nothing else in the engine has to
change, because the optimizer already takes a TenantConfig.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from .config import settings
from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class TenantConfig:
    """Everything needed to optimize ONE seller account."""

    tenant_id: str

    # Amazon Ads credentials (per seller)
    client_id: str
    client_secret: str
    refresh_token: str
    profile_id: str
    region: str = "NA"

    # Per-tenant data isolation
    bq_project: Optional[str] = None
    bq_dataset: Optional[str] = None

    # Per-tenant policy
    target_acos: float = field(default_factory=lambda: settings.default_target_acos)
    min_bid: float = field(default_factory=lambda: settings.min_bid)
    max_bid: float = field(default_factory=lambda: settings.max_bid)
    dry_run: bool = field(default_factory=lambda: settings.dry_run)

    def masked(self) -> str:
        """Loggable identity that never leaks secrets."""
        return f"tenant={self.tenant_id} profile={self.profile_id} region={self.region}"


def tenant_from_env() -> TenantConfig:
    """
    Build the single active tenant from environment variables (back-compat
    path so existing single-account deploys keep working).

    Required env: TENANT_ID, AMAZON_CLIENT_ID, AMAZON_CLIENT_SECRET,
    AMAZON_REFRESH_TOKEN, AMAZON_PROFILE_ID.
    """
    required = {
        "TENANT_ID": os.environ.get("TENANT_ID", "default"),
        "AMAZON_CLIENT_ID": os.environ.get("AMAZON_CLIENT_ID"),
        "AMAZON_CLIENT_SECRET": os.environ.get("AMAZON_CLIENT_SECRET"),
        "AMAZON_REFRESH_TOKEN": os.environ.get("AMAZON_REFRESH_TOKEN"),
        "AMAZON_PROFILE_ID": os.environ.get("AMAZON_PROFILE_ID"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            "Missing tenant env vars: " + ", ".join(missing)
        )

    return TenantConfig(
        tenant_id=required["TENANT_ID"],
        client_id=required["AMAZON_CLIENT_ID"],
        client_secret=required["AMAZON_CLIENT_SECRET"],
        refresh_token=required["AMAZON_REFRESH_TOKEN"],
        profile_id=required["AMAZON_PROFILE_ID"],
        region=os.environ.get("AMAZON_REGION", "NA"),
        bq_project=os.environ.get("GCP_PROJECT", settings.project_id),
        bq_dataset=os.environ.get("BQ_DATASET", settings.dataset_id),
    )


def build_amazon_client(tenant: TenantConfig):
    """Factory: a credentials-bound AmazonAdsClient for this tenant."""
    from .amazon_client import AmazonAdsClient

    return AmazonAdsClient(
        client_id=tenant.client_id,
        client_secret=tenant.client_secret,
        refresh_token=tenant.refresh_token,
        profile_id=tenant.profile_id,
        region=tenant.region,
    )


__all__ = ["TenantConfig", "tenant_from_env", "build_amazon_client"]
