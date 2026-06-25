"""Re-export the canonical tenant model from shared.tenant."""

from shared.tenant import TenantConfig, tenant_from_env, build_amazon_client

__all__ = ["TenantConfig", "tenant_from_env", "build_amazon_client"]
