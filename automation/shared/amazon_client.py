"""Re-export the canonical multi-tenant Amazon Ads client.

The single implementation lives in ``shared/amazon_client.py``. This shim
keeps the ``automation.shared`` import path working for the job scripts.
"""

from shared.amazon_client import AmazonAdsClient, safe_serialize

__all__ = ["AmazonAdsClient", "safe_serialize"]
