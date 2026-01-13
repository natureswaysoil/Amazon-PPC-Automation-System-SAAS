# Re-export shared modules for production package usage
from .config import settings
from .logger import get_logger
from .rules_engine import BidCalculator
from .token_manager import TokenManager, get_token_manager

__all__ = [
    "settings",
    "get_logger",
    "BidCalculator",
    "TokenManager",
    "get_token_manager",
]
