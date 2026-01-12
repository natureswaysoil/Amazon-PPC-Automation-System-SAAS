import shared.token_manager as _tm

TokenManager = _tm.TokenManager
get_token_manager = _tm.get_token_manager

# Expose the same `secretmanager` symbol for patching in tests
secretmanager = _tm.secretmanager

__all__ = ["TokenManager", "get_token_manager", "secretmanager"]
