"""
Structured logging for Cloud Run
Outputs JSON-formatted logs for Google Cloud Logging
"""

import logging
import json
import sys
from datetime import datetime, timezone

def get_logger(name: str) -> logging.Logger:
    """Get configured logger for Cloud Logging"""
    
    logger = logging.getLogger(name)
    
    # Only add handler if not already added (prevents duplicate logs)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        
        # Use JSON format for Cloud Logging
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                log_obj = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "severity": record.levelname,
                    "message": record.getMessage(),
                    "logger": record.name,
                    "function": record.funcName,
                    "line": record.lineno
                }
                
                if record.exc_info:
                    log_obj["exception"] = self.formatException(record.exc_info)
                
                return json.dumps(log_obj)
        
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        # Prevent logs from propagating to the root logger (avoids duplicates)
        logger.propagate = False
    
    return logger





Evaluate

Compare
