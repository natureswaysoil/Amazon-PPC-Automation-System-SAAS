"""
Structured logging for Cloud Run
"""

import logging
import json
from datetime import datetime
from .config import settings

def get_logger(name: str) -> logging.Logger:
    """Get configured logger for Cloud Logging"""
    
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        
        # Use JSON format for Cloud Logging
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                log_obj = {
                    "timestamp": datetime.utcnow().isoformat(),
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
    
    return logger
