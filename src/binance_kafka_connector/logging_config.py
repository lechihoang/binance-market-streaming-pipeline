"""Structured logging configuration with JSON formatter.

This module provides structured logging with JSON output for better
log aggregation and analysis in production environments.
"""

import logging
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging.
    
    Formats log records as JSON with timestamp, level, logger name,
    message, and any additional context.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON.
        
        Args:
            record: The log record to format
            
        Returns:
            JSON string representation of the log record
        """
        # Build the base log entry
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields from the record
        # These are fields added via logger.info("msg", extra={...})
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        
        # Add common contextual fields if present
        context_fields = [
            "url", "error_type", "error_details", "raw_message",
            "topic", "partition_key", "symbol", "stream_type",
            "reconnect_attempt", "delay_seconds", "batch_size",
            "message_count", "data"
        ]
        
        for field in context_fields:
            if hasattr(record, field):
                log_entry[field] = getattr(record, field)
        
        # Serialize to JSON
        return json.dumps(log_entry)


def setup_structured_logging(log_level: str = "INFO") -> None:
    """Set up structured logging with JSON formatter.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Get the root logger
    root_logger = logging.getLogger()
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Set the log level
    level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger.setLevel(level)
    
    # Create console handler with JSON formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(JSONFormatter())
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    # Log that structured logging is configured
    logger = logging.getLogger(__name__)
    logger.info("Structured logging configured", extra={
        "extra_fields": {"log_level": log_level}
    })
