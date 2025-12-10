"""
Core module for Binance-Kafka Connector.

Contains configuration, data models, and logging setup consolidated from:
- config.py: Configuration class with environment variable loading
- models.py: Pydantic models for Binance WebSocket messages
- logging_config.py: Structured JSON logging configuration

Table of Contents:
- Configuration (line ~40)
- Data Models (line ~100)
  - TradeMessage (line ~110)
  - KlineData (line ~150)
  - KlineMessage (line ~200)
  - TickerMessage (line ~240)
  - EnrichedMessage (line ~300)
- Logging Configuration (line ~380)
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

from pydantic import BaseModel, Field, field_validator

# Import config utilities from utils module (Requirement 9.2)
from src.utils.config import get_env_str, get_env_int, get_env_list


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration class that loads settings from environment variables.
    
    Uses utils config helpers for type-safe environment variable loading (Requirement 9.2).
    """

    # Default streams for Binance WebSocket
    _DEFAULT_STREAMS = [
        "btcusdt@trade", "ethusdt@trade", "bnbusdt@trade", "solusdt@trade", "xrpusdt@trade",
        "btcusdt@kline_1m", "ethusdt@kline_1m", "bnbusdt@kline_1m", "solusdt@kline_1m", "xrpusdt@kline_1m",
        "btcusdt@ticker", "ethusdt@ticker", "bnbusdt@ticker", "solusdt@ticker", "xrpusdt@ticker",
        "adausdt@ticker", "dogeusdt@ticker", "avaxusdt@ticker", "dotusdt@ticker", "maticusdt@ticker",
        "linkusdt@ticker", "ltcusdt@ticker", "uniusdt@ticker", "atomusdt@ticker", "shibusdt@ticker",
    ]

    # WebSocket Configuration
    BINANCE_WS_URL: str = get_env_str("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
    BINANCE_STREAMS: List[str] = get_env_list("BINANCE_STREAMS", _DEFAULT_STREAMS)
    PING_INTERVAL_SECONDS: int = get_env_int("PING_INTERVAL_SECONDS", 180)
    RECONNECT_MAX_DELAY_SECONDS: int = get_env_int("RECONNECT_MAX_DELAY_SECONDS", 60)
    WS_CONNECTION_TIMEOUT: int = get_env_int("WS_CONNECTION_TIMEOUT", 10)

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_COMPRESSION: str = get_env_str("KAFKA_COMPRESSION", "snappy")
    KAFKA_ACKS: int = get_env_int("KAFKA_ACKS", 1)

    # Kafka Topics
    TOPIC_RAW_TRADES: str = get_env_str("TOPIC_RAW_TRADES", "raw_trades")
    TOPIC_RAW_KLINES: str = get_env_str("TOPIC_RAW_KLINES", "raw_klines")
    TOPIC_RAW_TICKERS: str = get_env_str("TOPIC_RAW_TICKERS", "raw_tickers")

    # Batching Configuration
    BATCH_SIZE: int = get_env_int("BATCH_SIZE", 100)
    BATCH_TIMEOUT_MS: int = get_env_int("BATCH_TIMEOUT_MS", 100)
    BATCH_CHECK_INTERVAL_MS: int = get_env_int("BATCH_CHECK_INTERVAL_MS", 10)

    # Logging Configuration
    LOG_LEVEL: str = get_env_str("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration settings.

        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not cls.BINANCE_WS_URL:
            raise ValueError("BINANCE_WS_URL is required")

        if not cls.BINANCE_STREAMS:
            raise ValueError("BINANCE_STREAMS is required")

        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")

        if cls.BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE must be positive")

        if cls.BATCH_TIMEOUT_MS <= 0:
            raise ValueError("BATCH_TIMEOUT_MS must be positive")

        if cls.PING_INTERVAL_SECONDS <= 0:
            raise ValueError("PING_INTERVAL_SECONDS must be positive")

        if cls.RECONNECT_MAX_DELAY_SECONDS <= 0:
            raise ValueError("RECONNECT_MAX_DELAY_SECONDS must be positive")


# Create a singleton instance
config = Config()


# ============================================================================
# DATA MODELS
# ============================================================================

class TradeMessage(BaseModel):
    """Model for Binance trade event messages.
    
    Represents a single trade execution on Binance.
    """
    e: str = Field(..., description="Event type")
    E: int = Field(..., description="Event time (milliseconds)")
    s: str = Field(..., description="Symbol (e.g., BTCUSDT)")
    t: int = Field(..., description="Trade ID")
    p: str = Field(..., description="Price")
    q: str = Field(..., description="Quantity")
    T: int = Field(..., description="Trade time (milliseconds)")
    
    @field_validator('e')
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate that event type is 'trade'."""
        if v != "trade":
            raise ValueError(f"Expected event type 'trade', got '{v}'")
        return v
    
    @field_validator('s')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate that symbol is non-empty."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.upper()
    
    @field_validator('p', 'q')
    @classmethod
    def validate_numeric_string(cls, v: str) -> str:
        """Validate that price and quantity are valid numeric strings."""
        try:
            float(v)
        except (ValueError, TypeError):
            raise ValueError(f"Expected numeric string, got '{v}'")
        return v


class KlineData(BaseModel):
    """Model for kline (candlestick) data within a kline message."""
    t: int = Field(..., description="Kline start time (milliseconds)")
    T: int = Field(..., description="Kline close time (milliseconds)")
    s: str = Field(..., description="Symbol")
    i: str = Field(..., description="Interval (e.g., 1m, 5m)")
    f: int = Field(..., description="First trade ID")
    L: int = Field(..., description="Last trade ID")
    o: str = Field(..., description="Open price")
    c: str = Field(..., description="Close price")
    h: str = Field(..., description="High price")
    l: str = Field(..., description="Low price")
    v: str = Field(..., description="Base asset volume")
    n: int = Field(..., description="Number of trades")
    x: bool = Field(..., description="Is this kline closed?")
    q: str = Field(..., description="Quote asset volume")
    V: str = Field(..., description="Taker buy base asset volume")
    Q: str = Field(..., description="Taker buy quote asset volume")
    
    @field_validator('s')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate that symbol is non-empty."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.upper()
    
    @field_validator('o', 'c', 'h', 'l', 'v', 'q', 'V', 'Q')
    @classmethod
    def validate_numeric_string(cls, v: str) -> str:
        """Validate that OHLCV values are valid numeric strings."""
        try:
            float(v)
        except (ValueError, TypeError):
            raise ValueError(f"Expected numeric string, got '{v}'")
        return v


class KlineMessage(BaseModel):
    """Model for Binance kline event messages.
    
    Represents candlestick data for a specific time interval.
    """
    e: str = Field(..., description="Event type")
    E: int = Field(..., description="Event time (milliseconds)")
    s: str = Field(..., description="Symbol")
    k: KlineData = Field(..., description="Kline data")
    
    @field_validator('e')
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate that event type is 'kline'."""
        if v != "kline":
            raise ValueError(f"Expected event type 'kline', got '{v}'")
        return v
    
    @field_validator('s')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate that symbol is non-empty."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.upper()


class TickerMessage(BaseModel):
    """Model for Binance 24hr ticker statistics messages.
    
    Represents 24-hour rolling window price change statistics.
    """
    e: str = Field(..., description="Event type")
    E: int = Field(..., description="Event time (milliseconds)")
    s: str = Field(..., description="Symbol")
    p: str = Field(..., description="Price change")
    P: str = Field(..., description="Price change percent")
    w: str = Field(..., description="Weighted average price")
    c: str = Field(..., description="Last price")
    Q: str = Field(..., description="Last quantity")
    o: str = Field(..., description="Open price")
    h: str = Field(..., description="High price")
    l: str = Field(..., description="Low price")
    v: str = Field(..., description="Total traded base asset volume")
    q: str = Field(..., description="Total traded quote asset volume")
    n: int = Field(..., description="Number of trades")
    
    @field_validator('e')
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate that event type is '24hrTicker'."""
        if v != "24hrTicker":
            raise ValueError(f"Expected event type '24hrTicker', got '{v}'")
        return v
    
    @field_validator('s')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate that symbol is non-empty."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.upper()
    
    @field_validator('p', 'P', 'w', 'c', 'Q', 'o', 'h', 'l', 'v', 'q')
    @classmethod
    def validate_numeric_string(cls, v: str) -> str:
        """Validate that numeric fields are valid numeric strings."""
        try:
            float(v)
        except (ValueError, TypeError):
            raise ValueError(f"Expected numeric string, got '{v}'")
        return v


class EnrichedMessage(BaseModel):
    """Model for enriched messages with metadata.
    
    Contains the original message data plus additional metadata added
    during processing.
    """
    original_data: Dict[str, Any] = Field(..., description="Original message data")
    ingestion_timestamp: int = Field(..., description="Unix timestamp in milliseconds when message was ingested")
    source: str = Field(..., description="Data source identifier")
    data_version: str = Field(..., description="Data schema version")
    symbol: str = Field(..., description="Trading pair symbol")
    stream_type: str = Field(..., description="Stream type (trade, kline, ticker)")
    topic: str = Field(..., description="Target Kafka topic")
    
    @field_validator('source')
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Validate that source is 'binance'."""
        if v != "binance":
            raise ValueError(f"Expected source 'binance', got '{v}'")
        return v
    
    @field_validator('data_version')
    @classmethod
    def validate_data_version(cls, v: str) -> str:
        """Validate that data_version is 'v1'."""
        if v != "v1":
            raise ValueError(f"Expected data_version 'v1', got '{v}'")
        return v
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate that symbol is non-empty."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.upper()
    
    @field_validator('stream_type')
    @classmethod
    def validate_stream_type(cls, v: str) -> str:
        """Validate that stream_type is one of the known types."""
        valid_types = ['trade', 'kline', 'ticker']
        if v not in valid_types:
            raise ValueError(f"Stream type must be one of {valid_types}, got '{v}'")
        return v
    
    @field_validator('topic')
    @classmethod
    def validate_topic(cls, v: str) -> str:
        """Validate that topic is one of the known topics."""
        valid_topics = ['raw_trades', 'raw_klines', 'raw_tickers']
        if v not in valid_topics:
            raise ValueError(f"Topic must be one of {valid_topics}, got '{v}'")
        return v


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

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
