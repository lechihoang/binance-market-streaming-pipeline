"""Data models for Binance WebSocket messages.

This module defines Pydantic models for validating and parsing messages
received from Binance WebSocket API.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator


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
