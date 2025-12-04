"""
Pydantic response models for FastAPI endpoints.

Defines all request/response schemas for the Crypto Data API.
"""

from datetime import datetime
from typing import Optional, List, Any

from pydantic import BaseModel, Field


# ============================================================================
# Market Response Models
# ============================================================================

class TickerResponse(BaseModel):
    """24-hour ticker statistics for a symbol."""
    symbol: str
    price: float
    volume_24h: float = Field(alias="volume")
    price_change_24h: float = Field(default=0.0)
    high_24h: float = Field(alias="high", default=0.0)
    low_24h: float = Field(alias="low", default=0.0)
    timestamp: int

    class Config:
        populate_by_name = True


class PriceResponse(BaseModel):
    """Latest price for a symbol."""
    symbol: str
    price: float
    volume: float
    timestamp: int


class TradeResponse(BaseModel):
    """Individual trade record."""
    price: float
    quantity: float
    timestamp: int
    is_buyer_maker: bool = False
    side: str = "BUY"  # Derived from is_buyer_maker for dashboard compatibility


# ============================================================================
# Analytics Response Models
# ============================================================================

class KlineResponse(BaseModel):
    """OHLCV candlestick data."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None
    trades_count: Optional[int] = None


class IndicatorResponse(BaseModel):
    """Technical indicators for a symbol."""
    timestamp: datetime
    rsi: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    sma_20: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    atr: Optional[float] = None


class VolumeAnalysisResponse(BaseModel):
    """Volume analysis aggregation."""
    symbol: str
    interval: str
    total_volume: float
    avg_volume: float
    max_volume: float
    min_volume: float


class VolatilityResponse(BaseModel):
    """Volatility calculation result."""
    symbol: str
    period: str
    volatility: float
    std_dev: float


# ============================================================================
# Alert Response Models
# ============================================================================

class AlertResponse(BaseModel):
    """Alert notification record."""
    timestamp: datetime
    symbol: str
    alert_type: str
    severity: str
    message: Optional[str] = None
    metadata: Optional[dict] = None


# ============================================================================
# System Response Models
# ============================================================================

class ServiceHealth(BaseModel):
    """Individual service health status."""
    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """System health status."""
    status: str  # healthy, degraded, unhealthy
    redis: bool
    duckdb: bool
    kafka: bool
    timestamp: datetime
    services: Optional[List[ServiceHealth]] = None


class MetricsResponse(BaseModel):
    """Prometheus-compatible metrics."""
    request_count: int
    request_latency_avg_ms: float
    active_connections: int
    error_rate: float


class StatusResponse(BaseModel):
    """API status information."""
    version: str
    uptime_seconds: float
    environment: str = "production"


# ============================================================================
# Error Response Models
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str
    message: str
    detail: Optional[Any] = None
    error_id: Optional[str] = None


class ValidationErrorResponse(BaseModel):
    """Validation error response."""
    error: str = "validation_error"
    message: str = "Request validation failed"
    details: List[dict]


# ============================================================================
# WebSocket Message Models
# ============================================================================

class WebSocketMessage(BaseModel):
    """WebSocket message wrapper."""
    type: str  # trade, kline, ticker, heartbeat, error
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=datetime.now)
