"""
Analytics Routes - Consolidated analytics and alerts endpoints.

Contains all analytics-related REST API endpoints:
- Historical analytics (klines, indicators, volume analysis, volatility)
- Alert notifications (recent alerts, history, whale alerts)

Table of Contents:
- Imports and Configuration (line ~20)
- Response Models (line ~60)
- Analytics Helper Functions (line ~100)
- Analytics Endpoints (line ~200)
- Alerts Endpoints (line ~600)
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel

from src.api.dependencies import get_redis, get_postgres, get_query_router, get_minio
from src.api.models import (
    KlineResponse,
    IndicatorResponse,
    VolumeAnalysisResponse,
    VolatilityResponse,
    AlertResponse,
)
from src.storage import RedisStorage, PostgresStorage, MinioStorage, QueryRouter


logger = logging.getLogger(__name__)

# Combined router for all analytics-related endpoints
router = APIRouter()

# Valid indicator names
VALID_INDICATORS = {
    "rsi", "macd", "macd_signal", "sma_20", 
    "ema_12", "ema_26", "bb_upper", "bb_lower", "atr"
}

# Valid intervals for klines
VALID_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}

# Maximum time range: 1 year
MAX_TIME_RANGE_DAYS = 365

# Valid alert types
VALID_ALERT_TYPES = {"whale", "price_spike", "volume_anomaly", "volatility"}


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class OHLCResponse(BaseModel):
    """OHLC candlestick data for charts."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


class VolumeHeatmapResponse(BaseModel):
    """Volume heatmap data point."""
    hour: int
    symbol: str
    volume: float


class VolatilityComparisonResponse(BaseModel):
    """Volatility comparison data for multiple symbols."""
    symbol: str
    volatility: float
    std_dev: float
    period: str


class WhaleAlertResponse(BaseModel):
    """Response model for whale alerts."""
    timestamp: datetime
    symbol: str
    side: str
    amount: float
    price: float
    total_value: float


# ============================================================================
# ANALYTICS HELPER FUNCTIONS
# ============================================================================

def validate_time_range(start: datetime, end: datetime) -> None:
    """Validate that time range does not exceed 1 year.
    
    Args:
        start: Start datetime
        end: End datetime
        
    Raises:
        HTTPException 400: If time range exceeds 1 year
    """
    if (end - start).days > MAX_TIME_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail="Time range exceeds maximum allowed (1 year)"
        )


def query_klines_with_fallback(
    query_router: QueryRouter,
    minio: MinioStorage,
    symbol: str,
    start: datetime,
    end: datetime,
) -> Tuple[List[dict], str]:
    """Query klines with fallback chain.
    
    Args:
        query_router: QueryRouter instance (handles Redis -> PostgreSQL)
        minio: MinioStorage instance for final fallback
        symbol: Trading symbol
        start: Start datetime
        end: End datetime
        
    Returns:
        Tuple of (data, data_source)
    """
    # Determine which tier QueryRouter will use
    now = datetime.now()
    redis_cutoff = now - timedelta(hours=1)
    postgres_cutoff = now - timedelta(days=90)
    
    if start >= redis_cutoff:
        primary_tier = "redis"
    elif start >= postgres_cutoff:
        primary_tier = "postgres"
    else:
        primary_tier = "minio"
    
    try:
        data = query_router.query(
            data_type=QueryRouter.DATA_TYPE_KLINES,
            symbol=symbol,
            start=start,
            end=end,
        )
        if data:
            return data, primary_tier
    except Exception as e:
        logger.warning(f"QueryRouter failed for klines/{symbol}: {e}")
    
    # Final fallback to MinIO
    try:
        data = minio.read_klines(symbol, start, end)
        if data:
            return data, "minio"
    except Exception as e:
        logger.warning(f"MinIO fallback failed for klines/{symbol}: {e}")
    
    return [], "none"


def query_indicators_with_fallback(
    postgres: PostgresStorage,
    minio: MinioStorage,
    symbol: str,
    start: datetime,
    end: datetime,
) -> Tuple[List[dict], str]:
    """Query indicators with fallback: PostgreSQL -> MinIO.
    
    Args:
        postgres: PostgresStorage instance
        minio: MinioStorage instance
        symbol: Trading symbol
        start: Start datetime
        end: End datetime
        
    Returns:
        Tuple of (data, data_source)
    """
    # Try PostgreSQL first
    try:
        data = postgres.query_indicators(symbol, start, end)
        if data:
            return data, "postgres"
    except Exception as e:
        logger.warning(f"PostgreSQL query failed for indicators/{symbol}: {e}")
    
    # Fallback to MinIO
    try:
        data = minio.read_indicators(symbol, start, end)
        if data:
            return data, "minio"
    except Exception as e:
        logger.warning(f"MinIO fallback failed for indicators/{symbol}: {e}")
    
    return [], "none"


def query_candles_with_fallback(
    postgres: PostgresStorage,
    minio: MinioStorage,
    symbol: str,
    start: datetime,
    end: datetime,
) -> Tuple[List[dict], str]:
    """Query candles with fallback: PostgreSQL -> MinIO.
    
    Args:
        postgres: PostgresStorage instance
        minio: MinioStorage instance
        symbol: Trading symbol
        start: Start datetime
        end: End datetime
        
    Returns:
        Tuple of (data, data_source)
    """
    # Try PostgreSQL first
    try:
        data = postgres.query_candles(symbol, start, end)
        if data:
            return data, "postgres"
    except Exception as e:
        logger.warning(f"PostgreSQL query failed for candles/{symbol}: {e}")
    
    # Fallback to MinIO
    try:
        data = minio.read_klines(symbol, start, end)
        if data:
            return data, "minio"
    except Exception as e:
        logger.warning(f"MinIO fallback failed for candles/{symbol}: {e}")
    
    return [], "none"


# ============================================================================
# ANALYTICS ENDPOINTS
# ============================================================================

@router.get("/klines/{symbol}", response_model=List[KlineResponse], tags=["analytics"])
async def get_klines(
    symbol: str,
    response: Response,
    interval: str = Query(default="1m", description="Candle interval"),
    start: Optional[datetime] = Query(default=None, description="Start time"),
    end: Optional[datetime] = Query(default=None, description="End time"),
    query_router: QueryRouter = Depends(get_query_router),
    minio: MinioStorage = Depends(get_minio),
) -> List[KlineResponse]:
    """Get OHLCV klines for a symbol.
    
    Routes query based on time range with fallback:
    - < 1 hour: Redis -> PostgreSQL -> MinIO
    - < 90 days: PostgreSQL -> MinIO
    - >= 90 days: MinIO
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Candle interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        start: Start datetime (default: 24 hours ago)
        end: End datetime (default: now)
        
    Returns:
        List of KlineResponse with OHLCV data
        
    Raises:
        HTTPException 400: Invalid parameters or time range exceeds 1 year
        HTTPException 503: All data sources unavailable
    """
    # Validate interval
    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_INTERVALS))}"
        )
    
    # Default time range: last 24 hours
    now = datetime.now()
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)
    
    # Validate time range
    validate_time_range(start, end)
    
    # Query with fallback
    data, data_source = query_klines_with_fallback(
        query_router, minio, symbol, start, end
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    return [
        KlineResponse(
            timestamp=record.get("timestamp"),
            open=record.get("open", 0.0),
            high=record.get("high", 0.0),
            low=record.get("low", 0.0),
            close=record.get("close", 0.0),
            volume=record.get("volume", 0.0),
            quote_volume=record.get("quote_volume"),
            trades_count=record.get("trades_count"),
        )
        for record in data
    ]


@router.get("/indicators/latest/{symbol}", response_model=List[IndicatorResponse], tags=["analytics"])
async def get_indicators_latest(
    symbol: str,
    response: Response,
    limit: int = Query(default=100, ge=1, le=1000, description="Number of records"),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[IndicatorResponse]:
    """Get latest technical indicators available in the system.
    
    Returns the most recent indicators regardless of time range.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        limit: Maximum number of records to return
        
    Returns:
        List of IndicatorResponse with indicator data
    """
    try:
        now = datetime.now()
        start = now - timedelta(days=90)
        data = postgres.query_indicators(symbol, start, now)
        
        if data:
            data = data[-limit:]
            response.headers["X-Data-Source"] = "postgres"
            return [
                IndicatorResponse(
                    timestamp=record.get("timestamp"),
                    rsi=record.get("rsi"),
                    macd=record.get("macd"),
                    macd_signal=record.get("macd_signal"),
                    sma_20=record.get("sma_20"),
                    ema_12=record.get("ema_12"),
                    ema_26=record.get("ema_26"),
                    bb_upper=record.get("bb_upper"),
                    bb_lower=record.get("bb_lower"),
                    atr=record.get("atr"),
                )
                for record in data
            ]
    except Exception as e:
        logger.warning(f"PostgreSQL query failed: {e}")
    
    response.headers["X-Data-Source"] = "none"
    return []


@router.get("/indicators/{symbol}", response_model=List[IndicatorResponse], tags=["analytics"])
async def get_indicators(
    symbol: str,
    response: Response,
    indicators: Optional[str] = Query(
        default=None, 
        description="Comma-separated indicator names (rsi,macd,sma_20,etc.)"
    ),
    period: str = Query(default="24h", description="Time period (1h, 24h, 7d, 30d)"),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> List[IndicatorResponse]:
    """Get technical indicators for a symbol.
    
    Available indicators: rsi, macd, macd_signal, sma_20, ema_12, ema_26,
    bb_upper, bb_lower, atr
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        indicators: Comma-separated list of indicator names to return
        period: Time period (1h, 24h, 7d, 30d)
        
    Returns:
        List of IndicatorResponse with requested indicators
        
    Raises:
        HTTPException 400: Invalid indicator name or period
        HTTPException 503: All data sources unavailable
    """
    # Parse period to time range
    now = datetime.now()
    period_map = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
        "90d": timedelta(days=90),
    }
    
    if period not in period_map:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Valid values: {', '.join(period_map.keys())}"
        )
    
    start = now - period_map[period]
    end = now
    
    # Validate time range
    validate_time_range(start, end)
    
    # Parse requested indicators
    requested_indicators = None
    if indicators:
        requested_indicators = set(ind.strip().lower() for ind in indicators.split(","))
        invalid = requested_indicators - VALID_INDICATORS
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid indicators: {', '.join(invalid)}. Valid values: {', '.join(sorted(VALID_INDICATORS))}"
            )
    
    # Query with fallback
    data, data_source = query_indicators_with_fallback(
        postgres, minio, symbol, start, end
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    # Filter to requested indicators only
    results = []
    for record in data:
        response_model = IndicatorResponse(timestamp=record.get("timestamp"))
        
        # If specific indicators requested, only include those
        if requested_indicators:
            for ind in requested_indicators:
                setattr(response_model, ind, record.get(ind))
        else:
            # Include all indicators
            response_model.rsi = record.get("rsi")
            response_model.macd = record.get("macd")
            response_model.macd_signal = record.get("macd_signal")
            response_model.sma_20 = record.get("sma_20")
            response_model.ema_12 = record.get("ema_12")
            response_model.ema_26 = record.get("ema_26")
            response_model.bb_upper = record.get("bb_upper")
            response_model.bb_lower = record.get("bb_lower")
            response_model.atr = record.get("atr")
        
        results.append(response_model)
    
    return results


@router.get("/volume-analysis", response_model=List[VolumeAnalysisResponse], tags=["analytics"])
async def get_volume_analysis(
    response: Response,
    symbols: str = Query(description="Comma-separated symbol list"),
    interval: str = Query(default="1h", description="Aggregation interval"),
    start: Optional[datetime] = Query(default=None, description="Start time"),
    end: Optional[datetime] = Query(default=None, description="End time"),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> List[VolumeAnalysisResponse]:
    """Get aggregated volume analysis for symbols.
    
    Args:
        symbols: Comma-separated list of trading pair symbols
        interval: Aggregation interval (1h, 4h, 1d)
        start: Start datetime (default: 24 hours ago)
        end: End datetime (default: now)
        
    Returns:
        List of VolumeAnalysisResponse with volume statistics
        
    Raises:
        HTTPException 400: Invalid parameters or time range exceeds 1 year
        HTTPException 503: All data sources unavailable
    """
    # Parse symbols
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(
            status_code=400,
            detail="At least one symbol is required"
        )
    
    # Validate interval
    valid_intervals = {"1h", "4h", "1d"}
    if interval not in valid_intervals:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Valid values: {', '.join(sorted(valid_intervals))}"
        )
    
    # Default time range: last 24 hours
    now = datetime.now()
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)
    
    # Validate time range
    validate_time_range(start, end)
    
    results = []
    data_sources = set()
    
    for symbol in symbol_list:
        # Query candles with fallback
        candles, data_source = query_candles_with_fallback(
            postgres, minio, symbol, start, end
        )
        data_sources.add(data_source)
        
        if candles:
            volumes = [c.get("volume", 0.0) for c in candles]
            results.append(
                VolumeAnalysisResponse(
                    symbol=symbol,
                    interval=interval,
                    total_volume=sum(volumes),
                    avg_volume=sum(volumes) / len(volumes) if volumes else 0.0,
                    max_volume=max(volumes) if volumes else 0.0,
                    min_volume=min(volumes) if volumes else 0.0,
                )
            )
        else:
            # Return zero values if no data
            results.append(
                VolumeAnalysisResponse(
                    symbol=symbol,
                    interval=interval,
                    total_volume=0.0,
                    avg_volume=0.0,
                    max_volume=0.0,
                    min_volume=0.0,
                )
            )
    
    # Add X-Data-Source header (use primary source or mixed if multiple)
    if len(data_sources) == 1:
        response.headers["X-Data-Source"] = data_sources.pop()
    else:
        response.headers["X-Data-Source"] = "mixed"
    
    return results


@router.get("/volatility/{symbol}", response_model=VolatilityResponse, tags=["analytics"])
async def get_volatility(
    symbol: str,
    response: Response,
    period: str = Query(default="24h", description="Time period (1h, 24h, 7d, 30d)"),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> VolatilityResponse:
    """Get volatility (standard deviation) for a symbol.
    
    Calculates price volatility based on close prices over the specified period.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        period: Time period (1h, 24h, 7d, 30d)
        
    Returns:
        VolatilityResponse with volatility and standard deviation
        
    Raises:
        HTTPException 400: Invalid period
        HTTPException 404: No data available for symbol
        HTTPException 503: All data sources unavailable
    """
    # Parse period to time range
    now = datetime.now()
    period_map = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
        "90d": timedelta(days=90),
    }
    
    if period not in period_map:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Valid values: {', '.join(period_map.keys())}"
        )
    
    start = now - period_map[period]
    end = now
    
    # Query candles with fallback
    candles, data_source = query_candles_with_fallback(
        postgres, minio, symbol, start, end
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if not candles:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for symbol: {symbol}"
        )
    
    # Calculate standard deviation of close prices
    close_prices = [c.get("close", 0.0) for c in candles if c.get("close") is not None]
    
    if len(close_prices) < 2:
        return VolatilityResponse(
            symbol=symbol,
            period=period,
            volatility=0.0,
            std_dev=0.0,
        )
    
    # Calculate mean
    mean = sum(close_prices) / len(close_prices)
    
    # Calculate variance and standard deviation
    variance = sum((p - mean) ** 2 for p in close_prices) / len(close_prices)
    std_dev = variance ** 0.5
    
    # Volatility as percentage of mean
    volatility = (std_dev / mean * 100) if mean != 0 else 0.0
    
    return VolatilityResponse(
        symbol=symbol,
        period=period,
        volatility=volatility,
        std_dev=std_dev,
    )


@router.get("/ohlc/latest", response_model=List[OHLCResponse], tags=["analytics"])
async def get_ohlc_latest(
    response: Response,
    symbol: str = Query(description="Trading pair symbol"),
    interval: str = Query(default="1m", description="Candle interval (1m, 5m, 15m, 1h)"),
    limit: int = Query(default=100, ge=1, le=1000, description="Number of records"),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[OHLCResponse]:
    """Get latest OHLC candlestick data available in the system.
    
    Returns the most recent candles regardless of time range.
    Useful when real-time data may not be available.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Candle interval (1m, 5m, 15m, 1h)
        limit: Maximum number of records to return
        
    Returns:
        List of OHLCResponse with candlestick data
    """
    # Validate interval
    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_INTERVALS))}"
        )
    
    # Query latest candles directly from PostgreSQL
    try:
        # Query last 90 days to get any available data
        now = datetime.now()
        start = now - timedelta(days=90)
        candles = postgres.query_candles(symbol, start, now, interval=interval)
        
        if candles:
            # Return latest records up to limit
            candles = candles[-limit:]
            response.headers["X-Data-Source"] = "postgres"
            return [
                OHLCResponse(
                    timestamp=record.get("timestamp", datetime.now()),
                    open=record.get("open", 0.0),
                    high=record.get("high", 0.0),
                    low=record.get("low", 0.0),
                    close=record.get("close", 0.0),
                )
                for record in candles
            ]
    except Exception as e:
        logger.warning(f"PostgreSQL query failed: {e}")
    
    response.headers["X-Data-Source"] = "none"
    return []


@router.get("/ohlc", response_model=List[OHLCResponse], tags=["analytics"])
async def get_ohlc(
    response: Response,
    symbol: str = Query(description="Trading pair symbol"),
    interval: str = Query(default="1m", description="Candle interval"),
    start: Optional[datetime] = Query(default=None, description="Start time"),
    end: Optional[datetime] = Query(default=None, description="End time"),
    query_router: QueryRouter = Depends(get_query_router),
    minio: MinioStorage = Depends(get_minio),
) -> List[OHLCResponse]:
    """Get OHLC candlestick data for charting.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Candle interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        start: Start datetime (default: 6 hours ago)
        end: End datetime (default: now)
        
    Returns:
        List of OHLCResponse with candlestick data
    """
    # Validate interval
    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_INTERVALS))}"
        )
    
    # Default time range: last 6 hours
    now = datetime.now()
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=6)
    
    # Validate time range
    validate_time_range(start, end)
    
    # Query candles using QueryRouter (same as /klines)
    candles, data_source = query_klines_with_fallback(
        query_router, minio, symbol, start, end
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    return [
        OHLCResponse(
            timestamp=record.get("timestamp", datetime.now()),
            open=record.get("open", 0.0),
            high=record.get("high", 0.0),
            low=record.get("low", 0.0),
            close=record.get("close", 0.0),
        )
        for record in candles
    ]


@router.get("/volume-heatmap", response_model=List[VolumeHeatmapResponse], tags=["analytics"])
async def get_volume_heatmap(
    response: Response,
    symbols: str = Query(default="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT", description="Comma-separated symbols"),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> List[VolumeHeatmapResponse]:
    """Get volume heatmap data by hour and symbol.
    
    Returns volume aggregated by hour of day for the last 24 hours.
    
    Args:
        symbols: Comma-separated list of trading pair symbols
        
    Returns:
        List of VolumeHeatmapResponse with hourly volume data
    """
    # Parse symbols
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        symbol_list = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
    
    # Get last 24 hours of data
    now = datetime.now()
    start = now - timedelta(hours=24)
    
    results = []
    data_sources = set()
    
    for symbol in symbol_list:
        # Query candles with fallback
        candles, data_source = query_candles_with_fallback(
            postgres, minio, symbol, start, now
        )
        data_sources.add(data_source)
        
        # Aggregate by hour
        hourly_volumes = {}
        for candle in candles:
            ts = candle.get("timestamp")
            if isinstance(ts, datetime):
                hour = ts.hour
            else:
                continue
            
            volume = candle.get("volume", 0.0)
            if hour not in hourly_volumes:
                hourly_volumes[hour] = 0.0
            hourly_volumes[hour] += volume
        
        # Add results for each hour
        for hour in range(24):
            results.append(VolumeHeatmapResponse(
                hour=hour,
                symbol=symbol,
                volume=hourly_volumes.get(hour, 0.0),
            ))
    
    # Add X-Data-Source header
    if len(data_sources) == 1:
        response.headers["X-Data-Source"] = data_sources.pop()
    else:
        response.headers["X-Data-Source"] = "mixed"
    
    return results


@router.get("/volatility-comparison", response_model=List[VolatilityComparisonResponse], tags=["analytics"])
async def get_volatility_comparison(
    response: Response,
    symbols: str = Query(default="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT", description="Comma-separated symbols"),
    period: str = Query(default="24h", description="Time period (1h, 24h, 7d, 30d)"),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> List[VolatilityComparisonResponse]:
    """Get volatility comparison for multiple symbols.
    
    Args:
        symbols: Comma-separated list of trading pair symbols
        period: Time period (1h, 24h, 7d, 30d)
        
    Returns:
        List of VolatilityComparisonResponse with volatility data for each symbol
    """
    # Parse symbols
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        symbol_list = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
    
    # Parse period to time range
    now = datetime.now()
    period_map = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
        "90d": timedelta(days=90),
    }
    
    if period not in period_map:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Valid values: {', '.join(period_map.keys())}"
        )
    
    start = now - period_map[period]
    end = now
    
    results = []
    data_sources = set()
    
    for symbol in symbol_list:
        # Query candles with fallback
        candles, data_source = query_candles_with_fallback(
            postgres, minio, symbol, start, end
        )
        data_sources.add(data_source)
        
        if candles:
            # Calculate standard deviation of close prices
            close_prices = [c.get("close", 0.0) for c in candles if c.get("close") is not None]
            
            if len(close_prices) >= 2:
                mean = sum(close_prices) / len(close_prices)
                variance = sum((p - mean) ** 2 for p in close_prices) / len(close_prices)
                std_dev = variance ** 0.5
                volatility = (std_dev / mean * 100) if mean != 0 else 0.0
            else:
                volatility = 0.0
                std_dev = 0.0
        else:
            volatility = 0.0
            std_dev = 0.0
        
        results.append(VolatilityComparisonResponse(
            symbol=symbol,
            volatility=round(volatility, 4),
            std_dev=round(std_dev, 4),
            period=period,
        ))
    
    # Add X-Data-Source header
    if len(data_sources) == 1:
        response.headers["X-Data-Source"] = data_sources.pop()
    else:
        response.headers["X-Data-Source"] = "mixed"
    
    return results


# ============================================================================
# ALERTS ENDPOINTS
# ============================================================================

@router.get("/alerts/recent", response_model=List[AlertResponse], tags=["alerts"])
async def get_recent_alerts(
    limit: int = Query(default=100, ge=1, le=1000),
    redis: RedisStorage = Depends(get_redis),
) -> List[AlertResponse]:
    """Get recent alerts from Redis.
    
    Retrieves alerts from Redis list `alerts:recent`.
    
    Args:
        limit: Maximum number of alerts to return (default 100, max 1000)
        
    Returns:
        List of AlertResponse, most recent first
    """
    # Enforce max limit of 1000
    effective_limit = min(limit, 1000)
    
    alerts = redis.get_recent_alerts(limit=effective_limit)
    
    return [
        AlertResponse(
            timestamp=datetime.fromtimestamp(alert.get("timestamp", 0) / 1000)
            if isinstance(alert.get("timestamp"), (int, float)) and alert.get("timestamp") > 1e10
            else datetime.fromtimestamp(alert.get("timestamp", 0))
            if isinstance(alert.get("timestamp"), (int, float))
            else alert.get("timestamp", datetime.now()),
            symbol=alert.get("symbol", "UNKNOWN"),
            alert_type=alert.get("alert_type", "unknown"),
            severity=alert.get("severity", "info"),
            message=alert.get("message"),
            metadata=alert.get("metadata"),
        )
        for alert in alerts
    ]


@router.get("/alerts/history", response_model=List[AlertResponse], tags=["alerts"])
async def get_alert_history(
    start: datetime = Query(description="Start datetime"),
    end: datetime = Query(description="End datetime"),
    type: Optional[str] = Query(default=None, description="Alert type filter"),
    symbol: Optional[str] = Query(default=None, description="Symbol filter"),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[AlertResponse]:
    """Get historical alerts from PostgreSQL.
    
    Retrieves alerts from PostgreSQL alerts table filtered by time range and optional type.
    
    Args:
        start: Start datetime
        end: End datetime
        type: Optional alert type filter (whale, price_spike, volume_anomaly, volatility)
        symbol: Optional symbol filter
        
    Returns:
        List of AlertResponse matching filters
        
    Raises:
        HTTPException 400: Invalid alert type
    """
    # Validate alert type if provided
    if type is not None and type not in VALID_ALERT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid alert type. Valid values: {', '.join(sorted(VALID_ALERT_TYPES))}"
        )
    
    # Query alerts from PostgreSQL using the query_alerts method
    # If no symbol provided, use a wildcard symbol to get all
    query_symbol = symbol if symbol else "BTCUSDT"  # Default symbol for query
    
    # Get alerts from PostgreSQL
    alerts_data = postgres.query_alerts(query_symbol, start, end)
    
    # Filter by type if provided
    if type is not None:
        alerts_data = [a for a in alerts_data if a.get('alert_type') == type]
    
    # If no symbol filter was provided, we need to query for all symbols
    # For now, return what we have (the API may need enhancement for multi-symbol queries)
    
    alerts = []
    for alert_dict in alerts_data:
        alerts.append(AlertResponse(
            timestamp=alert_dict['timestamp'],
            symbol=alert_dict['symbol'],
            alert_type=alert_dict['alert_type'],
            severity=alert_dict['severity'],
            message=alert_dict.get('message'),
            metadata=alert_dict.get('metadata'),
        ))
    
    return alerts


@router.get("/alerts/whale-alerts", response_model=List[WhaleAlertResponse], tags=["alerts"])
async def get_whale_alerts(
    limit: int = Query(default=50, ge=1, le=500),
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[WhaleAlertResponse]:
    """Get whale alerts (large trades).
    
    Retrieves whale alerts from Redis (recent) and PostgreSQL (historical).
    
    Args:
        limit: Maximum number of alerts to return (default 50, max 500)
        
    Returns:
        List of WhaleAlertResponse, most recent first
    """
    import json as json_module
    
    effective_limit = min(limit, 500)
    whale_alerts = []
    
    # Try Redis first for recent alerts
    try:
        alerts = redis.get_recent_alerts(limit=effective_limit * 2)
        for alert in alerts:
            if alert.get("alert_type", "").upper() in ("WHALE", "WHALE_ALERT"):
                # Use 'details' field (from anomaly_detection_job) with fallback to 'metadata'
                details = alert.get("details", alert.get("metadata", {}))
                if isinstance(details, str):
                    try:
                        details = json_module.loads(details)
                    except:
                        details = {}
                
                ts = alert.get("timestamp", 0)
                if isinstance(ts, (int, float)):
                    if ts > 1e10:
                        ts = datetime.fromtimestamp(ts / 1000)
                    else:
                        ts = datetime.fromtimestamp(ts)
                elif isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except:
                        ts = datetime.now()
                elif not isinstance(ts, datetime):
                    ts = datetime.now()
                
                whale_alerts.append(WhaleAlertResponse(
                    timestamp=ts,
                    symbol=alert.get("symbol", "UNKNOWN"),
                    side=details.get("side", "BUY"),
                    amount=float(details.get("quantity", details.get("amount", 0))),
                    price=float(details.get("price", 0)),
                    total_value=float(details.get("value", details.get("total_value", 0))),
                ))
    except Exception:
        pass
    
    # If not enough from Redis, try PostgreSQL
    if len(whale_alerts) < effective_limit:
        try:
            now = datetime.now()
            start = now - timedelta(days=7)
            
            for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]:
                pg_alerts = postgres.query_alerts(symbol, start, now)
                for alert in pg_alerts:
                    if alert.get("alert_type", "").upper() in ("WHALE", "WHALE_ALERT"):
                        # Use 'metadata' field from PostgreSQL (mapped from 'details' in storage_writer)
                        details = alert.get("metadata", alert.get("details", {}))
                        if isinstance(details, str):
                            try:
                                details = json_module.loads(details)
                            except:
                                details = {}
                        
                        whale_alerts.append(WhaleAlertResponse(
                            timestamp=alert.get("timestamp", datetime.now()),
                            symbol=alert.get("symbol", "UNKNOWN"),
                            side=details.get("side", "BUY"),
                            amount=float(details.get("quantity", details.get("amount", 0))),
                            price=float(details.get("price", 0)),
                            total_value=float(details.get("value", details.get("total_value", 0))),
                        ))
        except Exception:
            pass
    
    # Sort by timestamp descending and limit
    whale_alerts.sort(key=lambda x: x.timestamp, reverse=True)
    return whale_alerts[:effective_limit]
