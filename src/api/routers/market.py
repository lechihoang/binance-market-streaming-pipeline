"""
Market Router - Real-time market data endpoints.

Provides REST API endpoints for ticker, price, and trades data
from Redis hot path storage with fallback to PostgreSQL/MinIO.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel

from src.api.dependencies import get_redis, get_postgres, get_minio
from src.api.models import TickerResponse, PriceResponse, TradeResponse
from src.storage.redis_storage import RedisStorage
from src.storage.postgres_storage import PostgresStorage
from src.storage.minio_storage import MinioStorage


logger = logging.getLogger(__name__)

router = APIRouter(tags=["market"])


class TopMoverResponse(BaseModel):
    """Response model for top movers."""
    symbol: str
    price: float
    change_percent: float
    volume: float


def query_with_fallback(
    redis: RedisStorage,
    postgres: PostgresStorage,
    minio: MinioStorage,
    query_type: str,
    symbol: str,
    **kwargs,
) -> Tuple[Any, str]:
    """Query data with fallback chain: Redis -> PostgreSQL -> MinIO.
    
    Args:
        redis: RedisStorage instance
        postgres: PostgresStorage instance
        minio: MinioStorage instance
        query_type: Type of query ('ticker', 'price', 'trades')
        symbol: Trading symbol
        **kwargs: Additional query parameters
        
    Returns:
        Tuple of (data, data_source) where data_source is 'redis', 'postgres', or 'minio'
        
    Raises:
        HTTPException 503: When all data sources fail
    """
    errors = []
    
    # Try Redis first
    try:
        if query_type == "ticker":
            data = redis.get_latest_ticker(symbol)
        elif query_type == "price":
            data = redis.get_latest_price(symbol)
        elif query_type == "trades":
            limit = kwargs.get("limit", 100)
            data = redis.get_recent_trades(symbol, limit=limit)
        else:
            data = None
            
        if data:
            return data, "redis"
    except Exception as e:
        logger.warning(f"Redis query failed for {query_type}/{symbol}: {e}")
        errors.append(f"Redis: {str(e)}")
    
    # Fallback to PostgreSQL
    try:
        now = datetime.now()
        start = now - timedelta(hours=24)  # Last 24 hours for ticker/price
        
        if query_type == "ticker":
            candles = postgres.query_candles(symbol, start, now)
            if candles:
                # Aggregate to ticker format
                latest = candles[-1]
                volumes = [c.get("volume", 0.0) for c in candles]
                highs = [c.get("high", 0.0) for c in candles]
                lows = [c.get("low", 0.0) for c in candles]
                first_close = candles[0].get("close", 0.0) if candles else 0.0
                last_close = latest.get("close", 0.0)
                price_change = last_close - first_close if first_close else 0.0
                
                data = {
                    "close": last_close,
                    "price": last_close,
                    "volume": sum(volumes),
                    "price_change_24h": price_change,
                    "high": max(highs) if highs else 0.0,
                    "low": min(lows) if lows else 0.0,
                    "timestamp": int(latest.get("timestamp", datetime.now()).timestamp() * 1000) 
                        if isinstance(latest.get("timestamp"), datetime) 
                        else int(latest.get("timestamp", 0)),
                }
                return data, "postgres"
                
        elif query_type == "price":
            candles = postgres.query_candles(symbol, start, now)
            if candles:
                latest = candles[-1]
                data = {
                    "price": latest.get("close", 0.0),
                    "volume": latest.get("volume", 0.0),
                    "timestamp": int(latest.get("timestamp", datetime.now()).timestamp() * 1000)
                        if isinstance(latest.get("timestamp"), datetime)
                        else int(latest.get("timestamp", 0)),
                }
                return data, "postgres"
                
        elif query_type == "trades":
            # PostgreSQL doesn't store individual trades, only candles
            # Skip to MinIO for trades
            pass
            
    except Exception as e:
        logger.warning(f"PostgreSQL query failed for {query_type}/{symbol}: {e}")
        errors.append(f"PostgreSQL: {str(e)}")
    
    # Fallback to MinIO
    try:
        now = datetime.now()
        start = now - timedelta(hours=24)
        
        if query_type == "ticker":
            klines = minio.read_klines(symbol, start, now)
            if klines:
                latest = klines[-1]
                volumes = [k.get("volume", 0.0) for k in klines]
                highs = [k.get("high", 0.0) for k in klines]
                lows = [k.get("low", 0.0) for k in klines]
                first_close = klines[0].get("close", 0.0) if klines else 0.0
                last_close = latest.get("close", 0.0)
                price_change = last_close - first_close if first_close else 0.0
                
                ts = latest.get("timestamp")
                if isinstance(ts, datetime):
                    ts = int(ts.timestamp() * 1000)
                elif ts is None:
                    ts = 0
                    
                data = {
                    "close": last_close,
                    "price": last_close,
                    "volume": sum(volumes),
                    "price_change_24h": price_change,
                    "high": max(highs) if highs else 0.0,
                    "low": min(lows) if lows else 0.0,
                    "timestamp": ts,
                }
                return data, "minio"
                
        elif query_type == "price":
            klines = minio.read_klines(symbol, start, now)
            if klines:
                latest = klines[-1]
                ts = latest.get("timestamp")
                if isinstance(ts, datetime):
                    ts = int(ts.timestamp() * 1000)
                elif ts is None:
                    ts = 0
                    
                data = {
                    "price": latest.get("close", 0.0),
                    "volume": latest.get("volume", 0.0),
                    "timestamp": ts,
                }
                return data, "minio"
                
        elif query_type == "trades":
            # MinIO doesn't have a read_trades method, return empty
            pass
                
    except Exception as e:
        logger.warning(f"MinIO query failed for {query_type}/{symbol}: {e}")
        errors.append(f"MinIO: {str(e)}")
    
    # All sources failed or returned no data
    return None, "none"


@router.get("/ticker/{symbol}", response_model=TickerResponse)
async def get_ticker(
    symbol: str,
    response: Response,
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> TickerResponse:
    """Get latest 24h ticker statistics for a symbol.
    
    Retrieves ticker data from Redis with fallback to PostgreSQL/MinIO.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        
    Returns:
        TickerResponse with 24h statistics
        
    Raises:
        HTTPException 404: Symbol not found
        HTTPException 503: All data sources unavailable
    """
    data, data_source = query_with_fallback(
        redis, postgres, minio, "ticker", symbol
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    # Map data to response model
    return TickerResponse(
        symbol=symbol,
        price=data.get("close", data.get("price", 0.0)),
        volume=data.get("volume", 0.0),
        price_change_24h=data.get("price_change_24h", 0.0),
        high=data.get("high", 0.0),
        low=data.get("low", 0.0),
        timestamp=int(data.get("timestamp", 0)),
    )


@router.get("/price/{symbol}", response_model=PriceResponse)
async def get_price(
    symbol: str,
    response: Response,
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> PriceResponse:
    """Get latest price for a symbol.
    
    Retrieves price data from Redis with fallback to PostgreSQL/MinIO.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        
    Returns:
        PriceResponse with current price, volume, timestamp
        
    Raises:
        HTTPException 404: Symbol not found
        HTTPException 503: All data sources unavailable
    """
    data, data_source = query_with_fallback(
        redis, postgres, minio, "price", symbol
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    return PriceResponse(
        symbol=symbol,
        price=data["price"],
        volume=data["volume"],
        timestamp=data["timestamp"],
    )


@router.get("/trades/{symbol}", response_model=List[TradeResponse])
async def get_trades(
    symbol: str,
    response: Response,
    limit: int = Query(default=100, ge=1, le=1000),
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> List[TradeResponse]:
    """Get recent trades for a symbol.
    
    Retrieves trades from Redis with fallback to MinIO.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        limit: Maximum number of trades to return (default 100, max 1000)
        
    Returns:
        List of TradeResponse, most recent first
        
    Raises:
        HTTPException 404: Symbol not found (no trades available)
        HTTPException 503: All data sources unavailable
    """
    # Enforce max limit of 1000
    effective_limit = min(limit, 1000)
    
    trades, data_source = query_with_fallback(
        redis, postgres, minio, "trades", symbol, limit=effective_limit
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if not trades:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    return [
        TradeResponse(
            price=trade.get("price", 0.0),
            quantity=trade.get("quantity", trade.get("qty", 0.0)),
            timestamp=trade.get("timestamp", 0) if not isinstance(trade.get("timestamp"), datetime) 
                else int(trade.get("timestamp").timestamp() * 1000),
            is_buyer_maker=trade.get("is_buyer_maker", False),
            side="SELL" if trade.get("is_buyer_maker", False) else "BUY",
        )
        for trade in trades
    ]


@router.get("/top-movers", response_model=List[TopMoverResponse])
async def get_top_movers(
    direction: str = Query(default="gainers", regex="^(gainers|losers)$"),
    limit: int = Query(default=10, ge=1, le=50),
    redis: RedisStorage = Depends(get_redis),
) -> List[TopMoverResponse]:
    """Get top gainers or losers.
    
    Retrieves symbols with highest price changes from Redis.
    
    Args:
        direction: 'gainers' for top gainers, 'losers' for top losers
        limit: Maximum number of results (default 10, max 50)
        
    Returns:
        List of TopMoverResponse sorted by change_percent
    """
    # Get all available symbols from Redis market keys
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
    movers = []
    
    for symbol in symbols:
        try:
            # Get data from market:{symbol} key (used by aggregation)
            data = redis.client.hgetall(f"market:{symbol}")
            if data:
                price = float(data.get("price", data.get("close", 0)))
                open_price = float(data.get("open", price))
                volume = float(data.get("volume", 0))
                
                # Calculate change percent
                if open_price > 0:
                    change_percent = ((price - open_price) / open_price) * 100
                else:
                    change_percent = 0.0
                
                movers.append(TopMoverResponse(
                    symbol=symbol,
                    price=price,
                    change_percent=round(change_percent, 4),
                    volume=volume,
                ))
        except Exception as e:
            logger.warning(f"Failed to get ticker for {symbol}: {e}")
            continue
    
    # Sort by change_percent
    if direction == "gainers":
        movers.sort(key=lambda x: x.change_percent, reverse=True)
    else:
        movers.sort(key=lambda x: x.change_percent)
    
    return movers[:limit]
