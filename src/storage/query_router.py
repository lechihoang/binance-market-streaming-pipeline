"""
QueryRouter - Automatic tier selection for queries.

Routes queries to appropriate storage tier based on time range:
- < 1 hour: Redis (Hot Path)
- < 90 days: PostgreSQL (Warm Path)
- >= 90 days: MinIO (Cold Path)

Supports multi-timeframe candle aggregation (1m, 5m, 15m intervals).
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from .redis import RedisStorage
from .postgres import PostgresStorage
from .minio import MinioStorage
from src.utils.logging import get_logger

logger = get_logger(__name__)


class QueryRouter:
    """Routes queries to appropriate storage tier based on time range."""
    
    REDIS_THRESHOLD_HOURS = 1
    POSTGRES_THRESHOLD_DAYS = 90
    TIER_ORDER = ["redis", "postgres", "minio"]
    
    # Data type constants
    DATA_TYPE_KLINES = "klines"
    DATA_TYPE_ALERTS = "alerts"
    DATA_TYPE_TRADES = "trades"
    
    # Valid intervals for klines aggregation
    VALID_INTERVALS = {"1m", "5m", "15m"}
    
    def __init__(self, redis: RedisStorage, postgres: PostgresStorage, minio: MinioStorage):
        self.redis = redis
        self.postgres = postgres
        self.minio = minio
        
        # Query method mapping: tier -> data_type -> (method, needs_time_range)
        # Note: klines methods are dynamically selected based on interval in _query_tier
        self._query_map = {
            "redis": {
                "klines": (lambda s, st, en: self._get_redis_candles(s), False),
                "trades": (lambda s, st, en: self.redis.get_recent_trades(s, limit=1000), False),
                "alerts": (lambda s, st, en: self.redis.get_recent_alerts(limit=1000), False),
            },
            "postgres": {
                "klines": (lambda s, st, en: self.postgres.query_candles(s, st, en), True),
                "alerts": (lambda s, st, en: self.postgres.query_alerts(s, st, en), True),
            },
            "minio": {
                "klines": (lambda s, st, en: self.minio.read_klines(s, st, en), True),
                "alerts": (lambda s, st, en: self.minio.read_alerts(s, st, en), True),
            },
        }


    def _wrap_single(self, result: Any) -> List[Dict[str, Any]]:
        """Wrap single result in list."""
        return [result] if result else []
    
    def _get_redis_candles(self, symbol: str, interval: str = "1m") -> List[Dict[str, Any]]:
        """Get candles from Redis, with optional aggregation.
        
        Args:
            symbol: Trading pair symbol
            interval: Time interval (1m, 5m, 15m)
            
        Returns:
            List of candle dictionaries
            
        Requirements: 2.1
        """
        if interval == "1m":
            result = self.redis.get_aggregation(symbol, "1m")
            return [result] if result else []
        else:
            # Use aggregation method for higher timeframes
            return self.redis.get_aggregations_multi(symbol, interval)
    
    def _select_tier(self, start: datetime) -> str:
        """Select storage tier based on start time.
        
        Tier selection rules:
        - < 1 hour ago: Redis (Hot Path)
        - >= 1 hour and < 90 days ago: PostgreSQL (Warm Path)
        - >= 90 days ago: MinIO (Cold Path)
        
        Note: If start is naive (no timezone), it's treated as local time.
        If start is aware, it's converted to local time for comparison.
        """
        # Use local time for comparison to match how callers typically create timestamps
        now = datetime.now()
        start_local = start.replace(tzinfo=None) if start.tzinfo else start
        if start_local > now - timedelta(hours=self.REDIS_THRESHOLD_HOURS):
            return "redis"
        if start_local > now - timedelta(days=self.POSTGRES_THRESHOLD_DAYS):
            return "postgres"
        return "minio"
    
    def _query_tier(
        self, tier: str, data_type: str, symbol: str, start: datetime, end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query a specific tier.
        
        Args:
            tier: Storage tier (redis, postgres, minio)
            data_type: Type of data to query (klines, alerts, trades)
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            interval: Time interval for klines (1m, 5m, 15m)
            
        Returns:
            List of data dictionaries
            
        Requirements: 1.2, 1.3, 2.1, 2.2, 2.3
        """
        # Handle klines with interval-aware methods
        if data_type == self.DATA_TYPE_KLINES:
            return self._query_klines_tier(tier, symbol, start, end, interval)
        
        # For other data types, use the standard query map
        tier_map = self._query_map.get(tier, {})
        query_fn = tier_map.get(data_type)
        if not query_fn:
            return []
        return query_fn[0](symbol, start, end)
    
    def _query_klines_tier(
        self, tier: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query klines from a specific tier with interval support.
        
        Routes to appropriate method based on tier and interval:
        - Redis: get_aggregations_multi() for aggregated data
        - PostgreSQL: query_candles_aggregated() for SQL-based aggregation
        - MinIO: read_klines_aggregated() for Pandas-based aggregation
        
        Args:
            tier: Storage tier (redis, postgres, minio)
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            interval: Time interval (1m, 5m, 15m)
            
        Returns:
            List of candle dictionaries
            
        Requirements: 2.1, 2.2, 2.3
        """
        if tier == "redis":
            return self._get_redis_candles(symbol, interval)
        
        elif tier == "postgres":
            if interval == "1m":
                return self.postgres.query_candles(symbol, start, end)
            else:
                return self.postgres.query_candles_aggregated(symbol, start, end, interval)
        
        elif tier == "minio":
            if interval == "1m":
                return self.minio.read_klines(symbol, start, end)
            else:
                return self.minio.read_klines_aggregated(symbol, start, end, interval)
        
        return []
    
    def query(
        self, data_type: str, symbol: str, start: datetime, end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query data with automatic tier selection and fallback.
        
        Args:
            data_type: Type of data to query (klines, alerts, trades)
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            interval: Time interval for klines aggregation (1m, 5m, 15m).
                     Only applies to klines data type. Defaults to "1m".
            
        Returns:
            List of data dictionaries
            
        Requirements: 1.2, 1.3, 2.1, 2.2, 2.3
        """
        selected_tier = self._select_tier(start)
        start_idx = self.TIER_ORDER.index(selected_tier)
        
        for tier in self.TIER_ORDER[start_idx:]:
            try:
                result = self._query_tier(tier, data_type, symbol, start, end, interval)
                if result:
                    logger.debug(f"Query succeeded on {tier}: {data_type}, {symbol}, interval={interval}")
                    return result
                logger.debug(f"{tier} returned empty, trying next tier")
            except Exception as e:
                logger.warning(f"{tier} query failed: {e}, trying next tier")
        
        return []
