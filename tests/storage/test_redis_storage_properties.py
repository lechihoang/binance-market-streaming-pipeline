"""
Property-based tests for RedisStorage.

Feature: three-tier-storage
Tests correctness properties for Redis hot path storage.
"""

import pytest
from hypothesis import given, settings, strategies as st, HealthCheck

from src.storage.redis_storage import RedisStorage


# =============================================================================
# Redis availability check
# =============================================================================

def is_redis_available():
    """Check if Redis is available for testing."""
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379, db=15, socket_connect_timeout=1)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


REDIS_AVAILABLE = is_redis_available()
skip_if_no_redis = pytest.mark.skipif(
    not REDIS_AVAILABLE,
    reason="Redis server not available at localhost:6379"
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def redis_storage():
    """Create RedisStorage instance and clean up after test."""
    storage = RedisStorage(host="localhost", port=6379, db=15)  # Use db=15 for tests
    storage.flush_db()
    yield storage
    storage.flush_db()


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Symbol strategy - realistic crypto trading pairs
symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"
])

# Price strategy - positive floats with reasonable precision
price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)

# Volume strategy - positive floats
volume_strategy = st.floats(min_value=0.0, max_value=1000000000.0, allow_nan=False, allow_infinity=False)

# Timestamp strategy - realistic Unix timestamps in milliseconds
timestamp_strategy = st.integers(min_value=1600000000000, max_value=2000000000000)

# Interval strategy for aggregations
interval_strategy = st.sampled_from(["1m", "5m", "15m", "1h", "4h", "1d"])


# Ticker stats strategy
ticker_stats_strategy = st.fixed_dictionaries({
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
})

# Indicators strategy
indicators_strategy = st.fixed_dictionaries({
    "rsi": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    "macd": st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
    "macd_signal": st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
    "sma_20": price_strategy,
    "ema_12": price_strategy,
    "ema_26": price_strategy,
    "bb_upper": price_strategy,
    "bb_lower": price_strategy,
    "atr": st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
})

# OHLCV strategy for aggregations
ohlcv_strategy = st.fixed_dictionaries({
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "timestamp": timestamp_strategy,
})

# Trade strategy
trade_strategy = st.fixed_dictionaries({
    "price": price_strategy,
    "quantity": volume_strategy,
    "timestamp": timestamp_strategy,
    "is_buyer_maker": st.booleans(),
})

# Alert strategy
alert_strategy = st.fixed_dictionaries({
    "timestamp": timestamp_strategy,
    "symbol": symbol_strategy,
    "alert_type": st.sampled_from(["price_spike", "volume_spike", "whale_alert", "volatility"]),
    "severity": st.sampled_from(["low", "medium", "high", "critical"]),
    "message": st.text(min_size=1, max_size=200),
})


# =============================================================================
# Property 1: Redis hash round-trip consistency
# Feature: three-tier-storage, Property 1: Redis hash round-trip consistency
# Validates: Requirements 1.1, 1.2, 1.4, 1.6
# =============================================================================

@skip_if_no_redis
class TestRedisHashRoundTrip:
    """Property tests for Redis hash round-trip consistency."""
    
    @given(symbol=symbol_strategy, price=price_strategy, volume=volume_strategy, timestamp=timestamp_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_latest_price_round_trip(self, redis_storage, symbol, price, volume, timestamp):
        """
        Feature: three-tier-storage, Property 1: Redis hash round-trip consistency
        Validates: Requirements 1.1
        
        For any valid price data, writing to Redis hash and reading back
        should return equivalent values.
        """
        # Write
        redis_storage.write_latest_price(symbol, price, volume, timestamp)
        
        # Read
        result = redis_storage.get_latest_price(symbol)
        
        # Verify round-trip
        assert result is not None
        assert abs(result["price"] - price) < 1e-9
        assert abs(result["volume"] - volume) < 1e-9
        assert result["timestamp"] == timestamp

    @given(symbol=symbol_strategy, stats=ticker_stats_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_latest_ticker_round_trip(self, redis_storage, symbol, stats):
        """
        Feature: three-tier-storage, Property 1: Redis hash round-trip consistency
        Validates: Requirements 1.2
        
        For any valid ticker stats, writing to Redis hash and reading back
        should return equivalent values.
        """
        # Write
        redis_storage.write_latest_ticker(symbol, stats)
        
        # Read
        result = redis_storage.get_latest_ticker(symbol)
        
        # Verify round-trip
        assert result is not None
        for key in stats:
            assert abs(result[key] - stats[key]) < 1e-9
    
    @given(symbol=symbol_strategy, indicators=indicators_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_indicators_round_trip(self, redis_storage, symbol, indicators):
        """
        Feature: three-tier-storage, Property 1: Redis hash round-trip consistency
        Validates: Requirements 1.4
        
        For any valid indicators, writing to Redis hash and reading back
        should return equivalent values.
        """
        # Write
        redis_storage.write_indicators(symbol, indicators)
        
        # Read
        result = redis_storage.get_indicators(symbol)
        
        # Verify round-trip
        assert result is not None
        for key in indicators:
            assert abs(result[key] - indicators[key]) < 1e-9
    
    @given(symbol=symbol_strategy, interval=interval_strategy, ohlcv=ohlcv_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_aggregation_round_trip(self, redis_storage, symbol, interval, ohlcv):
        """
        Feature: three-tier-storage, Property 1: Redis hash round-trip consistency
        Validates: Requirements 1.6
        
        For any valid OHLCV aggregation, writing to Redis hash and reading back
        should return equivalent values.
        """
        # Write
        redis_storage.write_aggregation(symbol, interval, ohlcv)
        
        # Read
        result = redis_storage.get_aggregation(symbol, interval)
        
        # Verify round-trip
        assert result is not None
        for key in ohlcv:
            if key == "timestamp":
                assert result[key] == ohlcv[key]
            else:
                assert abs(result[key] - ohlcv[key]) < 1e-9


# =============================================================================
# Property 2: Redis collection size limit invariant
# Feature: three-tier-storage, Property 2: Redis collection size limit invariant
# Validates: Requirements 1.3, 1.5
# =============================================================================

@skip_if_no_redis
class TestRedisCollectionSizeLimit:
    """Property tests for Redis collection size limits."""
    
    @given(
        symbol=symbol_strategy,
        trades=st.lists(trade_strategy, min_size=1, max_size=1500)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_recent_trades_size_limit(self, redis_storage, symbol, trades):
        """
        Feature: three-tier-storage, Property 2: Redis collection size limit invariant
        Validates: Requirements 1.3
        
        For any sequence of writes to Redis sorted set (recent_trades),
        the collection size should never exceed 1000 items.
        """
        # Write all trades
        for trade in trades:
            redis_storage.write_recent_trade(symbol, trade)
        
        # Verify size limit
        count = redis_storage.get_trades_count(symbol)
        assert count <= RedisStorage.MAX_TRADES
    
    @given(alerts=st.lists(alert_strategy, min_size=1, max_size=1500))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_alerts_size_limit(self, redis_storage, alerts):
        """
        Feature: three-tier-storage, Property 2: Redis collection size limit invariant
        Validates: Requirements 1.5
        
        For any sequence of writes to Redis list (alerts),
        the collection size should never exceed 1000 items.
        """
        # Write all alerts
        for alert in alerts:
            redis_storage.write_alert(alert)
        
        # Verify size limit
        count = redis_storage.get_alerts_count()
        assert count <= RedisStorage.MAX_ALERTS
