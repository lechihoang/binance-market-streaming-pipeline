"""
Property-based tests for Market Router.

Feature: fastapi-backend
Tests correctness properties for market data endpoints.
"""

import pytest
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient

from src.api.main import app
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
    storage = RedisStorage(host="localhost", port=6379, db=15)
    storage.flush_db()
    yield storage
    storage.flush_db()


@pytest.fixture
def mock_postgres():
    """Create a mock PostgreSQL storage that returns empty results."""
    from unittest.mock import MagicMock
    mock = MagicMock()
    mock.query_candles.return_value = []
    mock.query_indicators.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


@pytest.fixture
def mock_minio():
    """Create a mock MinIO storage that returns empty results."""
    from unittest.mock import MagicMock
    mock = MagicMock()
    mock.read_klines.return_value = []
    mock.read_indicators.return_value = []
    mock.read_alerts.return_value = []
    return mock


@pytest.fixture
def test_client(redis_storage, mock_postgres, mock_minio):
    """Create test client with mocked storage dependencies."""
    from src.api.dependencies import get_redis, get_postgres, get_minio
    from src.api.main import rate_tracker
    
    # Clear rate tracker state before test
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    def override_get_redis():
        return redis_storage
    
    def override_get_postgres():
        return mock_postgres
    
    def override_get_minio():
        return mock_minio
    
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_minio] = override_get_minio
    
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
    
    # Clear rate tracker state after test
    with rate_tracker._lock:
        rate_tracker._requests.clear()


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

# Ticker stats strategy
ticker_stats_strategy = st.fixed_dictionaries({
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
    "price_change_24h": st.floats(min_value=-100.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    "timestamp": timestamp_strategy,
})

# Trade strategy
trade_strategy = st.fixed_dictionaries({
    "price": price_strategy,
    "quantity": volume_strategy,
    "timestamp": timestamp_strategy,
    "is_buyer_maker": st.booleans(),
})

# Limit strategy for trades endpoint - max 1000 per API validation
limit_strategy = st.integers(min_value=1, max_value=1000)


# =============================================================================
# Property 1: Redis data retrieval consistency
# Feature: fastapi-backend, Property 1: Redis data retrieval consistency
# Validates: Requirements 1.1
# =============================================================================

@skip_if_no_redis
class TestTickerDataConsistency:
    """Property tests for ticker data consistency."""
    
    @given(symbol=symbol_strategy, stats=ticker_stats_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ticker_data_consistency(self, redis_storage, test_client, symbol, stats):
        """
        Feature: fastapi-backend, Property 1: Redis data retrieval consistency
        Validates: Requirements 1.1
        
        For any symbol with data stored in Redis, calling GET /api/v1/market/ticker/{symbol}
        SHALL return data matching what was stored in Redis hash `latest_ticker:{symbol}`.
        """
        # Store ticker data in Redis
        redis_storage.write_latest_ticker(symbol, stats)
        
        # Call API endpoint
        response = test_client.get(f"/api/v1/market/ticker/{symbol}")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        
        # Verify data matches what was stored
        # Note: Pydantic model uses aliases, so response uses original field names
        assert data["symbol"] == symbol
        assert abs(data["price"] - stats["close"]) < 1e-6
        assert abs(data["volume"] - stats["volume"]) < 1e-6
        assert abs(data["high"] - stats["high"]) < 1e-6
        assert abs(data["low"] - stats["low"]) < 1e-6
        assert data["timestamp"] == stats["timestamp"]


# =============================================================================
# Property 2: Price endpoint data consistency
# Feature: fastapi-backend, Property 2: Price endpoint data consistency
# Validates: Requirements 1.2
# =============================================================================

@skip_if_no_redis
class TestPriceDataConsistency:
    """Property tests for price data consistency."""
    
    @given(symbol=symbol_strategy, price=price_strategy, volume=volume_strategy, timestamp=timestamp_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_price_data_consistency(self, redis_storage, test_client, symbol, price, volume, timestamp):
        """
        Feature: fastapi-backend, Property 2: Price endpoint data consistency
        Validates: Requirements 1.2
        
        For any symbol with price data stored in Redis, calling GET /api/v1/market/price/{symbol}
        SHALL return price, volume, timestamp matching Redis hash `latest_price:{symbol}`.
        """
        # Store price data in Redis
        redis_storage.write_latest_price(symbol, price, volume, timestamp)
        
        # Call API endpoint
        response = test_client.get(f"/api/v1/market/price/{symbol}")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        
        # Verify data matches what was stored
        assert data["symbol"] == symbol
        assert abs(data["price"] - price) < 1e-6
        assert abs(data["volume"] - volume) < 1e-6
        assert data["timestamp"] == timestamp


# =============================================================================
# Property 3: Trades limit constraint
# Feature: fastapi-backend, Property 3: Trades limit constraint
# Validates: Requirements 1.3
# =============================================================================

@skip_if_no_redis
class TestTradesLimitConstraint:
    """Property tests for trades limit constraint."""
    
    @given(
        symbol=symbol_strategy,
        limit=limit_strategy,
        trades=st.lists(trade_strategy, min_size=1, max_size=100)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_trades_limit_constraint(self, redis_storage, test_client, symbol, limit, trades):
        """
        Feature: fastapi-backend, Property 3: Trades limit constraint
        Validates: Requirements 1.3
        
        For any limit parameter value N, calling GET /api/v1/market/trades/{symbol}?limit=N
        SHALL return at most min(N, 1000) trades.
        """
        # Flush db to ensure clean state for this test iteration
        redis_storage.flush_db()
        
        # Store trades in Redis
        for trade in trades:
            redis_storage.write_recent_trade(symbol, trade)
        
        # Call API endpoint with limit
        response = test_client.get(f"/api/v1/market/trades/{symbol}?limit={limit}")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        
        # Verify limit constraint: at most min(limit, 1000) trades
        expected_max = min(limit, 1000)
        assert len(data) <= expected_max
        
        # Also verify we get at most what we stored
        assert len(data) <= len(trades)
