"""
Property-based tests for Fallback Chain.

Feature: fastapi-backend
Tests correctness properties for fallback chain execution.
"""

import os
import pytest
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient

from src.api.main import app
from src.storage.redis_storage import RedisStorage
from src.storage.postgres_storage import PostgresStorage
from src.storage.minio_storage import MinioStorage


# =============================================================================
# Service availability checks
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
def postgres_storage():
    """Create mock PostgresStorage instance for testing."""
    mock = MagicMock(spec=PostgresStorage)
    mock.query_candles.return_value = []
    mock.query_indicators.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


@pytest.fixture
def minio_storage():
    """Create mock MinioStorage instance for testing."""
    mock = MagicMock(spec=MinioStorage)
    mock.read_klines.return_value = []
    mock.read_indicators.return_value = []
    mock.read_alerts.return_value = []
    return mock


@pytest.fixture
def test_client_with_fallback(redis_storage, postgres_storage, minio_storage):
    """Create test client with all storage dependencies."""
    from src.api.dependencies import get_redis, get_postgres, get_minio, get_query_router
    from src.storage.query_router import QueryRouter
    from src.api.main import rate_tracker
    
    # Clear rate tracker state before test
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    def override_get_redis():
        return redis_storage
    
    def override_get_postgres():
        return postgres_storage
    
    def override_get_minio():
        return minio_storage
    
    def override_get_query_router():
        return QueryRouter(redis_storage, postgres_storage, minio_storage)
    
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_minio] = override_get_minio
    app.dependency_overrides[get_query_router] = override_get_query_router
    
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
    
    # Clear rate tracker state after test
    with rate_tracker._lock:
        rate_tracker._requests.clear()


# =============================================================================
# Strategies for generating test data
# =============================================================================

symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"
])

price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)
volume_strategy = st.floats(min_value=0.0, max_value=1000000000.0, allow_nan=False, allow_infinity=False)
timestamp_strategy = st.integers(min_value=1600000000000, max_value=2000000000000)

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


# =============================================================================
# Property 10: Fallback chain execution
# Feature: fastapi-backend, Property 10: Fallback chain execution
# Validates: Requirements 7.1, 7.2, 7.4
# =============================================================================

@skip_if_no_redis
class TestFallbackChainExecution:
    """Property tests for fallback chain execution."""
    
    @given(symbol=symbol_strategy, stats=ticker_stats_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_primary_returns_redis_source(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol, stats
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.1, 7.2, 7.4
        
        For any query where Redis has data, the response SHALL include 
        X-Data-Source: redis header.
        """
        # Store data in Redis
        redis_storage.write_latest_ticker(symbol, stats)
        
        # Call API endpoint
        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")
        
        # Verify response
        assert response.status_code == 200
        
        # Verify X-Data-Source header indicates redis
        assert "X-Data-Source" in response.headers
        assert response.headers["X-Data-Source"] == "redis"
    
    @given(symbol=symbol_strategy)
    @settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_postgres_fallback_returns_postgres_source(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.1, 7.2, 7.4
        
        For any query where Redis fails/empty but PostgreSQL has data, 
        the response SHALL include X-Data-Source: postgres header.
        """
        # Ensure Redis is empty for this symbol
        redis_storage.flush_db()
        
        # Configure mock PostgreSQL to return data
        now = datetime.now()
        candle = {
            "timestamp": now,
            "symbol": symbol,
            "open": 100.0,
            "high": 110.0,
            "low": 90.0,
            "close": 105.0,
            "volume": 1000.0,
            "quote_volume": 100000.0,
            "trades_count": 50,
        }
        postgres_storage.query_candles.return_value = [candle]
        
        # Call API endpoint
        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")
        
        # Verify response
        assert response.status_code == 200
        
        # Verify X-Data-Source header indicates postgres
        assert "X-Data-Source" in response.headers
        assert response.headers["X-Data-Source"] == "postgres"
    
    @given(symbol=symbol_strategy)
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_minio_fallback_returns_minio_source(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.1, 7.2, 7.4
        
        For any query where Redis and PostgreSQL fail/empty but MinIO has data,
        the response SHALL include X-Data-Source: minio header.
        """
        # Ensure Redis and PostgreSQL are empty
        redis_storage.flush_db()
        postgres_storage.query_candles.return_value = []
        
        # Configure mock MinIO to return data
        now = datetime.now()
        klines = [{
            "timestamp": now,
            "symbol": symbol,
            "open": 100.0,
            "high": 110.0,
            "low": 90.0,
            "close": 105.0,
            "volume": 1000.0,
            "quote_volume": 100000.0,
            "trades_count": 50,
        }]
        minio_storage.read_klines.return_value = klines
        
        # Call API endpoint
        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")
        
        # Verify response
        assert response.status_code == 200
        
        # Verify X-Data-Source header indicates minio
        assert "X-Data-Source" in response.headers
        assert response.headers["X-Data-Source"] == "minio"
    
    @given(symbol=symbol_strategy)
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_all_sources_empty_returns_404(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.3
        
        For any query where all data sources are empty,
        the API SHALL return HTTP 404 (symbol not found).
        """
        # Ensure all sources are empty
        redis_storage.flush_db()
        postgres_storage.query_candles.return_value = []
        minio_storage.read_klines.return_value = []
        
        # Call API endpoint
        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")
        
        # Verify 404 response when no data available
        assert response.status_code == 404
    
    @given(symbol=symbol_strategy)
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_analytics_postgres_fallback_to_minio(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.2, 7.4
        
        For analytics queries where PostgreSQL is empty but MinIO has data,
        the response SHALL include X-Data-Source: minio header.
        """
        # Ensure PostgreSQL is empty
        postgres_storage.query_indicators.return_value = []
        
        # Configure mock MinIO to return indicators
        now = datetime.now()
        indicators = [{
            "timestamp": now,
            "symbol": symbol,
            "rsi": 50.0,
            "macd": 0.5,
            "macd_signal": 0.3,
            "sma_20": 100.0,
            "ema_12": 101.0,
            "ema_26": 99.0,
            "bb_upper": 110.0,
            "bb_lower": 90.0,
            "atr": 5.0,
        }]
        minio_storage.read_indicators.return_value = indicators
        
        # Call API endpoint
        response = test_client_with_fallback.get(
            f"/api/v1/analytics/indicators/{symbol}?period=24h"
        )
        
        # Verify response
        assert response.status_code == 200
        
        # Verify X-Data-Source header indicates minio
        assert "X-Data-Source" in response.headers
        assert response.headers["X-Data-Source"] == "minio"

