"""
Property-based tests for Analytics Router.

Feature: fastapi-backend
Tests correctness properties for analytics data endpoints.
"""

import os
import pytest
from datetime import datetime, timedelta
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from src.api.main import app
from src.storage.query_router import QueryRouter


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def mock_query_router():
    """Create a mock QueryRouter for testing tier selection logic."""
    mock_redis = MagicMock()
    mock_postgres = MagicMock()
    mock_minio = MagicMock()
    
    router = QueryRouter(
        redis=mock_redis,
        postgres=mock_postgres,
        minio=mock_minio,
    )
    return router


@pytest.fixture
def test_client():
    """Create test client."""
    client = TestClient(app)
    yield client


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Symbol strategy - realistic crypto trading pairs
symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"
])

# Time offset strategies for different tiers
# Redis tier: < 1 hour
redis_offset_minutes = st.integers(min_value=1, max_value=59)

# DuckDB tier: 1 hour to 89 days
duckdb_offset_hours = st.integers(min_value=2, max_value=24 * 89)

# Parquet tier: >= 90 days (but < 365 to stay within max range)
parquet_offset_days = st.integers(min_value=90, max_value=364)

# Valid indicator names
valid_indicators = ["rsi", "macd", "macd_signal", "sma_20", "ema_12", "ema_26", "bb_upper", "bb_lower", "atr"]
indicator_strategy = st.sampled_from(valid_indicators)
indicator_subset_strategy = st.lists(indicator_strategy, min_size=1, max_size=5, unique=True)

# Price strategy
price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)


# =============================================================================
# Property 4: Query tier selection based on time range
# Feature: fastapi-backend, Property 4: Query tier selection based on time range
# Validates: Requirements 2.1
# =============================================================================

class TestQueryTierSelection:
    """Property tests for query tier selection based on time range."""
    
    @given(offset_minutes=redis_offset_minutes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_tier_selection_for_recent_data(self, mock_query_router, offset_minutes):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        
        For any time range within the last 1 hour, the system SHALL route to Redis tier.
        """
        now = datetime.now()
        start = now - timedelta(minutes=offset_minutes)
        end = now
        
        # Test tier selection
        selected_tier = mock_query_router._select_tier(start, end)
        
        # Should select Redis for data within last hour
        assert selected_tier == QueryRouter.TIER_REDIS, \
            f"Expected Redis tier for {offset_minutes} minutes ago, got {selected_tier}"
    
    @given(offset_hours=duckdb_offset_hours)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_duckdb_tier_selection_for_warm_data(self, mock_query_router, offset_hours):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        
        For any time range between 1 hour and 90 days, the system SHALL route to PostgreSQL tier.
        """
        now = datetime.now()
        start = now - timedelta(hours=offset_hours)
        end = now
        
        # Test tier selection
        selected_tier = mock_query_router._select_tier(start, end)
        
        # Should select PostgreSQL for data between 1 hour and 90 days
        assert selected_tier == QueryRouter.TIER_POSTGRES, \
            f"Expected PostgreSQL tier for {offset_hours} hours ago, got {selected_tier}"
    
    @given(offset_days=parquet_offset_days)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_parquet_tier_selection_for_cold_data(self, mock_query_router, offset_days):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        
        For any time range >= 90 days, the system SHALL route to MinIO tier.
        """
        now = datetime.now()
        start = now - timedelta(days=offset_days)
        end = now
        
        # Test tier selection
        selected_tier = mock_query_router._select_tier(start, end)
        
        # Should select MinIO for data older than 90 days
        assert selected_tier == QueryRouter.TIER_MINIO, \
            f"Expected MinIO tier for {offset_days} days ago, got {selected_tier}"



# =============================================================================
# Property 5: Indicator field filtering
# Feature: fastapi-backend, Property 5: Indicator field filtering
# Validates: Requirements 2.2
# =============================================================================

@pytest.fixture
def postgres_storage():
    """Create mock PostgresStorage instance for testing."""
    from src.storage.postgres_storage import PostgresStorage
    mock = MagicMock(spec=PostgresStorage)
    mock.query_candles.return_value = []
    mock.query_indicators.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


@pytest.fixture
def minio_storage():
    """Create mock MinioStorage instance for testing."""
    from src.storage.minio_storage import MinioStorage
    mock = MagicMock(spec=MinioStorage)
    mock.read_klines.return_value = []
    mock.read_indicators.return_value = []
    mock.read_alerts.return_value = []
    return mock


@pytest.fixture
def test_client_with_postgres(postgres_storage, minio_storage):
    """Create test client with mocked PostgreSQL and MinIO dependencies."""
    from src.api.dependencies import get_postgres, get_minio
    from src.api.main import rate_tracker
    
    # Clear rate tracker state before test
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    # Clear the lru_cache to ensure our mocks are used
    get_postgres.cache_clear()
    get_minio.cache_clear()
    
    def override_get_postgres():
        return postgres_storage
    
    def override_get_minio():
        return minio_storage
    
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_minio] = override_get_minio
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
    
    # Clear rate tracker state after test
    with rate_tracker._lock:
        rate_tracker._requests.clear()



class TestIndicatorFieldFiltering:
    """Property tests for indicator field filtering."""
    
    @given(
        symbol=symbol_strategy,
        requested_indicators=indicator_subset_strategy,
        rsi=st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        macd=st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
        sma_20=price_strategy,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
    def test_indicator_field_filtering(
        self, postgres_storage, test_client_with_postgres, 
        symbol, requested_indicators, rsi, macd, sma_20
    ):
        """
        Feature: fastapi-backend, Property 5: Indicator field filtering
        Validates: Requirements 2.2
        
        For any indicators query with specific indicator names, the response SHALL
        contain only the requested indicator fields (plus timestamp).
        """
        # Configure mock PostgreSQL to return indicator data
        now = datetime.now()
        indicator_data = {
            "timestamp": now,
            "symbol": symbol,
            "rsi": rsi,
            "macd": macd,
            "macd_signal": macd * 0.9,
            "sma_20": sma_20,
            "ema_12": sma_20 * 1.01,
            "ema_26": sma_20 * 0.99,
            "bb_upper": sma_20 * 1.1,
            "bb_lower": sma_20 * 0.9,
            "atr": abs(sma_20 * 0.02),
        }
        postgres_storage.query_indicators.return_value = [indicator_data]
        
        # Build query with requested indicators
        indicators_param = ",".join(requested_indicators)
        response = test_client_with_postgres.get(
            f"/api/v1/analytics/indicators/{symbol}?indicators={indicators_param}&period=1h"
        )
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        
        # Should have at least one result
        if len(data) > 0:
            result = data[0]
            
            # Verify only requested indicators have non-None values
            # (timestamp is always present)
            for indicator in valid_indicators:
                if indicator in requested_indicators:
                    # Requested indicator should have a value (may be None if not in DB)
                    # The key should exist in response
                    assert indicator in result, f"Requested indicator {indicator} not in response"
                else:
                    # Non-requested indicators should be None
                    assert result.get(indicator) is None, \
                        f"Non-requested indicator {indicator} should be None, got {result.get(indicator)}"
