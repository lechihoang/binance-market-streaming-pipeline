"""
Property-based tests for QueryRouter.

Feature: three-tier-storage
Tests correctness properties for query routing tier selection.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from hypothesis import given, settings, strategies as st, assume, HealthCheck

from src.storage.query_router import QueryRouter


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Strategy for generating time offsets in minutes (for Redis tier: < 60 minutes)
redis_offset_minutes = st.integers(min_value=0, max_value=59)

# Strategy for generating time offsets in hours (for PostgreSQL tier: 1 hour to < 90 days)
# 90 days = 2160 hours, so we use 1 to 2159 hours
postgres_offset_hours = st.integers(min_value=1, max_value=2159)

# Strategy for generating time offsets in days (for MinIO tier: >= 90 days)
minio_offset_days = st.integers(min_value=90, max_value=365 * 5)  # Up to 5 years


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def mock_redis():
    """Create mock RedisStorage."""
    mock = MagicMock()
    mock.get_indicators.return_value = None
    mock.get_aggregation.return_value = None
    mock.get_recent_trades.return_value = []
    mock.get_recent_alerts.return_value = []
    return mock


@pytest.fixture
def mock_postgres():
    """Create mock PostgresStorage."""
    mock = MagicMock()
    mock.query_indicators.return_value = []
    mock.query_candles.return_value = []
    mock.query_alerts.return_value = []
    return mock


@pytest.fixture
def mock_minio():
    """Create mock MinioStorage."""
    mock = MagicMock()
    mock.read_indicators.return_value = []
    mock.read_klines.return_value = []
    mock.read_alerts.return_value = []
    return mock


@pytest.fixture
def query_router(mock_redis, mock_postgres, mock_minio):
    """Create QueryRouter with mock storage instances."""
    return QueryRouter(
        redis=mock_redis,
        postgres=mock_postgres,
        minio=mock_minio,
    )


# =============================================================================
# Property 6: Query routing correctness
# Feature: three-tier-storage, Property 6: Query routing correctness
# Validates: Requirements 4.1, 4.2, 4.3
# =============================================================================

class TestQueryRoutingCorrectness:
    """
    Property tests for query routing correctness.
    
    Feature: storage-tier-migration, Property 5: Query Interface Compatibility
    Validates: Requirements 4.1, 4.2
    
    For any query with time range, the router should select:
    - Redis for < 1 hour
    - PostgreSQL for < 90 days
    - MinIO for >= 90 days
    """
    
    @given(offset_minutes=redis_offset_minutes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_tier_selection_for_recent_queries(self, query_router, offset_minutes):
        """
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.1
        
        For any query with time range within last 1 hour,
        the router should select Redis tier.
        """
        now = datetime.now()
        # Start time is within the last hour (0-59 minutes ago)
        start = now - timedelta(minutes=offset_minutes)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        assert selected_tier == QueryRouter.TIER_REDIS, \
            f"Expected Redis for {offset_minutes} minutes ago, got {selected_tier}"
    
    @given(offset_hours=postgres_offset_hours)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_postgres_tier_selection_for_medium_range_queries(self, query_router, offset_hours):
        """
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.1
        
        For any query with time range between 1 hour and 90 days,
        the router should select PostgreSQL tier.
        """
        now = datetime.now()
        # Start time is between 1 hour and 90 days ago
        start = now - timedelta(hours=offset_hours)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        assert selected_tier == QueryRouter.TIER_POSTGRES, \
            f"Expected PostgreSQL for {offset_hours} hours ago, got {selected_tier}"
    
    @given(offset_days=minio_offset_days)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_minio_tier_selection_for_historical_queries(self, query_router, offset_days):
        """
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.2
        
        For any query with time range >= 90 days,
        the router should select MinIO tier.
        """
        now = datetime.now()
        # Start time is 90 days or more ago
        start = now - timedelta(days=offset_days)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        assert selected_tier == QueryRouter.TIER_MINIO, \
            f"Expected MinIO for {offset_days} days ago, got {selected_tier}"
    
    def test_boundary_exactly_1_hour(self, query_router):
        """
        Test boundary condition: exactly 1 hour ago should use PostgreSQL.
        
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.1
        """
        now = datetime.now()
        # Exactly 1 hour ago (60 minutes)
        start = now - timedelta(hours=1)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        # At exactly 1 hour boundary, should use PostgreSQL (not Redis)
        assert selected_tier == QueryRouter.TIER_POSTGRES
    
    def test_boundary_exactly_90_days(self, query_router):
        """
        Test boundary condition: exactly 90 days ago should use MinIO.
        
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.1, 4.2
        """
        now = datetime.now()
        # Exactly 90 days ago
        start = now - timedelta(days=90)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        # At exactly 90 days boundary, should use MinIO (not PostgreSQL)
        assert selected_tier == QueryRouter.TIER_MINIO
    
    def test_just_under_1_hour_uses_redis(self, query_router):
        """
        Test that 59 minutes 59 seconds ago uses Redis.
        
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.1
        """
        now = datetime.now()
        # Just under 1 hour (59 minutes, 59 seconds)
        start = now - timedelta(minutes=59, seconds=59)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        assert selected_tier == QueryRouter.TIER_REDIS
    
    def test_just_under_90_days_uses_postgres(self, query_router):
        """
        Test that 89 days 23 hours ago uses PostgreSQL.
        
        Feature: storage-tier-migration, Property 5: Query Interface Compatibility
        Validates: Requirements 4.1
        """
        now = datetime.now()
        # Just under 90 days (89 days, 23 hours)
        start = now - timedelta(days=89, hours=23)
        end = now
        
        selected_tier = query_router._select_tier(start, end)
        
        assert selected_tier == QueryRouter.TIER_POSTGRES
