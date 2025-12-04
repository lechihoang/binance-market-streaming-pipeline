"""
Property-based tests for Alerts Router.

Feature: fastapi-backend
Tests correctness properties for alerts endpoints.
"""

import json
import pytest
from datetime import datetime, timedelta
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient

from unittest.mock import MagicMock

from src.api.main import app
from src.storage.redis_storage import RedisStorage
from src.storage.postgres_storage import PostgresStorage


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
def postgres_storage():
    """Create mock PostgresStorage instance for testing."""
    mock = MagicMock(spec=PostgresStorage)
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


@pytest.fixture
def test_client_redis(redis_storage):
    """Create test client with mocked Redis dependency."""
    from src.api.dependencies import get_redis
    from src.api.main import rate_tracker
    
    # Clear rate tracker state before test
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    def override_get_redis():
        return redis_storage
    
    app.dependency_overrides[get_redis] = override_get_redis
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
    
    # Clear rate tracker state after test
    with rate_tracker._lock:
        rate_tracker._requests.clear()


@pytest.fixture
def test_client_postgres(postgres_storage):
    """Create test client with mocked PostgreSQL dependency."""
    from src.api.dependencies import get_postgres
    from src.api.main import rate_tracker
    
    # Clear rate tracker state before test
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    def override_get_postgres():
        return postgres_storage
    
    app.dependency_overrides[get_postgres] = override_get_postgres
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

# Alert type strategy - valid alert types
alert_type_strategy = st.sampled_from(["whale", "price_spike", "volume_anomaly", "volatility"])

# Severity strategy
severity_strategy = st.sampled_from(["info", "warning", "critical"])

# Timestamp strategy - realistic Unix timestamps in milliseconds
timestamp_strategy = st.integers(min_value=1600000000000, max_value=2000000000000)

# Alert strategy for Redis (stores timestamp in milliseconds)
alert_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "alert_type": alert_type_strategy,
    "severity": severity_strategy,
    "message": st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'Z'))),
    "timestamp": timestamp_strategy,
    "metadata": st.none() | st.fixed_dictionaries({
        "value": st.floats(min_value=0, max_value=1000000, allow_nan=False, allow_infinity=False),
    }),
})

# Limit strategy for alerts endpoint - max 1000 per API validation
limit_strategy = st.integers(min_value=1, max_value=1000)


# =============================================================================
# Property 6: Alerts limit constraint
# Feature: fastapi-backend, Property 6: Alerts limit constraint
# Validates: Requirements 3.1
# =============================================================================

@skip_if_no_redis
class TestAlertsLimitConstraint:
    """Property tests for alerts limit constraint."""
    
    @given(
        limit=limit_strategy,
        alerts=st.lists(alert_strategy, min_size=1, max_size=50)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_alerts_limit_constraint(self, redis_storage, test_client_redis, limit, alerts):
        """
        Feature: fastapi-backend, Property 6: Alerts limit constraint
        Validates: Requirements 3.1
        
        For any limit parameter value N, calling GET /api/v1/alerts/recent?limit=N
        SHALL return at most min(N, 1000) alerts.
        """
        # Flush db to ensure clean state for this test iteration
        redis_storage.flush_db()
        
        # Store alerts in Redis
        for alert in alerts:
            redis_storage.write_alert(alert)
        
        # Call API endpoint with limit
        response = test_client_redis.get(f"/api/v1/alerts/recent?limit={limit}")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        
        # Verify limit constraint: at most min(limit, 1000) alerts
        expected_max = min(limit, 1000)
        assert len(data) <= expected_max
        
        # Also verify we get at most what we stored
        assert len(data) <= len(alerts)


# =============================================================================
# Property 7: Alert history filtering
# Feature: fastapi-backend, Property 7: Alert history filtering
# Validates: Requirements 3.2
# =============================================================================

class TestAlertHistoryFiltering:
    """Property tests for alert history filtering."""
    
    @given(
        alert_type=alert_type_strategy,
        alerts=st.lists(alert_strategy, min_size=1, max_size=20)
    )
    @settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_alert_history_filtering(self, postgres_storage, test_client_postgres, alert_type, alerts):
        """
        Feature: fastapi-backend, Property 7: Alert history filtering
        Validates: Requirements 3.2
        
        For any alert history query with type filter, all returned alerts
        SHALL have alert_type matching the filter.
        """
        # Configure mock PostgreSQL to return filtered alerts
        now = datetime.now()
        filtered_alerts = []
        for i, alert in enumerate(alerts):
            if alert["alert_type"] == alert_type:
                alert_timestamp = now - timedelta(hours=i)
                filtered_alerts.append({
                    "timestamp": alert_timestamp,
                    "symbol": alert["symbol"],
                    "alert_type": alert["alert_type"],
                    "severity": alert["severity"],
                    "message": alert["message"],
                    "metadata": alert.get("metadata"),
                })
        
        postgres_storage.query_alerts.return_value = filtered_alerts
        
        # Query with type filter
        start = (now - timedelta(days=1)).isoformat()
        end = (now + timedelta(hours=1)).isoformat()
        
        response = test_client_postgres.get(
            f"/api/v1/alerts/history?start={start}&end={end}&type={alert_type}"
        )
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        
        # Verify all returned alerts have the correct type
        for alert in data:
            assert alert["alert_type"] == alert_type
