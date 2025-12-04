"""
Property-based tests for Rate Limiting.

Feature: fastapi-backend
Tests correctness properties for rate limit enforcement.
"""

import time
import pytest
from unittest.mock import MagicMock
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient

from src.api.main import app, rate_tracker, RATE_LIMIT_PER_MINUTE
from src.api.dependencies import get_redis, get_postgres, get_minio


# =============================================================================
# Mock Storage Classes
# =============================================================================

def create_mock_redis():
    """Create a mock Redis storage that returns None for all queries."""
    mock = MagicMock()
    mock.get_latest_ticker.return_value = None
    mock.get_latest_price.return_value = None
    mock.get_recent_trades.return_value = None
    mock.get_recent_alerts.return_value = []
    mock.health_check.return_value = True
    return mock


def create_mock_postgres():
    """Create a mock PostgreSQL storage that returns empty results."""
    mock = MagicMock()
    mock.query_candles.return_value = []
    mock.query_indicators.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


def create_mock_minio():
    """Create a mock MinIO storage that returns empty results."""
    mock = MagicMock()
    mock.read_klines.return_value = []
    mock.read_indicators.return_value = []
    mock.read_alerts.return_value = []
    return mock


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def fresh_client():
    """Create test client with fresh rate limit state and mocked dependencies."""
    # Clear rate tracker state before each test
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    # Override dependencies with mocks
    app.dependency_overrides[get_redis] = create_mock_redis
    app.dependency_overrides[get_postgres] = create_mock_postgres
    app.dependency_overrides[get_minio] = create_mock_minio
    
    client = TestClient(app)
    yield client
    
    # Clean up after test
    app.dependency_overrides.clear()
    with rate_tracker._lock:
        rate_tracker._requests.clear()


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Number of requests strategy - test various request counts
request_count_strategy = st.integers(min_value=1, max_value=150)


# =============================================================================
# Property 9: Rate limit enforcement
# Feature: fastapi-backend, Property 9: Rate limit enforcement
# Validates: Requirements 6.1
# =============================================================================

class TestRateLimitEnforcement:
    """Property tests for rate limit enforcement."""
    
    def test_rate_limit_headers_present(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1, 6.4
        
        Verify that rate limit headers are present in responses.
        """
        response = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
        
        # Headers should be present regardless of response status
        assert "X-RateLimit-Limit" in response.headers
        assert "X-RateLimit-Remaining" in response.headers
        assert "X-RateLimit-Reset" in response.headers
        
        # Verify header values are valid
        assert response.headers["X-RateLimit-Limit"] == str(RATE_LIMIT_PER_MINUTE)
        remaining = int(response.headers["X-RateLimit-Remaining"])
        assert 0 <= remaining <= RATE_LIMIT_PER_MINUTE
    
    def test_rate_limit_remaining_decreases(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        
        Verify that remaining count decreases with each request.
        """
        # Make first request
        response1 = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
        remaining1 = int(response1.headers["X-RateLimit-Remaining"])
        
        # Make second request
        response2 = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
        remaining2 = int(response2.headers["X-RateLimit-Remaining"])
        
        # Remaining should decrease
        assert remaining2 < remaining1
    
    def test_rate_limit_exceeded_returns_429(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        
        For any IP address exceeding 100 requests per minute,
        subsequent requests SHALL receive HTTP 429 response.
        """
        # Make 100 requests to hit the limit
        for i in range(RATE_LIMIT_PER_MINUTE):
            response = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
            # Should succeed until we hit the limit
            if response.status_code == 429:
                # We hit the limit early, which is fine
                break
        
        # The next request should be rate limited
        response = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
        
        assert response.status_code == 429
        assert "Retry-After" in response.headers
        assert "X-RateLimit-Limit" in response.headers
        assert response.headers["X-RateLimit-Remaining"] == "0"
    
    def test_rate_limit_429_includes_retry_after(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        
        When rate limit is exceeded, response SHALL include Retry-After header.
        """
        # Exhaust rate limit
        for _ in range(RATE_LIMIT_PER_MINUTE + 1):
            response = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
        
        # Verify 429 response has Retry-After
        assert response.status_code == 429
        assert "Retry-After" in response.headers
        
        retry_after = int(response.headers["Retry-After"])
        assert retry_after > 0
        assert retry_after <= 60  # Should be within the window
    
    @given(request_count=st.integers(min_value=101, max_value=110))
    @settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_requests_over_limit_always_rejected(self, fresh_client, request_count):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        
        For any number of requests N > 100, requests after the 100th
        SHALL receive HTTP 429 response.
        """
        # Clear state for this test iteration
        with rate_tracker._lock:
            rate_tracker._requests.clear()
        
        rejected_count = 0
        
        for i in range(request_count):
            response = fresh_client.get("/api/v1/market/ticker/BTCUSDT")
            if response.status_code == 429:
                rejected_count += 1
        
        # All requests over the limit should be rejected
        expected_rejections = request_count - RATE_LIMIT_PER_MINUTE
        assert rejected_count >= expected_rejections
    
    def test_health_endpoint_not_rate_limited(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        
        Health endpoint should be exempt from rate limiting.
        """
        # Exhaust rate limit on regular endpoint
        for _ in range(RATE_LIMIT_PER_MINUTE + 5):
            fresh_client.get("/api/v1/market/ticker/BTCUSDT")
        
        # Health endpoint should still work
        response = fresh_client.get("/api/v1/system/health")
        
        # Health endpoint should not return 429
        assert response.status_code != 429

