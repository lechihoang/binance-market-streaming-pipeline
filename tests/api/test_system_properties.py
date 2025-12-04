"""
Property-based tests for System Router.

Feature: fastapi-backend
Tests correctness properties for system health, metrics, and status endpoints.
"""

import pytest
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient

from src.api.main import app
from src.api.routers.system import determine_overall_status


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def test_client():
    """Create test client."""
    client = TestClient(app)
    yield client


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Boolean strategy for service health states
service_health_strategy = st.booleans()


# =============================================================================
# Property 8: Health status reflects service state
# Feature: fastapi-backend, Property 8: Health status reflects service state
# Validates: Requirements 5.1, 5.4
# =============================================================================

class TestHealthStatusLogic:
    """Property tests for health status logic."""
    
    @given(
        redis_healthy=service_health_strategy,
        duckdb_healthy=service_health_strategy,
        kafka_healthy=service_health_strategy
    )
    @settings(max_examples=100)
    def test_health_status_reflects_service_state(
        self, redis_healthy, duckdb_healthy, kafka_healthy
    ):
        """
        Feature: fastapi-backend, Property 8: Health status reflects service state
        Validates: Requirements 5.1, 5.4
        
        For any combination of service states (Redis up/down, DuckDB up/down, Kafka up/down),
        health endpoint SHALL return:
        - "healthy" only if all services are up
        - "degraded" if some services are down
        - "unhealthy" if all services are down
        """
        # Call the determine_overall_status function
        status = determine_overall_status(redis_healthy, duckdb_healthy, kafka_healthy)
        
        # Count healthy services
        services = [redis_healthy, duckdb_healthy, kafka_healthy]
        healthy_count = sum(services)
        total_services = len(services)
        
        # Verify status logic
        if healthy_count == total_services:
            # All services healthy -> status should be "healthy"
            assert status == "healthy", (
                f"Expected 'healthy' when all services up, got '{status}'. "
                f"Services: redis={redis_healthy}, duckdb={duckdb_healthy}, kafka={kafka_healthy}"
            )
        elif healthy_count == 0:
            # No services healthy -> status should be "unhealthy"
            assert status == "unhealthy", (
                f"Expected 'unhealthy' when all services down, got '{status}'. "
                f"Services: redis={redis_healthy}, duckdb={duckdb_healthy}, kafka={kafka_healthy}"
            )
        else:
            # Some services healthy -> status should be "degraded"
            assert status == "degraded", (
                f"Expected 'degraded' when some services down, got '{status}'. "
                f"Services: redis={redis_healthy}, duckdb={duckdb_healthy}, kafka={kafka_healthy}"
            )
    
    def test_all_healthy_returns_healthy(self):
        """Verify all services up returns 'healthy'."""
        status = determine_overall_status(True, True, True)
        assert status == "healthy"
    
    def test_all_unhealthy_returns_unhealthy(self):
        """Verify all services down returns 'unhealthy'."""
        status = determine_overall_status(False, False, False)
        assert status == "unhealthy"
    
    def test_partial_failure_returns_degraded(self):
        """Verify partial failure returns 'degraded'."""
        # Redis down only
        assert determine_overall_status(False, True, True) == "degraded"
        # DuckDB down only
        assert determine_overall_status(True, False, True) == "degraded"
        # Kafka down only
        assert determine_overall_status(True, True, False) == "degraded"
        # Two services down
        assert determine_overall_status(False, False, True) == "degraded"
        assert determine_overall_status(False, True, False) == "degraded"
        assert determine_overall_status(True, False, False) == "degraded"
