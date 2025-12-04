"""
Property-based tests for Grafana data source provisioning.

Feature: grafana-dashboard
Tests correctness properties for data source configuration.
"""

import pytest
import yaml
from pathlib import Path
from hypothesis import given, settings, strategies as st


# =============================================================================
# Constants
# =============================================================================

DATASOURCES_PATH = Path("grafana/provisioning/datasources/datasources.yml")

REQUIRED_DATASOURCES = {
    "Redis": {
        "type": "redis-datasource",
        "url": "redis:6379"
    },
    "Prometheus": {
        "type": "prometheus",
        "url": "http://prometheus:9090"
    },
    "FastAPI": {
        "type": "marcusolsson-json-datasource",
        "url": "http://crypto-api:8000"
    },
    "PostgreSQL": {
        "type": "postgres",
        "url": "postgres-data:5432"
    }
}


# =============================================================================
# Helper Functions
# =============================================================================

def load_datasources_yaml():
    """Load and parse the datasources.yml file."""
    if not DATASOURCES_PATH.exists():
        pytest.skip(f"Datasources file not found: {DATASOURCES_PATH}")
    
    with open(DATASOURCES_PATH, "r") as f:
        return yaml.safe_load(f)


# =============================================================================
# Property 1: Data Source Provisioning Completeness
# Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
# Validates: Requirements 1.1, 1.2, 1.3
# =============================================================================

class TestDataSourceProvisioning:
    """Property tests for data source provisioning completeness."""
    
    def test_datasources_file_exists(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.1, 1.2, 1.3
        
        The datasources.yml file must exist in the provisioning directory.
        """
        assert DATASOURCES_PATH.exists(), f"Datasources file not found: {DATASOURCES_PATH}"
    
    def test_datasources_yaml_valid(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.1, 1.2, 1.3
        
        The datasources.yml file must be valid YAML.
        """
        config = load_datasources_yaml()
        assert config is not None, "Failed to parse datasources.yml"
        assert "datasources" in config, "Missing 'datasources' key in config"
    
    def test_datasources_count(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.1, 1.2, 1.3, 4.3
        
        For any valid datasources.yml file, parsing the YAML SHALL produce
        a list containing exactly 4 data sources (Redis, Prometheus, FastAPI, PostgreSQL).
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        assert len(datasources) == 4, f"Expected 4 datasources, got {len(datasources)}"
    
    def test_all_required_datasources_present(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.1, 1.2, 1.3
        
        For any valid datasources.yml file, it SHALL contain data sources
        with names "Redis", "Prometheus", and "FastAPI".
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        datasource_names = {ds.get("name") for ds in datasources}
        required_names = set(REQUIRED_DATASOURCES.keys())
        
        assert required_names.issubset(datasource_names), \
            f"Missing datasources: {required_names - datasource_names}"
    
    @given(datasource_name=st.sampled_from(list(REQUIRED_DATASOURCES.keys())))
    @settings(max_examples=100)
    def test_datasource_configuration_correctness(self, datasource_name):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.1, 1.2, 1.3
        
        For any required data source name, the configuration SHALL have
        the correct type and URL as specified in requirements.
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        # Find the datasource by name
        datasource = None
        for ds in datasources:
            if ds.get("name") == datasource_name:
                datasource = ds
                break
        
        assert datasource is not None, f"Datasource '{datasource_name}' not found"
        
        expected = REQUIRED_DATASOURCES[datasource_name]
        
        # Verify type
        assert datasource.get("type") == expected["type"], \
            f"Datasource '{datasource_name}' has wrong type: expected {expected['type']}, got {datasource.get('type')}"
        
        # Verify URL
        assert datasource.get("url") == expected["url"], \
            f"Datasource '{datasource_name}' has wrong URL: expected {expected['url']}, got {datasource.get('url')}"
    
    def test_redis_datasource_config(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.1
        
        WHEN Grafana starts THEN the system SHALL auto-provision Redis data source
        với URL redis:6379.
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        redis_ds = next((ds for ds in datasources if ds.get("name") == "Redis"), None)
        
        assert redis_ds is not None, "Redis datasource not found"
        assert redis_ds.get("type") == "redis-datasource"
        assert redis_ds.get("url") == "redis:6379"
    
    def test_prometheus_datasource_config(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.2
        
        WHEN Grafana starts THEN the system SHALL auto-provision Prometheus data source
        với URL prometheus:9090.
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        prometheus_ds = next((ds for ds in datasources if ds.get("name") == "Prometheus"), None)
        
        assert prometheus_ds is not None, "Prometheus datasource not found"
        assert prometheus_ds.get("type") == "prometheus"
        assert prometheus_ds.get("url") == "http://prometheus:9090"
    
    def test_fastapi_datasource_config(self):
        """
        Feature: grafana-dashboard, Property 1: Data Source Provisioning Completeness
        Validates: Requirements 1.3
        
        WHEN Grafana starts THEN the system SHALL auto-provision JSON API data source
        với URL http://crypto-api:8000.
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        fastapi_ds = next((ds for ds in datasources if ds.get("name") == "FastAPI"), None)
        
        assert fastapi_ds is not None, "FastAPI datasource not found"
        assert fastapi_ds.get("type") == "marcusolsson-json-datasource"
        assert fastapi_ds.get("url") == "http://crypto-api:8000"

    def test_postgresql_datasource_config(self):
        """
        Feature: storage-tier-migration, Property: PostgreSQL Datasource Configuration
        Validates: Requirements 4.3
        
        WHEN Grafana starts THEN the system SHALL auto-provision PostgreSQL data source
        connected to postgres-data:5432 with crypto_data database.
        """
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        postgres_ds = next((ds for ds in datasources if ds.get("name") == "PostgreSQL"), None)
        
        assert postgres_ds is not None, "PostgreSQL datasource not found"
        assert postgres_ds.get("type") == "postgres"
        assert postgres_ds.get("url") == "postgres-data:5432"
        assert postgres_ds.get("database") == "crypto_data"
        assert postgres_ds.get("user") == "crypto"
