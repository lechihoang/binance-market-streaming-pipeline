"""
Property-based tests for Docker Compose Grafana configuration.

Feature: grafana-dashboard
Tests correctness properties for Docker Compose Grafana service configuration.
"""

import pytest
import yaml
from pathlib import Path
from hypothesis import given, settings, strategies as st


# =============================================================================
# Constants
# =============================================================================

DOCKER_COMPOSE_PATH = Path("docker-compose.yml")

REQUIRED_GRAFANA_CONFIG = {
    "port": "3000:3000",
    "provisioning_volume": "./grafana/provisioning:/etc/grafana/provisioning",
    "dashboards_volume": "./grafana/dashboards:/var/lib/grafana/dashboards",
    "network": "streaming-network"
}


# =============================================================================
# Helper Functions
# =============================================================================

def load_docker_compose():
    """Load and parse the docker-compose.yml file."""
    if not DOCKER_COMPOSE_PATH.exists():
        pytest.skip(f"Docker Compose file not found: {DOCKER_COMPOSE_PATH}")
    
    with open(DOCKER_COMPOSE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_grafana_service(config):
    """Extract Grafana service configuration from docker-compose."""
    services = config.get("services", {})
    return services.get("grafana")


# =============================================================================
# Property 5: Docker Compose Grafana Configuration
# Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
# Validates: Requirements 7.1, 7.2, 7.3
# =============================================================================

class TestDockerComposeGrafanaConfiguration:
    """Property tests for Docker Compose Grafana service configuration."""
    
    def test_docker_compose_file_exists(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1, 7.2, 7.3
        
        The docker-compose.yml file must exist.
        """
        assert DOCKER_COMPOSE_PATH.exists(), f"Docker Compose file not found: {DOCKER_COMPOSE_PATH}"
    
    def test_docker_compose_yaml_valid(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1, 7.2, 7.3
        
        The docker-compose.yml file must be valid YAML.
        """
        config = load_docker_compose()
        assert config is not None, "Failed to parse docker-compose.yml"
        assert "services" in config, "Missing 'services' key in config"
    
    def test_grafana_service_exists(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1
        
        WHEN docker-compose up is executed THEN the system SHALL start Grafana container.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found in docker-compose.yml"
    
    def test_grafana_image_configured(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1
        
        Grafana service SHALL use grafana/grafana image.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        image = grafana.get("image", "")
        assert "grafana/grafana" in image, f"Expected grafana/grafana image, got {image}"
    
    def test_grafana_port_3000_exposed(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.3
        
        WHEN Grafana starts THEN the system SHALL expose web UI trên port 3000.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        ports = grafana.get("ports", [])
        
        # Check if port 3000 is mapped
        port_3000_mapped = any("3000" in str(port) for port in ports)
        assert port_3000_mapped, f"Port 3000 not exposed. Ports: {ports}"
    
    def test_grafana_provisioning_volume_mounted(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.2
        
        WHEN Grafana starts THEN the system SHALL auto-provision all dashboards
        từ provisioning directory.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        volumes = grafana.get("volumes", [])
        
        # Check provisioning volume
        provisioning_mounted = any(
            "/etc/grafana/provisioning" in str(vol) 
            for vol in volumes
        )
        assert provisioning_mounted, \
            f"Provisioning volume not mounted. Volumes: {volumes}"
    
    def test_grafana_dashboards_volume_mounted(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.2
        
        Grafana SHALL have dashboards directory mounted for auto-provisioning.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        volumes = grafana.get("volumes", [])
        
        # Check dashboards volume
        dashboards_mounted = any(
            "/var/lib/grafana/dashboards" in str(vol) 
            for vol in volumes
        )
        assert dashboards_mounted, \
            f"Dashboards volume not mounted. Volumes: {volumes}"
    
    def test_grafana_network_configured(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1
        
        Grafana SHALL be connected to streaming-network for communication
        with other services.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        networks = grafana.get("networks", [])
        
        assert "streaming-network" in networks, \
            f"Grafana not connected to streaming-network. Networks: {networks}"
    
    def test_grafana_depends_on_redis(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1
        
        Grafana SHALL depend on Redis service for data source connectivity.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        depends_on = grafana.get("depends_on", {})
        
        # depends_on can be a list or dict
        if isinstance(depends_on, list):
            has_redis = "redis" in depends_on
        else:
            has_redis = "redis" in depends_on
        
        assert has_redis, f"Grafana does not depend on Redis. depends_on: {depends_on}"
    
    @given(volume_type=st.sampled_from(["provisioning", "dashboards"]))
    @settings(max_examples=100)
    def test_required_volumes_mounted(self, volume_type):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.2
        
        For any required volume type (provisioning or dashboards),
        the Grafana service SHALL have it properly mounted.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        volumes = grafana.get("volumes", [])
        
        expected_paths = {
            "provisioning": "/etc/grafana/provisioning",
            "dashboards": "/var/lib/grafana/dashboards"
        }
        
        expected_path = expected_paths[volume_type]
        volume_mounted = any(expected_path in str(vol) for vol in volumes)
        
        assert volume_mounted, \
            f"{volume_type} volume ({expected_path}) not mounted. Volumes: {volumes}"
    
    def test_grafana_plugins_configured(self):
        """
        Feature: grafana-dashboard, Property 5: Docker Compose Grafana Configuration
        Validates: Requirements 7.1
        
        Grafana SHALL have required plugins configured for data sources.
        """
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None, "Grafana service not found"
        environment = grafana.get("environment", {})
        
        plugins = environment.get("GF_INSTALL_PLUGINS", "")
        
        # Check for required plugins
        assert "redis-datasource" in plugins, \
            f"redis-datasource plugin not configured. Plugins: {plugins}"
        assert "marcusolsson-json-datasource" in plugins, \
            f"marcusolsson-json-datasource plugin not configured. Plugins: {plugins}"
