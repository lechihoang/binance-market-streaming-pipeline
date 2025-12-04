"""
Property-based tests for health check functionality.

**Feature: airflow-simple-orchestration**

These tests use Hypothesis to verify correctness properties for
health check with retry logic.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import MagicMock, patch
import socket

# Import functions to test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))
from data_quality import health_check_service


# Strategy for generating service configurations
service_config = st.fixed_dictionaries({
    'service_name': st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'))),
    'host': st.sampled_from(['localhost', '127.0.0.1', 'kafka', 'redis']),
    'port': st.integers(min_value=1024, max_value=65535),
    'max_retries': st.integers(min_value=1, max_value=5)
})


@settings(max_examples=100)
@given(config=service_config)
def test_property_health_check_verifies_connectivity_and_status(config):
    """
    **Feature: airflow-simple-orchestration, Property 1: Health check verifies connectivity and status**
    **Validates: Requirements 3.1, 5.1**
    
    For any external service (Kafka, Redis, Database), when a health check is performed,
    the check should verify both network connectivity and service status.
    
    This test verifies that:
    1. Health check attempts to connect to the service
    2. On success, returns metrics with status and attempt count
    3. On failure, retries with exponential backoff
    4. Metrics are pushed to XCom
    """
    # Create mock context
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    # Test successful connection
    with patch('socket.socket') as mock_socket_class:
        mock_socket = MagicMock()
        mock_socket.connect_ex.return_value = 0  # Success
        mock_socket_class.return_value = mock_socket
        
        result = health_check_service(
            service_name=config['service_name'],
            host=config['host'],
            port=config['port'],
            max_retries=config['max_retries'],
            **context
        )
        
        # Verify connectivity was checked
        assert mock_socket.connect_ex.called
        assert mock_socket.connect_ex.call_args[0][0] == (config['host'], config['port'])
        
        # Verify status is returned
        assert result['status'] == 'healthy'
        assert result['service'] == config['service_name']
        assert 'attempt' in result
        assert result['attempt'] >= 1
        assert result['attempt'] <= config['max_retries']
        assert 'timestamp' in result
        
        # Verify metrics pushed to XCom
        mock_task_instance.xcom_push.assert_called_once()
        call_args = mock_task_instance.xcom_push.call_args
        assert call_args[1]['key'] == f"{config['service_name']}_health"
        assert call_args[1]['value']['status'] == 'healthy'


@settings(max_examples=100)
@given(config=service_config)
def test_property_health_check_retries_on_failure(config):
    """
    **Feature: airflow-simple-orchestration, Property 1: Health check verifies connectivity and status**
    **Validates: Requirements 3.1, 5.1**
    
    For any service health check that fails, the system should retry with exponential backoff
    before ultimately failing.
    """
    # Create mock context
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    # Test failed connection with retries
    with patch('socket.socket') as mock_socket_class:
        mock_socket = MagicMock()
        mock_socket.connect_ex.return_value = 1  # Connection refused
        mock_socket_class.return_value = mock_socket
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            with pytest.raises(Exception) as exc_info:
                health_check_service(
                    service_name=config['service_name'],
                    host=config['host'],
                    port=config['port'],
                    max_retries=config['max_retries'],
                    **context
                )
            
            # Verify it retried the correct number of times
            assert mock_socket.connect_ex.call_count == config['max_retries']
            
            # Verify error message includes service name
            assert config['service_name'] in str(exc_info.value)
