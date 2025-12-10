"""
Consolidated test module for Airflow DAGs and Data Quality.
Contains all tests for DAG syntax, health checks, field validation, completeness checks, and logging.

Table of Contents:
- Imports and Setup (line ~20)
- DAG Syntax Tests (line ~80)
- Health Check Tests (line ~200)
- Field Validation Tests (line ~350)
- Completeness Check Tests (line ~500)
- Logging Property Tests (line ~650)
- Integration Tests (line ~800)

Requirements: 6.5
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import pytest
import ast
import os
import sys
import socket
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from hypothesis import given, strategies as st, settings

# Add dags directory to path
dags_path = Path(__file__).parent.parent / 'dags'
sys.path.insert(0, str(dags_path))

# Set environment variables for DAG imports
os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(dags_path))
os.environ.setdefault('AIRFLOW__CORE__UNIT_TEST_MODE', 'True')

# Import data quality functions
from data_quality import (
    health_check_service,
    validate_fields,
    check_completeness,
    on_failure_callback,
    handle_validation_failure,
    log_with_context,
    push_task_metrics,
    AirflowException
)

# Check if airflow is available
try:
    from airflow import DAG
    from airflow.models import DagBag
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    DagBag = None


# ============================================================================
# DAG SYNTAX TESTS
# ============================================================================

class TestStreamingPipelineDAGSyntax:
    """Test streaming_processing_dag.py syntax and structure."""
    
    @pytest.fixture
    def dag_file_path(self):
        """Get path to streaming_processing_dag.py."""
        return dags_path / 'streaming_processing_dag.py'
    
    @pytest.fixture
    def dag_ast(self, dag_file_path):
        """Parse DAG file into AST."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        return ast.parse(content)
    
    def test_dag_file_exists(self, dag_file_path):
        """Test that streaming_processing_dag.py exists."""
        assert dag_file_path.exists(), "streaming_processing_dag.py not found"
    
    def test_dag_file_has_valid_syntax(self, dag_file_path):
        """Test DAG file has valid Python syntax."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        try:
            ast.parse(content)
        except SyntaxError as e:
            pytest.fail(f"DAG file has syntax error: {e}")
    
    def test_dag_imports_required_modules(self, dag_ast):
        """Test DAG imports required Airflow modules."""
        imports = []
        for node in ast.walk(dag_ast):
            if isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        assert 'airflow' in imports or any('airflow' in imp for imp in imports)
        assert any('operators' in imp for imp in imports)
    
    def test_dag_has_health_checks(self, dag_file_path):
        """Test DAG file contains health check tasks for 3-tier storage."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'test_redis_health' in content
        assert 'test_postgres_health' in content
        assert 'test_minio_health' in content
    
    def test_dag_has_run_tasks(self, dag_file_path):
        """Test DAG has run tasks for streaming jobs."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'run_trade_aggregation_job' in content
        assert 'run_technical_indicators_job' in content
        assert 'run_anomaly_detection_job' in content
    
    def test_dag_sets_dependencies(self, dag_file_path):
        """Test DAG sets dependencies between tasks."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert '>>' in content


# ============================================================================
# HEALTH CHECK TESTS
# ============================================================================

class TestHealthCheck:
    """Unit tests for health_check_service function."""
    
    def test_healthy_service_first_attempt(self):
        """Test health check succeeds on first attempt with healthy service."""
        mock_ti = Mock()
        mock_ti.xcom_push = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        with patch('socket.socket') as mock_socket:
            mock_sock_instance = Mock()
            mock_sock_instance.connect_ex.return_value = 0
            mock_socket.return_value = mock_sock_instance
            
            result = health_check_service('kafka', 'localhost', 9092, max_retries=3, **context)
            
            assert result['service'] == 'kafka'
            assert result['status'] == 'healthy'
            assert result['attempt'] == 1
            mock_ti.xcom_push.assert_called_once()
    
    def test_unhealthy_service_all_retries_fail(self):
        """Test health check fails after all retry attempts with unhealthy service."""
        mock_ti = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        with patch('socket.socket') as mock_socket:
            mock_sock_instance = Mock()
            mock_sock_instance.connect_ex.return_value = 1
            mock_socket.return_value = mock_sock_instance
            
            with patch('time.sleep'):
                with pytest.raises(Exception) as exc_info:
                    health_check_service('redis', 'localhost', 6379, max_retries=3, **context)
                
                assert 'Health check failed for redis' in str(exc_info.value)
    
    def test_health_check_succeeds_on_retry(self):
        """Test health check succeeds on second attempt after first failure."""
        mock_ti = Mock()
        mock_ti.xcom_push = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        with patch('socket.socket') as mock_socket:
            mock_sock_instance = Mock()
            mock_sock_instance.connect_ex.side_effect = [1, 0]
            mock_socket.return_value = mock_sock_instance
            
            with patch('time.sleep'):
                result = health_check_service('database', 'localhost', 5432, max_retries=3, **context)
                
                assert result['status'] == 'healthy'
                assert result['attempt'] == 2


# ============================================================================
# FIELD VALIDATION TESTS
# ============================================================================

class TestFieldValidation:
    """Unit tests for validate_fields function."""
    
    def test_validation_with_valid_data(self):
        """Test field validation passes with all valid data."""
        mock_ti = Mock()
        mock_ti.xcom_push = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        data = [
            {'price': 100.0, 'quantity': 10.0, 'symbol': 'BTCUSDT'},
            {'price': 200.0, 'quantity': 5.0, 'symbol': 'ETHUSDT'}
        ]
        schema = {'price': float, 'quantity': float, 'symbol': str}
        
        result = validate_fields(data, schema, **context)
        
        assert result['total_records'] == 2
        assert result['valid_records'] == 2
        assert result['violations'] == 0
        mock_ti.xcom_push.assert_called_once()
    
    def test_validation_with_missing_field(self):
        """Test field validation fails when required field is missing."""
        mock_ti = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        data = [{'price': 100.0, 'symbol': 'BTCUSDT'}]
        schema = {'price': float, 'quantity': float, 'symbol': str}
        
        with pytest.raises(AirflowException) as exc_info:
            validate_fields(data, schema, **context)
        
        assert 'Field validation failed with 1 violations' in str(exc_info.value)
    
    def test_validation_with_type_mismatch(self):
        """Test field validation fails when field type is incorrect."""
        mock_ti = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        data = [{'price': '100.0', 'quantity': 10.0, 'symbol': 'BTCUSDT'}]
        schema = {'price': float, 'quantity': float, 'symbol': str}
        
        with pytest.raises(AirflowException) as exc_info:
            validate_fields(data, schema, **context)
        
        assert 'Field validation failed' in str(exc_info.value)


# ============================================================================
# COMPLETENESS CHECK TESTS
# ============================================================================

class TestCompletenessCheck:
    """Unit tests for check_completeness function."""
    
    def test_completeness_with_complete_data(self):
        """Test completeness check passes with all complete data."""
        mock_ti = Mock()
        mock_ti.xcom_push = Mock()
        mock_ti.log = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        data = [
            {'price': 100.0, 'quantity': 10.0, 'symbol': 'BTCUSDT'},
            {'price': 200.0, 'quantity': 5.0, 'symbol': 'ETHUSDT'}
        ]
        required_fields = ['price', 'quantity', 'symbol']
        
        result = check_completeness(data, required_fields, **context)
        
        assert result['total_records'] == 2
        assert result['complete_records'] == 2
        assert result['incomplete_records'] == 0
        assert result['completeness_pct'] == 100.0
        mock_ti.xcom_push.assert_called_once()
    
    def test_completeness_with_null_values(self):
        """Test completeness check fails when required fields have null values."""
        mock_ti = Mock()
        mock_ti.log = Mock()
        mock_ti.log.error = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        data = [
            {'price': None, 'quantity': 10.0, 'symbol': 'BTCUSDT'},
            {'price': 200.0, 'quantity': None, 'symbol': 'ETHUSDT'}
        ]
        required_fields = ['price', 'quantity']
        
        with pytest.raises(AirflowException) as exc_info:
            check_completeness(data, required_fields, **context)
        
        assert 'Completeness check failed: 2 incomplete records' in str(exc_info.value)
    
    def test_completeness_with_empty_strings(self):
        """Test completeness check fails when required fields have empty strings."""
        mock_ti = Mock()
        mock_ti.log = Mock()
        mock_ti.log.error = Mock()
        mock_ti.task_id = 'test_task'
        mock_ti.dag_id = 'test_dag'
        mock_ti.execution_date = datetime.now()
        
        context = {'task_instance': mock_ti}
        
        data = [
            {'price': 100.0, 'quantity': 10.0, 'symbol': ''},
            {'price': 200.0, 'quantity': 5.0, 'symbol': 'ETHUSDT'}
        ]
        required_fields = ['symbol']
        
        with pytest.raises(AirflowException) as exc_info:
            check_completeness(data, required_fields, **context)
        
        assert 'Completeness check failed: 1 incomplete records' in str(exc_info.value)


# ============================================================================
# HEALTH CHECK PROPERTY TESTS
# ============================================================================

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
    """
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    with patch('socket.socket') as mock_socket_class:
        mock_socket = MagicMock()
        mock_socket.connect_ex.return_value = 0
        mock_socket_class.return_value = mock_socket
        
        result = health_check_service(
            service_name=config['service_name'],
            host=config['host'],
            port=config['port'],
            max_retries=config['max_retries'],
            **context
        )
        
        assert mock_socket.connect_ex.called
        assert result['status'] == 'healthy'
        assert result['service'] == config['service_name']
        assert 'attempt' in result
        mock_task_instance.xcom_push.assert_called_once()


# ============================================================================
# LOGGING PROPERTY TESTS
# ============================================================================

def task_context_strategy():
    """Generate random task context for testing."""
    return st.fixed_dictionaries({
        'task_id': st.text(min_size=1, max_size=30, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        'dag_id': st.text(min_size=1, max_size=30, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        'execution_date': st.datetimes(min_value=datetime(2024, 1, 1), max_value=datetime(2025, 12, 31)),
        'try_number': st.integers(min_value=1, max_value=5)
    })


@settings(max_examples=100)
@given(
    context=task_context_strategy(),
    log_level=st.sampled_from(['info', 'warning', 'error', 'debug']),
    message=st.text(min_size=1, max_size=100)
)
def test_property_task_execution_logs_include_timestamp_and_context(context, log_level, message):
    """
    **Feature: airflow-simple-orchestration, Property 7: Task execution logs include timestamp and context**
    **Validates: Requirements 8.1**
    """
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = context['task_id']
    mock_task_instance.dag_id = context['dag_id']
    mock_task_instance.execution_date = context['execution_date']
    mock_task_instance.try_number = context['try_number']
    mock_task_instance.log = MagicMock()
    
    log_with_context(mock_task_instance, log_level, message)
    
    if log_level == 'info':
        assert mock_task_instance.log.info.called
        logged_message = mock_task_instance.log.info.call_args[0][0]
    elif log_level == 'warning':
        assert mock_task_instance.log.warning.called
        logged_message = mock_task_instance.log.warning.call_args[0][0]
    elif log_level == 'error':
        assert mock_task_instance.log.error.called
        logged_message = mock_task_instance.log.error.call_args[0][0]
    elif log_level == 'debug':
        assert mock_task_instance.log.debug.called
        logged_message = mock_task_instance.log.debug.call_args[0][0]
    
    assert context['task_id'] in logged_message
    assert context['dag_id'] in logged_message
    assert message in logged_message
    assert 'timestamp=' in logged_message


@settings(max_examples=100)
@given(
    context=task_context_strategy(),
    metrics=st.fixed_dictionaries({
        'records_processed': st.integers(min_value=0, max_value=10000),
        'execution_time': st.floats(min_value=0.1, max_value=3600.0, allow_nan=False, allow_infinity=False),
        'status': st.sampled_from(['success', 'completed', 'processed'])
    })
)
def test_property_task_completion_writes_metrics_to_xcom(context, metrics):
    """
    **Feature: airflow-simple-orchestration, Property 9: Task completion writes metrics to XCom**
    **Validates: Requirements 8.3**
    """
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = context['task_id']
    mock_task_instance.dag_id = context['dag_id']
    mock_task_instance.execution_date = context['execution_date']
    mock_task_instance.try_number = context['try_number']
    
    push_task_metrics(mock_task_instance, metrics, key='test_metrics')
    
    assert mock_task_instance.xcom_push.called
    call_args = mock_task_instance.xcom_push.call_args
    assert call_args[1]['key'] == 'test_metrics'
    
    pushed_metrics = call_args[1]['value']
    assert 'timestamp' in pushed_metrics
    assert 'task_id' in pushed_metrics
    assert pushed_metrics['records_processed'] == metrics['records_processed']


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow is not installed")
class TestFullPipelineExecution:
    """Integration tests for full pipeline execution."""
    
    @pytest.fixture
    def streaming_dag(self):
        """Load streaming_processing_dag."""
        try:
            from streaming_processing_dag import dag
            return dag
        except ImportError:
            pytest.skip("streaming_processing_dag not available")
    
    def test_streaming_pipeline_structure(self, streaming_dag):
        """Test that streaming processing DAG has correct structure."""
        assert streaming_dag is not None
        assert streaming_dag.dag_id == 'streaming_processing_dag'
        
        tasks = streaming_dag.tasks
        task_ids = [t.task_id for t in tasks]
        
        assert 'test_redis_health' in task_ids or 'health_checks.test_redis_health' in task_ids
        assert 'run_trade_aggregation_job' in task_ids or 'trade_aggregation.run_trade_aggregation_job' in task_ids


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow is not installed")
class TestAutoDiscovery:
    """Integration tests for DAG auto-discovery."""
    
    def test_dag_bag_loads_existing_dags(self):
        """Test that DagBag can load existing DAGs from dags/ directory."""
        dagbag = DagBag(dag_folder=str(dags_path), include_examples=False)
        
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
        
        dag_ids = list(dagbag.dag_ids)
        assert 'streaming_processing_dag' in dag_ids
    
    def test_new_dag_can_be_discovered(self):
        """Test that a new DAG file can be discovered by DagBag."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dag_content = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_task(**context):
    return "Test task executed"

with DAG(
    dag_id='test_new_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:
    task = PythonOperator(
        task_id='test_task',
        python_callable=test_task,
    )
'''
            
            test_dag_path = os.path.join(temp_dir, 'test_new_dag.py')
            with open(test_dag_path, 'w') as f:
                f.write(test_dag_content)
            
            dagbag = DagBag(dag_folder=temp_dir, include_examples=False)
            
            assert len(dagbag.import_errors) == 0
            assert 'test_new_dag' in dagbag.dag_ids


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow is not installed")
class TestStreamingPipelineDAG:
    """Test suite for streaming_processing_dag structure."""
    
    @pytest.fixture(scope='class')
    def dagbag(self):
        """Load DAGs from dags directory."""
        return DagBag(dag_folder=str(dags_path), include_examples=False)
    
    @pytest.fixture(scope='class')
    def dag(self, dagbag):
        """Get streaming_processing_dag DAG."""
        dag_id = 'streaming_processing_dag'
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
        return dagbag.dags[dag_id]
    
    def test_dag_loaded(self, dagbag):
        """Test that streaming_processing_dag DAG is loaded without errors."""
        assert 'streaming_processing_dag' in dagbag.dags
        assert len(dagbag.import_errors) == 0
    
    def test_dag_has_correct_properties(self, dag):
        """Test DAG has correct configuration."""
        assert dag.dag_id == 'streaming_processing_dag'
        assert dag.schedule_interval == '* * * * *'
        assert dag.catchup is False
        assert 'streaming' in dag.tags
    
    def test_dag_has_no_cycles(self, dag):
        """Test DAG has no circular dependencies."""
        for task in dag.tasks:
            upstream_tasks = task.get_flat_relatives(upstream=True)
            assert task not in upstream_tasks, f"Task {task.task_id} has circular dependency"
