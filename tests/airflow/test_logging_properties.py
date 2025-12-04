"""
Property-based tests for logging and monitoring framework.

**Feature: airflow-simple-orchestration**

These tests use Hypothesis to verify correctness properties for
structured logging and metrics collection.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import MagicMock, patch, call
from datetime import datetime

# Import functions to test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))
from data_quality import (
    log_with_context,
    push_task_metrics,
    on_failure_callback
)


# Strategy for generating task context
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
    
    For any task execution, the logs should include timestamps and task context information
    (task_id, dag_id, execution_date).
    
    This test verifies that:
    1. Logs include timestamp
    2. Logs include task_id
    3. Logs include dag_id
    4. Logs include execution_date
    5. Logs are written at the correct level
    """
    # Create mock task instance
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = context['task_id']
    mock_task_instance.dag_id = context['dag_id']
    mock_task_instance.execution_date = context['execution_date']
    mock_task_instance.try_number = context['try_number']
    mock_task_instance.log = MagicMock()
    
    # Call log_with_context
    log_with_context(mock_task_instance, log_level, message)
    
    # Verify the appropriate log method was called
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
    
    # Verify log message contains required context
    assert context['task_id'] in logged_message, "Log should contain task_id"
    assert context['dag_id'] in logged_message, "Log should contain dag_id"
    assert message in logged_message, "Log should contain original message"
    
    # Verify timestamp is present (check for ISO format pattern)
    assert 'timestamp=' in logged_message, "Log should contain timestamp"
    
    # Verify execution_date is present
    assert 'execution_date=' in logged_message, "Log should contain execution_date"


@settings(max_examples=100)
@given(
    context=task_context_strategy(),
    exception_type=st.sampled_from([ValueError, KeyError, RuntimeError, Exception]),
    exception_message=st.text(min_size=1, max_size=100)
)
def test_property_task_failures_log_stack_traces(context, exception_type, exception_message):
    """
    **Feature: airflow-simple-orchestration, Property 8: Task failures log stack traces**
    **Validates: Requirements 8.2**
    
    For any task that fails with an exception, the system should log the complete
    stack trace to the task logs.
    
    This test verifies that:
    1. Failure callback logs error with context
    2. Stack trace is logged
    3. Exception information is included
    """
    # Create mock task instance
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = context['task_id']
    mock_task_instance.dag_id = context['dag_id']
    mock_task_instance.execution_date = context['execution_date']
    mock_task_instance.try_number = context['try_number']
    mock_task_instance.log = MagicMock()
    
    # Create exception
    exception = exception_type(exception_message)
    
    # Create Airflow context
    airflow_context = {
        'task_instance': mock_task_instance,
        'exception': exception,
        'execution_date': context['execution_date']
    }
    
    # Mock traceback.format_exc to return a predictable stack trace
    with patch('traceback.format_exc') as mock_format_exc:
        mock_format_exc.return_value = f"Traceback (most recent call last):\n  File test.py\n{exception_type.__name__}: {exception_message}"
        
        # Call on_failure_callback
        on_failure_callback(airflow_context)
        
        # Verify error was logged with context
        assert mock_task_instance.log.error.called
        
        # Get all error log calls
        error_calls = [call[0][0] for call in mock_task_instance.log.error.call_args_list]
        
        # Verify at least one call contains task context
        context_logged = any(
            context['task_id'] in msg and context['dag_id'] in msg
            for msg in error_calls
        )
        assert context_logged, "Error log should contain task context (task_id, dag_id)"
        
        # Verify stack trace was logged
        stack_trace_logged = any('Traceback' in msg for msg in error_calls)
        assert stack_trace_logged, "Error log should contain stack trace"
        
        # Verify exception information is in logs
        exception_logged = any(exception_message in msg for msg in error_calls)
        assert exception_logged, "Error log should contain exception message"


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
    
    For any task that completes successfully, the task should write execution metrics
    (execution time, records processed) to XCom.
    
    This test verifies that:
    1. Metrics are pushed to XCom
    2. Metrics include timestamp
    3. Metrics include task context (task_id, dag_id, execution_date)
    4. Metrics include the provided metrics data
    """
    # Create mock task instance
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = context['task_id']
    mock_task_instance.dag_id = context['dag_id']
    mock_task_instance.execution_date = context['execution_date']
    mock_task_instance.try_number = context['try_number']
    
    # Call push_task_metrics
    push_task_metrics(mock_task_instance, metrics, key='test_metrics')
    
    # Verify xcom_push was called
    assert mock_task_instance.xcom_push.called
    
    # Get the call arguments
    call_args = mock_task_instance.xcom_push.call_args
    assert call_args[1]['key'] == 'test_metrics'
    
    # Verify metrics contain required fields
    pushed_metrics = call_args[1]['value']
    
    # Check timestamp is present
    assert 'timestamp' in pushed_metrics, "Metrics should include timestamp"
    
    # Check task context is present
    assert 'task_id' in pushed_metrics, "Metrics should include task_id"
    assert pushed_metrics['task_id'] == context['task_id']
    
    assert 'dag_id' in pushed_metrics, "Metrics should include dag_id"
    assert pushed_metrics['dag_id'] == context['dag_id']
    
    assert 'execution_date' in pushed_metrics, "Metrics should include execution_date"
    
    # Check original metrics are preserved
    assert 'records_processed' in pushed_metrics, "Metrics should include records_processed"
    assert pushed_metrics['records_processed'] == metrics['records_processed']
    
    assert 'execution_time' in pushed_metrics, "Metrics should include execution_time"
    assert pushed_metrics['execution_time'] == metrics['execution_time']
    
    assert 'status' in pushed_metrics, "Metrics should include status"
    assert pushed_metrics['status'] == metrics['status']


@settings(max_examples=100)
@given(
    context=task_context_strategy(),
    extra_context=st.dictionaries(
        keys=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Ll',))),
        values=st.one_of(
            st.integers(min_value=0, max_value=1000),
            st.text(min_size=1, max_size=50),
            st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
        ),
        min_size=0,
        max_size=5
    )
)
def test_property_structured_logging_preserves_extra_context(context, extra_context):
    """
    **Feature: airflow-simple-orchestration, Property 7: Task execution logs include timestamp and context**
    **Validates: Requirements 8.1**
    
    For any task execution with extra context, the structured logging should preserve
    and include the extra context in the log message.
    """
    # Create mock task instance
    mock_task_instance = MagicMock()
    mock_task_instance.task_id = context['task_id']
    mock_task_instance.dag_id = context['dag_id']
    mock_task_instance.execution_date = context['execution_date']
    mock_task_instance.try_number = context['try_number']
    mock_task_instance.log = MagicMock()
    
    # Call log_with_context with extra context
    log_with_context(mock_task_instance, 'info', 'Test message', **extra_context)
    
    # Verify log was called
    assert mock_task_instance.log.info.called
    logged_message = mock_task_instance.log.info.call_args[0][0]
    
    # Verify extra context is in the log message
    for key, value in extra_context.items():
        assert key in logged_message, f"Log should contain extra context key: {key}"
        assert str(value) in logged_message, f"Log should contain extra context value: {value}"
