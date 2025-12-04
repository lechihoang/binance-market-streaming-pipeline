"""
Data Quality Test Framework for Airflow DAGs.

This module provides health checks, field validation, and completeness checks
for data quality testing in Airflow pipelines.
"""

import socket
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable

try:
    from airflow.exceptions import AirflowException
except ImportError:
    # For testing without Airflow installed
    class AirflowException(Exception):
        pass


def log_with_context(task_instance, level: str, message: str, **extra_context) -> None:
    """
    Log message with structured context including timestamp and task information.
    
    Args:
        task_instance: Airflow task instance
        level: Log level ('info', 'warning', 'error', 'debug')
        message: Log message
        extra_context: Additional context to include in log
        
    Example:
        >>> log_with_context(task_instance, 'info', 'Processing started', records=100)
    """
    if not task_instance:
        return
    
    # Build structured log context
    log_context = {
        'timestamp': datetime.now().isoformat(),
        'task_id': task_instance.task_id,
        'dag_id': task_instance.dag_id,
        'execution_date': str(task_instance.execution_date) if hasattr(task_instance, 'execution_date') else None,
        'try_number': task_instance.try_number if hasattr(task_instance, 'try_number') else None,
    }
    
    # Add extra context
    log_context.update(extra_context)
    
    # Format log message with context
    context_str = ' | '.join([f"{k}={v}" for k, v in log_context.items() if v is not None])
    formatted_message = f"[{context_str}] {message}"
    
    # Log at appropriate level
    logger = task_instance.log
    if level == 'info':
        logger.info(formatted_message)
    elif level == 'warning':
        logger.warning(formatted_message)
    elif level == 'error':
        logger.error(formatted_message)
    elif level == 'debug':
        logger.debug(formatted_message)
    else:
        logger.info(formatted_message)


def push_task_metrics(task_instance, metrics: Dict[str, Any], key: str = 'task_metrics') -> None:
    """
    Push task execution metrics to XCom with timestamp and context.
    
    Args:
        task_instance: Airflow task instance
        metrics: Dictionary of metrics to push
        key: XCom key for storing metrics
        
    Example:
        >>> push_task_metrics(task_instance, {'records_processed': 100, 'execution_time': 5.2})
    """
    if not task_instance:
        return
    
    # Add timestamp and context to metrics
    enriched_metrics = {
        'timestamp': datetime.now().isoformat(),
        'task_id': task_instance.task_id,
        'dag_id': task_instance.dag_id,
        'execution_date': str(task_instance.execution_date) if hasattr(task_instance, 'execution_date') else None,
        **metrics
    }
    
    task_instance.xcom_push(key=key, value=enriched_metrics)


def health_check_service(
    service_name: str,
    host: str,
    port: int,
    max_retries: int = 3,
    **context
) -> Dict[str, Any]:
    """
    Generic health check with exponential backoff retry.
    
    Args:
        service_name: Name of service (Kafka, Redis, Database, etc.)
        host: Service hostname or IP address
        port: Service port number
        max_retries: Maximum number of retry attempts (default: 3)
        context: Airflow context dictionary
        
    Returns:
        Dictionary containing health check status and metrics:
        - service: Service name
        - status: 'healthy' if check passed
        - attempt: Number of attempts taken
        - timestamp: ISO format timestamp
        
    Raises:
        Exception: If health check fails after all retry attempts
        
    Example:
        >>> health_check_service('kafka', 'localhost', 9092, max_retries=3, **context)
        {'service': 'kafka', 'status': 'healthy', 'attempt': 1, 'timestamp': '2024-01-01T12:00:00'}
    """
    task_instance = context.get('task_instance')
    
    log_with_context(
        task_instance, 'info',
        f"Starting health check for {service_name}",
        service=service_name, host=host, port=port, max_retries=max_retries
    )
    
    for attempt in range(max_retries):
        try:
            # Create socket connection to test service availability
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                # Connection successful
                metrics = {
                    'service': service_name,
                    'status': 'healthy',
                    'attempt': attempt + 1,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Push metrics to XCom with structured logging
                if task_instance:
                    task_instance.xcom_push(
                        key=f'{service_name}_health',
                        value=metrics
                    )
                    log_with_context(
                        task_instance, 'info',
                        f"Health check passed for {service_name}",
                        service=service_name, attempt=attempt + 1, status='healthy'
                    )
                
                return metrics
                
        except Exception as e:
            if attempt == max_retries - 1:
                # Last attempt failed
                log_with_context(
                    task_instance, 'error',
                    f"Health check failed for {service_name} after all retries",
                    service=service_name, max_retries=max_retries, error=str(e)
                )
                raise Exception(f"Health check failed for {service_name} after {max_retries} attempts: {str(e)}")
            
            # Exponential backoff: 2^attempt seconds
            delay = 2 ** attempt
            log_with_context(
                task_instance, 'warning',
                f"Health check attempt failed, retrying",
                service=service_name, attempt=attempt + 1, retry_delay=delay, error=str(e)
            )
            time.sleep(delay)
    
    # Should not reach here, but just in case
    raise Exception(f"Health check failed for {service_name}")


def validate_fields(
    data: List[Dict[str, Any]],
    schema: Dict[str, type],
    **context
) -> Dict[str, Any]:
    """
    Validate data fields against schema definition.
    
    Args:
        data: List of records (dictionaries) to validate
        schema: Schema definition mapping field names to expected types
        context: Airflow context dictionary
        
    Returns:
        Dictionary containing validation results:
        - total_records: Total number of records checked
        - valid_records: Number of valid records
        - violations: Number of violations found
        - timestamp: ISO format timestamp
        
    Raises:
        AirflowException: If validation fails (violations found)
        
    Example:
        >>> schema = {'price': float, 'quantity': float, 'symbol': str}
        >>> data = [{'price': 100.0, 'quantity': 10.0, 'symbol': 'BTCUSDT'}]
        >>> validate_fields(data, schema, **context)
        {'total_records': 1, 'valid_records': 1, 'violations': 0, 'timestamp': '...'}
    """
    task_instance = context.get('task_instance')
    
    log_with_context(
        task_instance, 'info',
        f"Starting field validation",
        total_records=len(data), schema_fields=list(schema.keys())
    )
    
    violations = []
    
    for idx, record in enumerate(data):
        for field_name, field_type in schema.items():
            # Check if field exists
            if field_name not in record:
                violations.append({
                    'record_index': idx,
                    'field': field_name,
                    'error': 'missing_field',
                    'record': record
                })
                continue
            
            # Check field type
            value = record[field_name]
            if not isinstance(value, field_type):
                violations.append({
                    'record_index': idx,
                    'field': field_name,
                    'error': 'type_mismatch',
                    'expected': field_type.__name__,
                    'actual': type(value).__name__,
                    'record': record
                })
    
    # Prepare metrics
    metrics = {
        'total_records': len(data),
        'valid_records': len(data) - len(violations),
        'violations': len(violations),
        'timestamp': datetime.now().isoformat()
    }
    
    if violations:
        # Log violations (first 10) with structured logging
        log_with_context(
            task_instance, 'error',
            f"Field validation failed",
            total_violations=len(violations), total_records=len(data)
        )
        
        if task_instance:
            for i, violation in enumerate(violations[:10]):
                task_instance.log.error(f"Violation {i+1}: {violation}")
            
            if len(violations) > 10:
                task_instance.log.error(
                    f"... and {len(violations) - 10} more violations"
                )
        
        raise AirflowException(
            f"Field validation failed with {len(violations)} violations"
        )
    
    # Push metrics to XCom on success
    if task_instance:
        task_instance.xcom_push(key='validation_metrics', value=metrics)
        log_with_context(
            task_instance, 'info',
            f"Field validation passed",
            valid_records=metrics['valid_records'], total_records=metrics['total_records']
        )
    
    return metrics


def check_completeness(
    data: List[Dict[str, Any]],
    required_fields: List[str],
    **context
) -> Dict[str, Any]:
    """
    Check data completeness for required fields.
    
    Args:
        data: List of records (dictionaries) to check
        required_fields: List of field names that must not be null or empty
        context: Airflow context dictionary
        
    Returns:
        Dictionary containing completeness metrics:
        - total_records: Total number of records checked
        - complete_records: Number of complete records
        - incomplete_records: Number of incomplete records
        - null_counts: Dictionary mapping field names to null counts
        - completeness_pct: Percentage of complete records
        - timestamp: ISO format timestamp
        
    Raises:
        AirflowException: If completeness check fails (incomplete records found)
        
    Example:
        >>> data = [{'price': 100.0, 'quantity': 10.0}]
        >>> check_completeness(data, ['price', 'quantity'], **context)
        {'total_records': 1, 'complete_records': 1, 'incomplete_records': 0, ...}
    """
    task_instance = context.get('task_instance')
    
    log_with_context(
        task_instance, 'info',
        f"Starting completeness check",
        total_records=len(data), required_fields=required_fields
    )
    
    total_records = len(data)
    null_counts = {field: 0 for field in required_fields}
    incomplete_records = []
    
    for idx, record in enumerate(data):
        has_nulls = False
        for field in required_fields:
            value = record.get(field)
            if value is None or value == '':
                null_counts[field] += 1
                has_nulls = True
        
        if has_nulls:
            incomplete_records.append({
                'record_index': idx,
                'record': record
            })
    
    # Calculate completeness percentage
    completeness_pct = 0.0
    if total_records > 0:
        completeness_pct = (total_records - len(incomplete_records)) / total_records * 100
    
    # Prepare metrics
    metrics = {
        'total_records': total_records,
        'complete_records': total_records - len(incomplete_records),
        'incomplete_records': len(incomplete_records),
        'null_counts': null_counts,
        'completeness_pct': completeness_pct,
        'timestamp': datetime.now().isoformat()
    }
    
    if incomplete_records:
        # Log incomplete records (first 10) with structured logging
        log_with_context(
            task_instance, 'error',
            f"Completeness check failed",
            incomplete_records=len(incomplete_records), total_records=total_records,
            completeness_pct=completeness_pct
        )
        
        if task_instance:
            for i, record in enumerate(incomplete_records[:10]):
                task_instance.log.error(f"Incomplete record {i+1}: {record}")
            
            if len(incomplete_records) > 10:
                task_instance.log.error(
                    f"... and {len(incomplete_records) - 10} more incomplete records"
                )
        
        raise AirflowException(
            f"Completeness check failed: {len(incomplete_records)} incomplete records"
        )
    
    # Push metrics to XCom on success
    if task_instance:
        task_instance.xcom_push(key='completeness_metrics', value=metrics)
        log_with_context(
            task_instance, 'info',
            f"Completeness check passed",
            completeness_pct=completeness_pct, complete_records=metrics['complete_records']
        )
    
    return metrics


def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Callback executed when any task fails.
    
    Logs failure details including stack trace for debugging.
    Can be extended for alerting (Slack, email, etc.).
    
    Args:
        context: Airflow context dictionary containing task instance and exception
        
    Example:
        In DAG definition:
        >>> default_args = {
        ...     'on_failure_callback': on_failure_callback
        ... }
    """
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    
    if task_instance:
        # Use structured logging for failure
        log_with_context(
            task_instance, 'error',
            f"Task failed",
            exception_type=type(exception).__name__,
            exception_message=str(exception)
        )
        
        # Log full stack trace with timestamp and context
        stack_trace = traceback.format_exc()
        task_instance.log.error(f"Stack trace:\n{stack_trace}")


def handle_validation_failure(
    violations: List[Dict[str, Any]],
    **context
) -> None:
    """
    Handle validation failures by logging details and pushing metrics.
    
    Logs first 10 violations in detail, pushes failure metrics to XCom,
    then raises AirflowException to stop pipeline.
    
    Args:
        violations: List of violation dictionaries
        context: Airflow context dictionary
        
    Raises:
        AirflowException: Always raises to stop pipeline execution
        
    Example:
        >>> violations = [{'record_index': 0, 'field': 'price', 'error': 'missing_field'}]
        >>> handle_validation_failure(violations, **context)
    """
    task_instance = context.get('task_instance')
    
    if task_instance:
        # Use structured logging for validation failure
        log_with_context(
            task_instance, 'error',
            f"Validation failed",
            total_violations=len(violations)
        )
        
        # Log first 10 violations
        for i, violation in enumerate(violations[:10]):
            task_instance.log.error(f"Violation {i+1}: {violation}")
        
        if len(violations) > 10:
            task_instance.log.error(
                f"... and {len(violations) - 10} more violations"
            )
        
        # Push failure metrics to XCom before raising exception
        task_instance.xcom_push(
            key='validation_failure_metrics',
            value={
                'total_violations': len(violations),
                'timestamp': datetime.now().isoformat()
            }
        )
    
    raise AirflowException(f"Validation failed with {len(violations)} violations")
