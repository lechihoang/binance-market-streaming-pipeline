"""
Prometheus metrics utilities for production monitoring.

Provides:
- Shared metrics registry to avoid duplicate registration
- Pre-defined metrics for common patterns (requests, errors, latency)
- Helper functions for recording metrics
- Labels for multi-dimensional metrics

Usage:
    from src.utils.metrics import (
        record_request, record_error, record_latency,
        REQUESTS_TOTAL, ERRORS_TOTAL, LATENCY_HISTOGRAM
    )

Requirements Coverage:
---------------------
    - Requirement 5.1: Shared Counter, Gauge, and Histogram metrics
    - Requirement 5.2: Consistent naming convention (app_*_total, app_*_seconds)
    - Requirement 5.3: Helper functions for recording metrics
    - Requirement 5.4: Latency tracking utilities
"""

from contextlib import contextmanager
from functools import wraps
import time
from typing import Optional, Callable, TypeVar, Any

# Try to import prometheus_client, provide stubs if not available
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Provide stub classes when prometheus_client is not installed
    class _StubMetric:
        """Stub metric that does nothing when prometheus_client is not available."""
        def __init__(self, *args, **kwargs):
            pass
        
        def labels(self, **kwargs):
            return self
        
        def inc(self, amount: float = 1) -> None:
            pass
        
        def dec(self, amount: float = 1) -> None:
            pass
        
        def set(self, value: float) -> None:
            pass
        
        def observe(self, amount: float) -> None:
            pass
    
    Counter = _StubMetric
    Gauge = _StubMetric
    Histogram = _StubMetric


T = TypeVar("T")


# ============================================================================
# SHARED METRICS DEFINITIONS
# ============================================================================

# Request metrics
REQUESTS_TOTAL = Counter(
    'app_requests_total',
    'Total number of requests',
    ['service', 'endpoint', 'method', 'status']
)

# Error metrics
ERRORS_TOTAL = Counter(
    'app_errors_total',
    'Total number of errors',
    ['service', 'error_type', 'severity']
)

# Latency metrics
LATENCY_HISTOGRAM = Histogram(
    'app_latency_seconds',
    'Request latency in seconds',
    ['service', 'operation'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Connection metrics
CONNECTIONS_ACTIVE = Gauge(
    'app_connections_active',
    'Number of active connections',
    ['service', 'backend']
)

# Processing metrics
MESSAGES_PROCESSED = Counter(
    'app_messages_processed_total',
    'Total messages processed',
    ['service', 'topic', 'status']
)

PROCESSING_LATENCY = Histogram(
    'app_processing_latency_ms',
    'Message processing latency in milliseconds',
    ['service', 'operation'],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000]
)

# Retry metrics
RETRY_ATTEMPTS = Counter(
    'app_retry_attempts_total',
    'Total retry attempts',
    ['service', 'operation', 'result']
)


# ============================================================================
# HELPER FUNCTIONS FOR RECORDING METRICS
# ============================================================================

def record_request(
    service: str,
    endpoint: str,
    method: str = "GET",
    status: str = "success"
) -> None:
    """Record a request metric.
    
    Args:
        service: Name of the service (e.g., "api", "storage")
        endpoint: The endpoint being called (e.g., "/health", "/query")
        method: HTTP method (e.g., "GET", "POST")
        status: Request status ("success", "error", "timeout")
    """
    REQUESTS_TOTAL.labels(
        service=service,
        endpoint=endpoint,
        method=method,
        status=status
    ).inc()


def record_error(
    service: str,
    error_type: str,
    severity: str = "error"
) -> None:
    """Record an error metric.
    
    Args:
        service: Name of the service (e.g., "api", "storage")
        error_type: Type of error (e.g., "connection_error", "timeout")
        severity: Error severity ("warning", "error", "critical")
    """
    ERRORS_TOTAL.labels(
        service=service,
        error_type=error_type,
        severity=severity
    ).inc()


def record_latency(
    service: str,
    operation: str,
    latency_seconds: float
) -> None:
    """Record a latency metric.
    
    Args:
        service: Name of the service (e.g., "api", "storage")
        operation: The operation being measured (e.g., "query", "write")
        latency_seconds: Latency in seconds
    """
    LATENCY_HISTOGRAM.labels(
        service=service,
        operation=operation
    ).observe(latency_seconds)


def record_message_processed(
    service: str,
    topic: str,
    status: str = "success"
) -> None:
    """Record a message processing metric.
    
    Args:
        service: Name of the service (e.g., "ticker_consumer", "connector")
        topic: The topic/queue being processed
        status: Processing status ("success", "error", "skipped")
    """
    MESSAGES_PROCESSED.labels(
        service=service,
        topic=topic,
        status=status
    ).inc()


def record_retry(
    service: str,
    operation: str,
    result: str
) -> None:
    """Record a retry attempt metric.
    
    Args:
        service: Name of the service (e.g., "storage", "connector")
        operation: The operation being retried (e.g., "connect", "query")
        result: Retry result ("success", "failed", "exhausted")
    """
    RETRY_ATTEMPTS.labels(
        service=service,
        operation=operation,
        result=result
    ).inc()


def set_active_connections(
    service: str,
    backend: str,
    count: int
) -> None:
    """Set the number of active connections.
    
    Args:
        service: Name of the service (e.g., "storage", "api")
        backend: The backend type (e.g., "postgres", "redis", "minio")
        count: Number of active connections
    """
    CONNECTIONS_ACTIVE.labels(
        service=service,
        backend=backend
    ).set(count)


def increment_active_connections(
    service: str,
    backend: str
) -> None:
    """Increment the number of active connections by 1.
    
    Args:
        service: Name of the service
        backend: The backend type
    """
    CONNECTIONS_ACTIVE.labels(
        service=service,
        backend=backend
    ).inc()


def decrement_active_connections(
    service: str,
    backend: str
) -> None:
    """Decrement the number of active connections by 1.
    
    Args:
        service: Name of the service
        backend: The backend type
    """
    CONNECTIONS_ACTIVE.labels(
        service=service,
        backend=backend
    ).dec()


# ============================================================================
# LATENCY TRACKING UTILITIES
# ============================================================================

@contextmanager
def track_latency(service: str, operation: str):
    """Context manager to track operation latency.
    
    Automatically records the latency of the wrapped code block
    to the LATENCY_HISTOGRAM metric.
    
    Args:
        service: Name of the service (e.g., "storage", "api")
        operation: The operation being measured (e.g., "query", "write")
    
    Usage:
        with track_latency("storage", "query"):
            result = db.query(...)
    
    Yields:
        None
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        latency = time.perf_counter() - start
        record_latency(service, operation, latency)


def track_latency_decorator(
    service: str,
    operation: Optional[str] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to track function latency.
    
    Automatically records the latency of the decorated function
    to the LATENCY_HISTOGRAM metric.
    
    Args:
        service: Name of the service (e.g., "storage", "api")
        operation: The operation name. If None, uses the function name.
    
    Returns:
        Decorator function
    
    Usage:
        @track_latency_decorator("storage", "query")
        def query_data():
            ...
        
        # Or use function name as operation:
        @track_latency_decorator("storage")
        def my_operation():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation if operation is not None else func.__name__
        
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            with track_latency(service, op_name):
                return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


@contextmanager
def track_processing_latency(service: str, operation: str):
    """Context manager to track processing latency in milliseconds.
    
    Similar to track_latency but records to PROCESSING_LATENCY
    histogram which uses millisecond buckets.
    
    Args:
        service: Name of the service
        operation: The operation being measured
    
    Usage:
        with track_processing_latency("ticker_consumer", "process_message"):
            process_message(msg)
    
    Yields:
        None
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        latency_ms = (time.perf_counter() - start) * 1000
        PROCESSING_LATENCY.labels(
            service=service,
            operation=operation
        ).observe(latency_ms)


def track_processing_latency_decorator(
    service: str,
    operation: Optional[str] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to track function processing latency in milliseconds.
    
    Args:
        service: Name of the service
        operation: The operation name. If None, uses the function name.
    
    Returns:
        Decorator function
    
    Usage:
        @track_processing_latency_decorator("ticker_consumer", "process")
        def process_message(msg):
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation if operation is not None else func.__name__
        
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            with track_processing_latency(service, op_name):
                return func(*args, **kwargs)
        
        return wrapper
    
    return decorator
