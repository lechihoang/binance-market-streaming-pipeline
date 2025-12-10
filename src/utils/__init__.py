"""
Shared utilities module for the data pipeline application.

This module provides centralized, reusable utilities extracted from various
components to reduce code duplication and ensure consistent behavior across
the application.

Utilities provided:
-------------------

**Retry Utilities** (retry.py):
    - ExponentialBackoff: Calculator for backoff delays with jitter
    - RetryConfig: Configuration dataclass for retry behavior
    - retry_operation(): Function wrapper for retrying callables
    - retry_operation_async(): Async version of retry_operation
    - @with_retry: Decorator for adding retry logic to functions

**Configuration Utilities** (config.py):
    - get_env_str(): Get string from environment variable
    - get_env_int(): Get integer from environment variable
    - get_env_float(): Get float from environment variable
    - get_env_bool(): Get boolean from environment variable
    - get_env_list(): Get list from environment variable
    - get_env_optional(): Get optional string from environment variable

**Logging Utilities** (logging.py):
    - setup_logging(): Configure logging for the application
    - get_logger(): Get a configured logger by name

**Metrics Utilities** (metrics.py):
    - REQUESTS_TOTAL, ERRORS_TOTAL, LATENCY_HISTOGRAM: Shared metrics
    - CONNECTIONS_ACTIVE, MESSAGES_PROCESSED: Connection/processing metrics
    - RETRY_ATTEMPTS: Retry tracking metric
    - record_request(), record_error(), record_latency(): Helper functions
    - track_latency: Context manager for latency tracking
    - @track_latency_decorator: Decorator for function latency tracking

**Graceful Shutdown Utilities** (shutdown.py):
    - GracefulShutdown: Handler for shutdown signals
    - ShutdownState: Dataclass for tracking shutdown state
    - ShutdownEvent: Dataclass for shutdown events

Usage:
------
    from src.utils import (
        # Retry utilities
        ExponentialBackoff,
        RetryConfig,
        retry_operation,
        with_retry,
        # Config utilities
        get_env_str,
        get_env_int,
        get_env_bool,
        get_env_list,
        # Logging utilities
        setup_logging,
        get_logger,
        # Metrics utilities
        record_request,
        record_error,
        track_latency,
        # Shutdown utilities
        GracefulShutdown,
        ShutdownState,
    )

Requirements Coverage:
---------------------
    - Requirement 1: Retry Utilities
    - Requirement 2: Configuration Utilities
    - Requirement 3: Connection Utilities
    - Requirement 4: Logging Utilities
    - Requirement 5: Prometheus Metrics Utilities
    - Requirement 6: Graceful Shutdown Utilities
"""

# Import retry utilities
from src.utils.retry import (
    ExponentialBackoff,
    RetryConfig,
    retry_operation,
    retry_operation_async,
    with_retry,
    with_retry_async,
)

# Import config utilities
from src.utils.config import (
    get_env_str,
    get_env_int,
    get_env_float,
    get_env_bool,
    get_env_list,
    get_env_optional,
)

# Import logging utilities
from src.utils.logging import (
    setup_logging,
    get_logger,
    JSONFormatter,
)

# Import metrics utilities
from src.utils.metrics import (
    # Metrics definitions
    REQUESTS_TOTAL,
    ERRORS_TOTAL,
    LATENCY_HISTOGRAM,
    CONNECTIONS_ACTIVE,
    MESSAGES_PROCESSED,
    PROCESSING_LATENCY,
    RETRY_ATTEMPTS,
    PROMETHEUS_AVAILABLE,
    # Helper functions
    record_request,
    record_error,
    record_latency,
    record_message_processed,
    record_retry,
    set_active_connections,
    increment_active_connections,
    decrement_active_connections,
    # Latency tracking utilities
    track_latency,
    track_latency_decorator,
    track_processing_latency,
    track_processing_latency_decorator,
)

# Import shutdown utilities
from src.utils.shutdown import (
    GracefulShutdown,
    ShutdownState,
    ShutdownEvent,
)

# Public exports
__all__ = [
    # Retry utilities
    "ExponentialBackoff",
    "RetryConfig",
    "retry_operation",
    "retry_operation_async",
    "with_retry",
    "with_retry_async",
    # Config utilities
    "get_env_str",
    "get_env_int",
    "get_env_float",
    "get_env_bool",
    "get_env_list",
    "get_env_optional",
    # Logging utilities
    "setup_logging",
    "get_logger",
    "JSONFormatter",
    # Metrics definitions
    "REQUESTS_TOTAL",
    "ERRORS_TOTAL",
    "LATENCY_HISTOGRAM",
    "CONNECTIONS_ACTIVE",
    "MESSAGES_PROCESSED",
    "PROCESSING_LATENCY",
    "RETRY_ATTEMPTS",
    "PROMETHEUS_AVAILABLE",
    # Metrics helper functions
    "record_request",
    "record_error",
    "record_latency",
    "record_message_processed",
    "record_retry",
    "set_active_connections",
    "increment_active_connections",
    "decrement_active_connections",
    # Latency tracking utilities
    "track_latency",
    "track_latency_decorator",
    "track_processing_latency",
    "track_processing_latency_decorator",
    # Shutdown utilities
    "GracefulShutdown",
    "ShutdownState",
    "ShutdownEvent",
]
