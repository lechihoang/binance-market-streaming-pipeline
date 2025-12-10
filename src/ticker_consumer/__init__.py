"""
Ticker Consumer Module

Provides real-time ticker data processing from Kafka to Redis.
Bypasses Spark processing for low-latency ticker updates (~100-200ms).

Components:
- TickerConsumer: Core consumer service
- TickerValidator: Message validation
- LateDataHandler: Late data handling with watermark
- ErrorHandler: Error handling and reconnection
- TickerConsumerConfig: Configuration management

Module Structure (after consolidation):
- core.py: Configuration, Validator, Error Handler
- service.py: Late Data Handler, Health Check, Ticker Consumer
- main.py: CLI entrypoint
"""

# Configuration, Validator, Error Handler from core.py
from src.ticker_consumer.core import (
    TickerConsumerConfig,
    KafkaConsumerConfig,
    RedisConfig,
    LateDataConfig,
    TickerValidator,
    ValidationResult,
    ErrorHandler,
    ErrorType,
    ErrorMetrics,
    KafkaReconnectionHandler,
    DEFAULT_TICKER_SYMBOLS,
)

# Re-export ExponentialBackoff from utils for backward compatibility
# (Requirement 11.2: Keep ExponentialBackoff re-export for backward compatibility)
from src.utils.retry import ExponentialBackoff

# Late Data Handler, Health Check, Ticker Consumer from service.py
from src.ticker_consumer.service import (
    TickerConsumer,
    LateDataHandler,
    LateDataMetrics,
    ProcessingMetrics,
    check_redis_health,
    check_kafka_connectivity,
    health_check_main,
)

__all__ = [
    # Configuration
    "TickerConsumerConfig",
    "KafkaConsumerConfig",
    "RedisConfig",
    "LateDataConfig",
    "DEFAULT_TICKER_SYMBOLS",
    # Validator
    "TickerValidator",
    "ValidationResult",
    # Error Handler
    "ErrorHandler",
    "ErrorType",
    "ErrorMetrics",
    "ExponentialBackoff",
    "KafkaReconnectionHandler",
    # Late Data Handler
    "LateDataHandler",
    "LateDataMetrics",
    # Consumer
    "TickerConsumer",
    "ProcessingMetrics",
    # Health Check
    "check_redis_health",
    "check_kafka_connectivity",
    "health_check_main",
]
