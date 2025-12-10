"""
PySpark Streaming Processor

Real-time data processing pipeline using Apache Spark Structured Streaming
to process cryptocurrency market data from Kafka.

Structure (consolidated):
- core.py: Configuration classes and connector utilities
- trade_aggregation_job.py: Aggregates raw trades into OHLCV candles
- analytics_jobs.py: Technical indicators and anomaly detection jobs
- graceful_shutdown.py: Graceful shutdown utility

Storage Architecture:
- Hot Path: Redis (real-time queries)
- Warm Path: PostgreSQL (90-day analytics)
- Cold Path: MinIO (historical archive)
"""

__version__ = "0.1.0"

# Configuration classes from core.py
from .core import (
    Config,
    KafkaConfig,
    SparkConfig,
    RedisConfig,
    PostgresConfig,
    MinioConfig,
)

# Connectors from core.py
from .core import (
    KafkaConnector,
    RedisConnector,
)

# Job classes
from .trade_aggregation_job import TradeAggregationJob
from .analytics_jobs import (
    TechnicalIndicatorsJob,
    AnomalyDetectionJob,
    IndicatorCalculator,
    CandleState,
)

# Graceful shutdown utility
# Re-export from utils via graceful_shutdown.py for backward compatibility
# (Requirement 11.1: Re-export GracefulShutdown from utils)
from .graceful_shutdown import GracefulShutdown, ShutdownState, ShutdownEvent

__all__ = [
    # Configuration
    "Config",
    "KafkaConfig",
    "SparkConfig",
    "RedisConfig",
    "PostgresConfig",
    "MinioConfig",
    # Connectors
    "KafkaConnector",
    "RedisConnector",
    # Jobs
    "TradeAggregationJob",
    "TechnicalIndicatorsJob",
    "AnomalyDetectionJob",
    # Utilities
    "IndicatorCalculator",
    "CandleState",
    # Graceful shutdown (re-exported from utils)
    "GracefulShutdown",
    "ShutdownState",
    "ShutdownEvent",
]
