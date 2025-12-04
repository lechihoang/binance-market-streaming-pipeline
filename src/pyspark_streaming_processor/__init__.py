"""
PySpark Streaming Processor

Real-time data processing pipeline using Apache Spark Structured Streaming
to process cryptocurrency market data from Kafka.

Structure (simplified):
- trade_aggregation_job.py: Aggregates raw trades into OHLCV candles
- technical_indicators_job.py: Calculates technical indicators (SMA, EMA, RSI, MACD, BB, ATR)
- anomaly_detection_job.py: Detects all 6 anomaly types (whale, volume spike, price spike, RSI extreme, BB breakout, MACD crossover)
- connectors.py: Connector utilities for Kafka, Redis, DuckDB, Parquet
- config.py: Configuration management
"""

__version__ = "0.1.0"

# Job classes
from .trade_aggregation_job import TradeAggregationJob
from .technical_indicators_job import TechnicalIndicatorsJob
from .anomaly_detection_job import AnomalyDetectionJob

# Connectors
from .connectors import (
    KafkaConnector,
    RedisConnector,
    DuckDBConnector,
    ParquetWriter,
)

# Configuration
from .config import Config

__all__ = [
    # Jobs
    "TradeAggregationJob",
    "TechnicalIndicatorsJob",
    "AnomalyDetectionJob",
    # Connectors
    "KafkaConnector",
    "RedisConnector",
    "DuckDBConnector",
    "ParquetWriter",
    # Config
    "Config",
]
