"""Binance-Kafka Connector: Real-time cryptocurrency data ingestion pipeline.

This package provides components for ingesting real-time cryptocurrency data
from Binance WebSocket API and producing to Kafka topics.

Consolidated Structure:
- core.py: Configuration, data models, logging
- client.py: WebSocket client, message processor
- producer.py: Message batcher, Kafka producer
- connector.py: Main orchestrator
"""

__version__ = "0.1.0"

# Main orchestrator
from .connector import BinanceKafkaConnector

# Configuration and models (from core.py)
from .core import (
    Config,
    config,
    TradeMessage,
    KlineMessage,
    KlineData,
    TickerMessage,
    EnrichedMessage,
    JSONFormatter,
    setup_structured_logging,
)

# Client components (from client.py)
from .client import BinanceWebSocketClient, MessageProcessor

# Producer components (from producer.py)
from .producer import MessageBatcher, KafkaProducerClient

__all__ = [
    # Main
    "BinanceKafkaConnector",
    # Config
    "Config",
    "config",
    # Models
    "TradeMessage",
    "KlineMessage",
    "KlineData",
    "TickerMessage",
    "EnrichedMessage",
    # Logging
    "JSONFormatter",
    "setup_structured_logging",
    # Client
    "BinanceWebSocketClient",
    "MessageProcessor",
    # Producer
    "MessageBatcher",
    "KafkaProducerClient",
]
