"""Configuration module for Binance-Kafka Connector.

Loads all settings from environment variables with sensible defaults.
"""

import os
from typing import List


class Config:
    """Configuration class that loads settings from environment variables."""

    # WebSocket Configuration
    BINANCE_WS_URL: str = os.getenv(
        "BINANCE_WS_URL", "wss://stream.binance.com:9443/stream"
    )
    BINANCE_STREAMS: List[str] = os.getenv(
        "BINANCE_STREAMS",
        "btcusdt@trade,ethusdt@trade,bnbusdt@trade,btcusdt@kline_1m,ethusdt@kline_1m,btcusdt@ticker",
    ).split(",")
    PING_INTERVAL_SECONDS: int = int(os.getenv("PING_INTERVAL_SECONDS", "180"))
    RECONNECT_MAX_DELAY_SECONDS: int = int(
        os.getenv("RECONNECT_MAX_DELAY_SECONDS", "60")
    )
    WS_CONNECTION_TIMEOUT: int = int(os.getenv("WS_CONNECTION_TIMEOUT", "10"))

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    KAFKA_COMPRESSION: str = os.getenv("KAFKA_COMPRESSION", "snappy")
    KAFKA_ACKS: int = int(os.getenv("KAFKA_ACKS", "1"))

    # Kafka Topics
    TOPIC_RAW_TRADES: str = os.getenv("TOPIC_RAW_TRADES", "raw_trades")
    TOPIC_RAW_KLINES: str = os.getenv("TOPIC_RAW_KLINES", "raw_klines")
    TOPIC_RAW_TICKERS: str = os.getenv("TOPIC_RAW_TICKERS", "raw_tickers")

    # Batching Configuration
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    BATCH_TIMEOUT_MS: int = int(os.getenv("BATCH_TIMEOUT_MS", "100"))
    BATCH_CHECK_INTERVAL_MS: int = int(os.getenv("BATCH_CHECK_INTERVAL_MS", "10"))

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration settings.

        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not cls.BINANCE_WS_URL:
            raise ValueError("BINANCE_WS_URL is required")

        if not cls.BINANCE_STREAMS:
            raise ValueError("BINANCE_STREAMS is required")

        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")

        if cls.BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE must be positive")

        if cls.BATCH_TIMEOUT_MS <= 0:
            raise ValueError("BATCH_TIMEOUT_MS must be positive")

        if cls.PING_INTERVAL_SECONDS <= 0:
            raise ValueError("PING_INTERVAL_SECONDS must be positive")

        if cls.RECONNECT_MAX_DELAY_SECONDS <= 0:
            raise ValueError("RECONNECT_MAX_DELAY_SECONDS must be positive")


# Create a singleton instance
config = Config()
