"""
Kafka Connector Module.

Provides KafkaConnector class for writing data to Kafka topics.
Extracted from anomaly_detection_job.py for reuse across streaming jobs.
"""

import json
from typing import Any, Callable, Dict, Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)


class KafkaConnector:
    """
    Kafka connector for writing data to Kafka topics.
    
    Provides lazy producer initialization and simple send/close interface.
    Supports both immediate flush and batching modes.
    
    Example (immediate flush - default):
        connector = KafkaConnector(bootstrap_servers="localhost:9092")
        connector.send(topic="alerts", value={"type": "whale"}, key="BTCUSDT")
        connector.close()
    
    Example (batching mode for high throughput):
        connector = KafkaConnector(
            bootstrap_servers="localhost:9092",
            linger_ms=100,
            batch_size=16384
        )
        connector.send(topic="trades", value=data, key="BTCUSDT")
        connector.close()  # flushes remaining messages
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: Optional[str] = None,
        linger_ms: int = 0,
        batch_size: int = 16384,
        value_serializer: Optional[Callable[[Any], bytes]] = None,
    ):
        """
        Initialize Kafka connector.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated)
            client_id: Optional client ID for producer identification
            linger_ms: Time to wait for batching (0 = no batching, flush immediately)
            batch_size: Max batch size in bytes (default 16KB)
            value_serializer: Custom value serializer (default: JSON)
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.linger_ms = linger_ms
        self.batch_size = batch_size
        self._value_serializer = value_serializer
        self._producer = None
    
    @property
    def producer(self):
        """Get Kafka producer, creating if needed (lazy initialization)."""
        if self._producer is None:
            from kafka import KafkaProducer
            
            serializer = self._value_serializer or (lambda v: json.dumps(v).encode('utf-8'))
            
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                linger_ms=self.linger_ms,
                batch_size=self.batch_size,
                value_serializer=serializer,
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.debug(f"KafkaProducer created: {self.bootstrap_servers}, linger_ms={self.linger_ms}")
        
        return self._producer
    
    def send(self, topic: str, value: Any, key: Optional[str] = None) -> None:
        """
        Send message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            value: Message value (will be serialized by value_serializer)
            key: Optional message key (for partitioning)
        """
        self.producer.send(topic, value=value, key=key)
        # Only flush immediately if not batching
        if self.linger_ms == 0:
            self.producer.flush()
    
    def close(self) -> None:
        """Flush pending messages and close Kafka producer."""
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logger.debug("KafkaProducer closed")
