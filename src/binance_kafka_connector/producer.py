"""
Producer module for Binance-Kafka Connector.

Contains message batching and Kafka production consolidated from:
- batcher.py: Message batching logic with size and time thresholds
- kafka_producer.py: Kafka producer client with serialization

Table of Contents:
- Message Batcher (line ~40)
- Kafka Producer Client (line ~180)
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional

import cityhash
import orjson
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .core import config, EnrichedMessage

# Import metrics utilities from utils module (Requirement 5.3)
from src.utils.metrics import record_message_processed, record_error

logger = logging.getLogger(__name__)


# ============================================================================
# MESSAGE BATCHER
# ============================================================================

class MessageBatcher:
    """Batches messages per topic before sending to Kafka.
    
    Accumulates messages in per-topic buffers and flushes when either:
    - Batch size reaches configured threshold (default 100)
    - Configured timeout has elapsed since first message (default 100ms)
    """
    
    def __init__(self, batch_size: int = 100, batch_timeout_ms: int = 100):
        """Initialize the message batcher.
        
        Args:
            batch_size: Maximum number of messages per batch
            batch_timeout_ms: Maximum time in milliseconds to wait before flushing
        """
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        
        # Per-topic message buffers
        self._batches: Dict[str, List[EnrichedMessage]] = {}
        
        # Per-topic first message timestamps (in milliseconds)
        self._timers: Dict[str, Optional[int]] = {}
        
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()
        
        logger.info(f"MessageBatcher initialized with batch_size={batch_size}, batch_timeout_ms={batch_timeout_ms}")

    async def add_message(self, topic: str, message: EnrichedMessage) -> None:
        """Add a message to the appropriate topic buffer.
        
        Args:
            topic: Kafka topic name
            message: Enriched message to add to batch
        """
        async with self._lock:
            if topic not in self._batches:
                self._batches[topic] = []
                self._timers[topic] = None
            
            if len(self._batches[topic]) == 0:
                self._timers[topic] = int(time.time() * 1000)
            
            self._batches[topic].append(message)
            logger.debug(f"Added message to topic={topic}, batch_size={len(self._batches[topic])}")

    async def should_flush(self, topic: str) -> bool:
        """Check if a topic's batch should be flushed.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            True if batch should be flushed
        """
        async with self._lock:
            if topic not in self._batches or len(self._batches[topic]) == 0:
                return False
            
            batch = self._batches[topic]
            first_message_time = self._timers[topic]
            
            # Check size threshold
            if len(batch) >= self.batch_size:
                logger.debug(f"Batch for topic={topic} reached size threshold: {len(batch)}")
                return True
            
            # Check time threshold
            if first_message_time is not None:
                current_time = int(time.time() * 1000)
                elapsed_ms = current_time - first_message_time
                
                if elapsed_ms >= self.batch_timeout_ms:
                    logger.debug(f"Batch for topic={topic} reached time threshold: {elapsed_ms}ms")
                    return True
            
            return False

    async def flush_topic(self, topic: str) -> List[EnrichedMessage]:
        """Flush and return all messages for a specific topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            List of messages that were in the batch
        """
        async with self._lock:
            messages = self._batches.get(topic, [])
            self._batches[topic] = []
            self._timers[topic] = None
            
            if len(messages) > 0:
                logger.info(f"Flushed {len(messages)} messages from topic={topic}")
            
            return messages

    async def flush_all(self) -> Dict[str, List[EnrichedMessage]]:
        """Flush all topics with pending messages.
        
        Returns:
            Dictionary of topic -> list of messages
        """
        async with self._lock:
            result = {}
            topics_to_flush = [topic for topic, batch in self._batches.items() if len(batch) > 0]
            
            for topic in topics_to_flush:
                messages = self._batches[topic]
                result[topic] = messages
                self._batches[topic] = []
                self._timers[topic] = None
                logger.info(f"Flushed {len(messages)} messages from topic={topic} (flush_all)")
            
            total_messages = sum(len(messages) for messages in result.values())
            if total_messages > 0:
                logger.info(f"flush_all completed: flushed {total_messages} messages across {len(result)} topics")
            
            return result


# ============================================================================
# KAFKA PRODUCER CLIENT
# ============================================================================

class KafkaProducerClient:
    """Kafka producer client for sending enriched messages.
    
    Handles serialization, partition key computation, and error handling
    for sending messages to Kafka topics.
    """
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize Kafka producer with configuration.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (defaults to config value)
        """
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            compression_type='snappy',
            acks=1,
            value_serializer=None,
            key_serializer=None,
        )
        
        logger.info(
            f"Initialized KafkaProducerClient with bootstrap_servers={self.bootstrap_servers}, "
            f"compression=snappy, acks=1"
        )

    def compute_partition_key(self, symbol: str) -> bytes:
        """Compute partition key using cityHash64 algorithm.
        
        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)
            
        Returns:
            Hash value as bytes for use as partition key
        """
        symbol_bytes = symbol.encode('utf-8')
        hash_value = cityhash.CityHash64(symbol_bytes)
        partition_key = hash_value.to_bytes(8, byteorder='big')
        return partition_key

    def serialize_message(self, message: EnrichedMessage) -> bytes:
        """Serialize EnrichedMessage to JSON bytes.
        
        Args:
            message: EnrichedMessage to serialize
            
        Returns:
            JSON bytes representation of the message
        """
        try:
            message_dict = message.model_dump()
            json_bytes = orjson.dumps(message_dict)
            return json_bytes
        except Exception as e:
            logger.error(
                "Failed to serialize message",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "symbol": message.symbol,
                    "topic": message.topic
                },
                exc_info=True
            )
            raise

    async def send_batch(self, topic: str, messages: List[EnrichedMessage]) -> None:
        """Send a batch of enriched messages to Kafka.
        
        Args:
            topic: Kafka topic to send to
            messages: List of EnrichedMessage to send
        """
        if not messages:
            logger.debug(f"No messages to send to topic {topic}")
            return
        
        logger.debug(f"Sending batch of {len(messages)} messages to topic {topic}")
        
        for message in messages:
            try:
                value_bytes = self.serialize_message(message)
                key_bytes = self.compute_partition_key(message.symbol)
                
                future = self.producer.send(
                    topic=topic,
                    value=value_bytes,
                    key=key_bytes
                )
                
                # Record successful message processing (Requirement 5.3)
                record_message_processed("binance_connector", topic, "success")
                
            except Exception as e:
                logger.error(
                    "Failed to send message to Kafka",
                    extra={
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "topic": topic,
                        "partition_key": self.compute_partition_key(message.symbol).hex(),
                        "symbol": message.symbol,
                        "stream_type": message.stream_type
                    },
                    exc_info=True
                )
                # Record failed message processing (Requirement 5.3)
                record_message_processed("binance_connector", topic, "error")
                record_error("binance_connector", "kafka_send_error", "error")
                continue
        
        logger.debug(f"Batch send completed for topic {topic}")

    def close(self) -> None:
        """Close the Kafka producer connection."""
        try:
            logger.info("Flushing pending messages before closing Kafka producer")
            self.producer.flush()
            logger.info("Closing Kafka producer connection")
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(
                "Error closing Kafka producer",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e)
                },
                exc_info=True
            )
            raise
