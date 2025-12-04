"""Kafka producer client for sending enriched messages to Kafka.

This module provides a KafkaProducerClient class that handles message
serialization, partition key computation, and batch sending to Kafka topics.
"""

import logging
from typing import List
import cityhash
import orjson
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .models import EnrichedMessage
from .config import config

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    """Kafka producer client for sending enriched messages.
    
    Handles serialization, partition key computation, and error handling
    for sending messages to Kafka topics.
    """
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize Kafka producer with configuration.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (defaults to config value)
            
        Note:
            We set value_serializer=None and key_serializer=None to handle
            serialization manually. This gives us:
            1. Better error handling - can log and skip individual messages
            2. Flexibility for batch processing - serialize before sending
            3. Easier testing - can test serialization logic independently
            
            Alternative approach would be:
                value_serializer=lambda v: orjson.dumps(v.model_dump())
                key_serializer=lambda k: k  # already bytes
            But this makes error handling harder in batch scenarios.
        """
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        
        # Initialize Kafka producer with configuration
        # We handle serialization manually for better control and error handling
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            compression_type='snappy',
            acks=1,
            value_serializer=None,  # Manual serialization for better error handling
            key_serializer=None,    # Manual key serialization
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
        # Convert symbol to bytes and compute hash
        symbol_bytes = symbol.encode('utf-8')
        hash_value = cityhash.CityHash64(symbol_bytes)
        
        # Convert hash to bytes (8 bytes for 64-bit hash)
        partition_key = hash_value.to_bytes(8, byteorder='big')
        
        return partition_key

    def serialize_message(self, message: EnrichedMessage) -> bytes:
        """Serialize EnrichedMessage to JSON bytes.
        
        Args:
            message: EnrichedMessage to serialize
            
        Returns:
            JSON bytes representation of the message
            
        Raises:
            Exception: If serialization fails
        """
        try:
            # Convert Pydantic model to dict and serialize with orjson
            message_dict = message.model_dump()
            json_bytes = orjson.dumps(message_dict)
            return json_bytes
        except Exception as e:
            # Log serialization error with structured context
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
            
        Note:
            Errors are logged but processing continues. This implements
            at-most-once delivery semantics as per requirements.
        """
        if not messages:
            logger.debug(f"No messages to send to topic {topic}")
            return
        
        logger.debug(f"Sending batch of {len(messages)} messages to topic {topic}")
        
        for message in messages:
            try:
                # Serialize the message
                value_bytes = self.serialize_message(message)
                
                # Compute partition key
                key_bytes = self.compute_partition_key(message.symbol)
                
                # Send to Kafka (non-blocking)
                future = self.producer.send(
                    topic=topic,
                    value=value_bytes,
                    key=key_bytes
                )
                
                # We don't wait for the future to complete (fire and forget)
                # This is intentional for throughput
                
            except Exception as e:
                # Log error with structured context including topic, partition key, and error details
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
                # Continue with next message
                continue
        
        logger.debug(f"Batch send completed for topic {topic}")

    def close(self) -> None:
        """Close the Kafka producer connection.
        
        Flushes all pending messages before closing to ensure no data loss.
        """
        try:
            logger.info("Flushing pending messages before closing Kafka producer")
            # Flush all pending messages (blocks until complete)
            self.producer.flush()
            
            logger.info("Closing Kafka producer connection")
            # Close the producer
            self.producer.close()
            
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            # Log error closing Kafka producer with structured context
            logger.error(
                "Error closing Kafka producer",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e)
                },
                exc_info=True
            )
            raise
