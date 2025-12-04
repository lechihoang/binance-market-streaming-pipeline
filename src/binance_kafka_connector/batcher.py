"""Message batcher for accumulating messages before sending to Kafka.

This module implements batching logic to optimize Kafka throughput by
accumulating messages and flushing based on size or time thresholds.
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional
from .models import EnrichedMessage


logger = logging.getLogger(__name__)


class MessageBatcher:
    """Batches messages per topic before sending to Kafka.
    
    Accumulates messages in per-topic buffers and flushes when either:
    - Batch size reaches 100 messages
    - 100 milliseconds have elapsed since first message in batch
    """
    
    def __init__(self, batch_size: int = 100, batch_timeout_ms: int = 100):
        """Initialize the message batcher.
        
        Args:
            batch_size: Maximum number of messages per batch (default: 100)
            batch_timeout_ms: Maximum time in milliseconds to wait before flushing (default: 100)
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
        
        Tracks the first message timestamp if the batch is empty.
        
        Args:
            topic: Kafka topic name
            message: Enriched message to add to batch
        """
        async with self._lock:
            # Initialize batch and timer for topic if not exists
            if topic not in self._batches:
                self._batches[topic] = []
                self._timers[topic] = None
            
            # Track first message timestamp if batch is empty
            if len(self._batches[topic]) == 0:
                self._timers[topic] = int(time.time() * 1000)  # Current time in milliseconds
            
            # Add message to batch
            self._batches[topic].append(message)
            
            logger.debug(f"Added message to topic={topic}, batch_size={len(self._batches[topic])}")

    async def should_flush(self, topic: str) -> bool:
        """Check if a topic's batch should be flushed.
        
        Returns True if either:
        - Batch size >= configured batch_size (default 100)
        - Time since first message >= configured batch_timeout_ms (default 100ms)
        
        Args:
            topic: Kafka topic name
            
        Returns:
            True if batch should be flushed, False otherwise
        """
        async with self._lock:
            # Topic doesn't exist or has no messages
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
                current_time = int(time.time() * 1000)  # Current time in milliseconds
                elapsed_ms = current_time - first_message_time
                
                if elapsed_ms >= self.batch_timeout_ms:
                    logger.debug(f"Batch for topic={topic} reached time threshold: {elapsed_ms}ms")
                    return True
            
            return False

    async def flush_topic(self, topic: str) -> List[EnrichedMessage]:
        """Flush and return all messages for a specific topic.
        
        Clears the batch and resets the timer for the topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            List of messages that were in the batch
        """
        async with self._lock:
            # Get messages for topic (or empty list if topic doesn't exist)
            messages = self._batches.get(topic, [])
            
            # Clear the batch
            self._batches[topic] = []
            
            # Reset the timer
            self._timers[topic] = None
            
            if len(messages) > 0:
                logger.info(f"Flushed {len(messages)} messages from topic={topic}")
            
            return messages

    async def flush_all(self) -> Dict[str, List[EnrichedMessage]]:
        """Flush all topics with pending messages.
        
        Returns a dictionary mapping topic names to their flushed messages.
        
        Returns:
            Dictionary of topic -> list of messages
        """
        async with self._lock:
            result = {}
            
            # Get all topics that have messages
            topics_to_flush = [topic for topic, batch in self._batches.items() if len(batch) > 0]
            
            # Flush each topic
            for topic in topics_to_flush:
                messages = self._batches[topic]
                result[topic] = messages
                
                # Clear the batch
                self._batches[topic] = []
                
                # Reset the timer
                self._timers[topic] = None
                
                logger.info(f"Flushed {len(messages)} messages from topic={topic} (flush_all)")
            
            total_messages = sum(len(messages) for messages in result.values())
            if total_messages > 0:
                logger.info(f"flush_all completed: flushed {total_messages} messages across {len(result)} topics")
            
            return result
