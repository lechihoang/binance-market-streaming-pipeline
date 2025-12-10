"""Main application orchestrator for Binance-Kafka Connector.

This module provides the BinanceKafkaConnector class that coordinates all
components and manages the application lifecycle.
"""

import asyncio
import logging
import signal
from typing import Optional

from .core import config
from .client import BinanceWebSocketClient, MessageProcessor
from .producer import MessageBatcher, KafkaProducerClient

logger = logging.getLogger(__name__)


class BinanceKafkaConnector:
    """Main application orchestrator that coordinates all components.
    
    Manages the lifecycle of:
    - WebSocket client for receiving messages
    - Message processor for parsing and enrichment
    - Message batcher for accumulating messages
    - Kafka producer for sending to Kafka
    """
    
    def __init__(self):
        """Initialize the connector with all components.
        
        Loads configuration from environment variables and initializes
        all components needed for the data pipeline.
        """
        # Validate configuration
        config.validate()
        
        # Initialize components
        self.websocket_client = BinanceWebSocketClient(
            url=config.BINANCE_WS_URL,
            streams=config.BINANCE_STREAMS
        )
        self.processor = MessageProcessor()
        self.batcher = MessageBatcher(
            batch_size=config.BATCH_SIZE,
            batch_timeout_ms=config.BATCH_TIMEOUT_MS
        )
        self.kafka_producer = KafkaProducerClient(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
        
        # Shutdown flag
        self.shutdown_flag = False
        
        # Background tasks
        self.receiver_task: Optional[asyncio.Task] = None
        self.flush_task: Optional[asyncio.Task] = None
        
        logger.info("BinanceKafkaConnector initialized successfully")
        logger.info(f"Configuration: WS_URL={config.BINANCE_WS_URL}, "
                   f"STREAMS={config.BINANCE_STREAMS}, "
                   f"KAFKA_SERVERS={config.KAFKA_BOOTSTRAP_SERVERS}, "
                   f"BATCH_SIZE={config.BATCH_SIZE}, "
                   f"BATCH_TIMEOUT_MS={config.BATCH_TIMEOUT_MS}")

    async def _message_processing_loop(self) -> None:
        """Background task that receives and processes messages from WebSocket.
        
        This loop:
        1. Receives raw JSON messages from WebSocket
        2. Parses and validates using MessageProcessor
        3. Adds validated messages to MessageBatcher
        4. Handles errors without crashing
        
        Runs until shutdown_flag is set.
        """
        logger.info("Starting message processing loop")
        
        try:
            async for raw_message in self.websocket_client.receive_messages():
                # Check shutdown flag
                if self.shutdown_flag:
                    logger.info("Shutdown flag set, stopping message processing loop")
                    break
                
                try:
                    # Parse the message
                    parsed_data = self.processor.parse_message(raw_message)
                    if parsed_data is None:
                        # Parsing failed, error already logged in processor
                        continue
                    
                    # Validate the message
                    is_valid, validated_message, stream_type, error_msg = self.processor.validate_message(parsed_data)
                    if not is_valid:
                        # Validation failed, error already logged in processor
                        continue
                    
                    # Determine the target topic
                    topic = self.processor.determine_topic(stream_type)
                    if topic is None:
                        # Unknown stream type, error already logged in processor
                        continue
                    
                    # Enrich the message
                    enriched_message = self.processor.enrich_message(parsed_data, stream_type, topic)
                    
                    # Add to batcher
                    await self.batcher.add_message(topic, enriched_message)
                    
                except Exception as e:
                    # Catch any unexpected errors to prevent loop from crashing
                    logger.error(f"Error processing message: {type(e).__name__}: {e}", exc_info=True)
                    # Continue processing next message
                    continue
        
        except asyncio.CancelledError:
            logger.info("Message processing loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Fatal error in message processing loop: {type(e).__name__}: {e}", exc_info=True)
            raise

    async def _batch_flush_loop(self) -> None:
        """Background task that checks and flushes batches periodically.
        
        This loop:
        1. Checks flush conditions every 10ms (configurable)
        2. Flushes batches that meet size or time threshold
        3. Sends flushed batches to Kafka via KafkaProducerClient
        
        Runs until shutdown_flag is set.
        """
        logger.info(f"Starting batch flush loop with check interval: {config.BATCH_CHECK_INTERVAL_MS}ms")
        
        # Convert milliseconds to seconds for asyncio.sleep
        check_interval_seconds = config.BATCH_CHECK_INTERVAL_MS / 1000.0
        
        try:
            while not self.shutdown_flag:
                try:
                    # Check all known topics for flush conditions
                    # We need to check all topics that might have messages
                    topics_to_check = ['raw_trades', 'raw_klines', 'raw_tickers']
                    
                    for topic in topics_to_check:
                        # Check if this topic should be flushed
                        should_flush = await self.batcher.should_flush(topic)
                        
                        if should_flush:
                            # Flush the topic
                            messages = await self.batcher.flush_topic(topic)
                            
                            if messages:
                                # Send to Kafka
                                await self.kafka_producer.send_batch(topic, messages)
                    
                    # Sleep for the check interval
                    await asyncio.sleep(check_interval_seconds)
                    
                except Exception as e:
                    # Log error but continue the loop
                    logger.error(f"Error in batch flush loop: {type(e).__name__}: {e}", exc_info=True)
                    # Brief sleep before retrying
                    await asyncio.sleep(check_interval_seconds)
                    continue
        
        except asyncio.CancelledError:
            logger.info("Batch flush loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Fatal error in batch flush loop: {type(e).__name__}: {e}", exc_info=True)
            raise

    async def start(self) -> None:
        """Start the connector and all background tasks.
        
        Startup sequence:
        1. Initialize Kafka producer (already done in __init__)
        2. Initialize message processor and batcher (already done in __init__)
        3. Connect WebSocket client
        4. Subscribe to streams
        5. Start receiver loop
        6. Start keepalive loop
        7. Start batch flush loop
        8. Log startup completion with config
        """
        logger.info("Starting BinanceKafkaConnector...")
        
        try:
            # Connect to WebSocket
            logger.info("Connecting to Binance WebSocket...")
            await self.websocket_client.connect()
            
            # Subscribe to streams
            logger.info("Subscribing to streams...")
            await self.websocket_client.subscribe()
            
            # Start keepalive loop
            logger.info("Starting keepalive loop...")
            self.websocket_client.start_keepalive()
            
            # Start message processing loop
            logger.info("Starting message processing loop...")
            self.receiver_task = asyncio.create_task(self._message_processing_loop())
            
            # Start batch flush loop
            logger.info("Starting batch flush loop...")
            self.flush_task = asyncio.create_task(self._batch_flush_loop())
            
            # Log startup completion
            logger.info("=" * 80)
            logger.info("BinanceKafkaConnector started successfully!")
            logger.info(f"WebSocket URL: {config.BINANCE_WS_URL}")
            logger.info(f"Streams: {config.BINANCE_STREAMS}")
            logger.info(f"Kafka Bootstrap Servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"Batch Size: {config.BATCH_SIZE}")
            logger.info(f"Batch Timeout: {config.BATCH_TIMEOUT_MS}ms")
            logger.info(f"Batch Check Interval: {config.BATCH_CHECK_INTERVAL_MS}ms")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to start connector: {type(e).__name__}: {e}", exc_info=True)
            # Clean up on startup failure
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the connector and perform graceful shutdown.
        
        Shutdown sequence:
        1. Set shutdown flag to stop accepting new messages
        2. Cancel background tasks
        3. Flush all pending batches to Kafka
        4. Close Kafka producer (with flush)
        5. Close WebSocket connection
        6. Log shutdown completion
        """
        logger.info("Stopping BinanceKafkaConnector...")
        
        # Set shutdown flag to stop accepting new messages
        self.shutdown_flag = True
        logger.info("Shutdown flag set, stopping message acceptance")
        
        # Cancel background tasks
        tasks_to_cancel = []
        
        if self.receiver_task and not self.receiver_task.done():
            tasks_to_cancel.append(self.receiver_task)
        
        if self.flush_task and not self.flush_task.done():
            tasks_to_cancel.append(self.flush_task)
        
        if tasks_to_cancel:
            logger.info(f"Cancelling {len(tasks_to_cancel)} background tasks...")
            for task in tasks_to_cancel:
                task.cancel()
            
            # Wait for tasks to complete cancellation
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("Background tasks cancelled")
        
        # Flush all pending batches
        logger.info("Flushing all pending batches...")
        try:
            batches = await self.batcher.flush_all()
            
            # Send all flushed batches to Kafka
            for topic, messages in batches.items():
                if messages:
                    logger.info(f"Sending final batch of {len(messages)} messages to topic={topic}")
                    await self.kafka_producer.send_batch(topic, messages)
            
            logger.info("All pending batches flushed successfully")
        except Exception as e:
            logger.error(f"Error flushing pending batches: {type(e).__name__}: {e}", exc_info=True)
        
        # Close Kafka producer (with flush)
        logger.info("Closing Kafka producer...")
        try:
            self.kafka_producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {type(e).__name__}: {e}", exc_info=True)
        
        # Close WebSocket connection
        logger.info("Closing WebSocket connection...")
        try:
            await self.websocket_client.close()
            logger.info("WebSocket connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing WebSocket: {type(e).__name__}: {e}", exc_info=True)
        
        # Log shutdown completion
        logger.info("=" * 80)
        logger.info("BinanceKafkaConnector shutdown completed successfully")
        logger.info("=" * 80)

    async def run(self) -> None:
        """Run the connector with signal handlers for graceful shutdown.
        
        Registers signal handlers for SIGINT (Ctrl+C) and SIGTERM to trigger
        graceful shutdown. Runs until a shutdown signal is received.
        """
        # Set up signal handlers
        loop = asyncio.get_running_loop()
        
        def signal_handler(sig):
            """Handle shutdown signals."""
            logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
            # Create a task to stop the connector
            asyncio.create_task(self.stop())
        
        # Register signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
        
        logger.info("Signal handlers registered for SIGINT and SIGTERM")
        
        # Start the connector
        await self.start()
        
        # Wait for background tasks to complete (they run until shutdown)
        try:
            # Wait for both tasks
            await asyncio.gather(self.receiver_task, self.flush_task)
        except asyncio.CancelledError:
            logger.info("Main run loop cancelled")
        except Exception as e:
            logger.error(f"Error in main run loop: {type(e).__name__}: {e}", exc_info=True)
            raise
