"""
Service module for Ticker Consumer.

Contains the core consumer service, late data handling, and health checks.
Consolidates consumer.py, late_data_handler.py, and healthcheck.py.

Table of Contents:
- Late Data Handler (line ~50)
  - LateDataMetrics
  - LateDataHandler
- Health Check Functions (line ~250)
  - check_redis_health
  - check_kafka_connectivity
  - health_check_main
- Ticker Consumer (line ~350)
  - ProcessingMetrics
  - TickerConsumer

Requirements:
- 1.1: Write data to Redis within 100 milliseconds
- 1.4: Redis unavailable - log error and continue without crashing
- 2.1: Operate as standalone service
- 2.2: Read from Kafka using dedicated consumer group
- 2.3: Begin consuming from latest offset
- 2.4: Kafka connection loss - attempt reconnection with exponential backoff
- 5.3: Ignore messages for non-configured symbols
- 7.1-7.3: Validate ticker messages before storage

Properties validated:
- Property 8: Non-configured symbols ignored
- Property 9: Missing required fields rejected
- Property 10: Invalid numeric values rejected
- Property 12-15: Late data handling
"""

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

# Import metrics utilities for tracking message processing
from src.utils.metrics import (
    record_message_processed,
    track_processing_latency,
    PROCESSING_LATENCY,
    MESSAGES_PROCESSED,
)

logger = logging.getLogger(__name__)


# ============================================================================
# LATE DATA HANDLER
# ============================================================================

@dataclass
class LateDataMetrics:
    """Metrics for late data handling."""
    
    total_checked: int = 0
    accepted: int = 0
    rejected_watermark: int = 0
    rejected_stale: int = 0
    rejected_future: int = 0
    
    def to_dict(self) -> Dict[str, int]:
        """Convert metrics to dictionary."""
        return {
            "total_checked": self.total_checked,
            "accepted": self.accepted,
            "rejected_watermark": self.rejected_watermark,
            "rejected_stale": self.rejected_stale,
            "rejected_future": self.rejected_future,
            "acceptance_rate_pct": round(
                self.accepted / max(self.total_checked, 1) * 100, 2
            ),
        }


class LateDataHandler:
    """
    Handler for late data detection and rejection.
    
    Implements a hybrid approach combining:
    1. Watermark-based rejection for data that's too old
    2. Per-symbol tracking to prevent overwriting newer data with older data
    3. Future data rejection for clock skew protection
    
    This ensures:
    - Property 12: Data outside watermark is rejected
    - Property 13: Stale data (older than last processed) is rejected
    - Property 14: Future data (beyond max_future_ms) is rejected
    - Property 15: Event times are strictly increasing per symbol
    """
    
    # Rejection reasons
    REASON_ACCEPTED = "accepted"
    REASON_WATERMARK = "outside_watermark"
    REASON_STALE = "stale_data"
    REASON_FUTURE = "future_data"
    
    def __init__(
        self,
        watermark_delay_ms: int = 10000,
        max_future_ms: int = 5000,
        max_tracked_symbols: int = 100,
        configured_symbols: Optional[Set[str]] = None,
    ):
        """
        Initialize late data handler.
        
        Args:
            watermark_delay_ms: Maximum age of data to accept (default 10s)
            max_future_ms: Maximum future time to accept (default 5s)
            max_tracked_symbols: Maximum symbols to track in memory
            configured_symbols: Set of configured symbols for cleanup
        """
        self.watermark_delay_ms = watermark_delay_ms
        self.max_future_ms = max_future_ms
        self.max_tracked_symbols = max_tracked_symbols
        self.configured_symbols = configured_symbols or set()
        
        # Per-symbol event time tracking
        self._last_event_times: Dict[str, int] = {}
        
        # Metrics
        self.metrics = LateDataMetrics()
        
        logger.info(
            f"LateDataHandler initialized: watermark={watermark_delay_ms}ms, "
            f"max_future={max_future_ms}ms, max_symbols={max_tracked_symbols}"
        )
    
    def should_process(
        self,
        symbol: str,
        event_time: int,
        current_time: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """
        Check if a message should be processed based on event time.
        
        Args:
            symbol: Trading pair symbol
            event_time: Event timestamp in milliseconds
            current_time: Current time in milliseconds (for testing)
            
        Returns:
            Tuple of (should_process, reason)
            
        Properties:
            - Property 12: Watermark rejection
            - Property 13: Stale data rejection
            - Property 14: Future data rejection
            - Property 15: Event time monotonicity
        """
        self.metrics.total_checked += 1
        
        if current_time is None:
            current_time = int(time.time() * 1000)
        
        symbol = symbol.upper()
        
        # Check 1: Future data rejection (Property 14)
        # Reject data from the future (clock skew protection)
        if event_time > current_time + self.max_future_ms:
            self.metrics.rejected_future += 1
            logger.debug(
                f"Rejected {symbol}: future data "
                f"(event_time={event_time}, current={current_time}, "
                f"max_future={self.max_future_ms})"
            )
            return False, self.REASON_FUTURE
        
        # Check 2: Watermark rejection (Property 12)
        # Reject data that's too old
        watermark = current_time - self.watermark_delay_ms
        if event_time < watermark:
            self.metrics.rejected_watermark += 1
            logger.debug(
                f"Rejected {symbol}: outside watermark "
                f"(event_time={event_time}, watermark={watermark})"
            )
            return False, self.REASON_WATERMARK
        
        # Check 3: Per-symbol stale data rejection (Property 13, 15)
        # Reject data older than or equal to last processed for this symbol
        last_event_time = self._last_event_times.get(symbol, 0)
        if event_time <= last_event_time:
            self.metrics.rejected_stale += 1
            logger.debug(
                f"Rejected {symbol}: stale data "
                f"(event_time={event_time}, last={last_event_time})"
            )
            return False, self.REASON_STALE
        
        # Accept and update tracking (Property 15: monotonicity)
        self._last_event_times[symbol] = event_time
        self.metrics.accepted += 1
        
        # Cleanup if needed
        self._cleanup_tracking()
        
        return True, self.REASON_ACCEPTED
    
    def _cleanup_tracking(self) -> None:
        """
        Remove oldest entries if tracking cache is too large.
        
        Memory management for production - keeps only configured symbols
        when cache exceeds max_tracked_symbols.
        """
        if len(self._last_event_times) > self.max_tracked_symbols:
            if self.configured_symbols:
                # Keep only configured symbols
                self._last_event_times = {
                    k: v for k, v in self._last_event_times.items()
                    if k in self.configured_symbols
                }
            else:
                # No configured symbols - keep most recent half
                sorted_items = sorted(
                    self._last_event_times.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                keep_count = self.max_tracked_symbols // 2
                self._last_event_times = dict(sorted_items[:keep_count])
            
            logger.debug(
                f"Cleaned up tracking cache, now tracking "
                f"{len(self._last_event_times)} symbols"
            )
    
    def get_last_event_time(self, symbol: str) -> Optional[int]:
        """
        Get the last processed event time for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Last event time in milliseconds, or None if not tracked
        """
        return self._last_event_times.get(symbol.upper())
    
    def get_tracked_symbols(self) -> Set[str]:
        """Get set of currently tracked symbols."""
        return set(self._last_event_times.keys())
    
    def get_metrics(self) -> Dict[str, int]:
        """Get late data handling metrics."""
        return self.metrics.to_dict()
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics = LateDataMetrics()
    
    def reset_tracking(self) -> None:
        """Reset all event time tracking."""
        self._last_event_times.clear()
        logger.info("Event time tracking reset")
    
    def update_configured_symbols(self, symbols: Set[str]) -> None:
        """
        Update the set of configured symbols.
        
        Args:
            symbols: New set of configured symbols
        """
        self.configured_symbols = {s.upper() for s in symbols}
        logger.info(f"Updated configured symbols: {len(self.configured_symbols)}")
    
    def get_event_time_lag(self, symbol: str, current_time: Optional[int] = None) -> int:
        """
        Get the lag between current time and last event time for a symbol.
        
        Args:
            symbol: Trading pair symbol
            current_time: Current time in milliseconds (for testing)
            
        Returns:
            Lag in milliseconds, or -1 if symbol not tracked
        """
        if current_time is None:
            current_time = int(time.time() * 1000)
        
        last_event_time = self._last_event_times.get(symbol.upper())
        if last_event_time is None:
            return -1
        
        return current_time - last_event_time


# ============================================================================
# HEALTH CHECK FUNCTIONS
# ============================================================================

def check_redis_health() -> bool:
    """Check if Redis is reachable and has recent ticker data."""
    try:
        import redis
        
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        password = os.getenv("REDIS_PASSWORD")
        
        r = redis.Redis(
            host=host,
            port=port,
            password=password,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        )
        
        # Basic connectivity check
        if not r.ping():
            print("ERROR: Redis ping failed")
            return False
        
        # Check for recent ticker data (optional - service might be starting)
        ticker_keys = r.keys("ticker:*")
        if ticker_keys:
            # Check if at least one ticker has recent data
            current_time_ms = int(time.time() * 1000)
            for key in ticker_keys[:5]:  # Check first 5 keys
                data = r.hgetall(key)
                if data and b"updated_at" in data:
                    updated_at = int(data[b"updated_at"])
                    age_ms = current_time_ms - updated_at
                    # Data should be less than 60 seconds old (TTL)
                    if age_ms < 60000:
                        return True
            
            # If we have keys but all are stale, still consider healthy
            # (consumer might be catching up)
            print(f"INFO: Found {len(ticker_keys)} ticker keys, checking freshness...")
        
        # Redis is reachable, consider healthy even without data
        # (service might be starting up)
        return True
        
    except ImportError:
        print("ERROR: redis package not installed")
        return False
    except Exception as e:
        print(f"ERROR: Redis health check failed: {e}")
        return False


def check_kafka_connectivity() -> bool:
    """Check if Kafka is reachable (optional check)."""
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        
        # Quick connectivity check with short timeout
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        consumer.close()
        return True
        
    except NoBrokersAvailable:
        print("WARNING: Kafka brokers not available")
        return False
    except ImportError:
        # kafka-python not installed, skip this check
        return True
    except Exception as e:
        print(f"WARNING: Kafka connectivity check failed: {e}")
        # Don't fail health check just because Kafka check failed
        return True


def health_check_main() -> int:
    """
    Run health checks and return exit code.
    
    Returns:
        0 if healthy, 1 if unhealthy
    """
    # Primary check: Redis must be reachable
    if not check_redis_health():
        return 1
    
    # Secondary check: Kafka connectivity (warning only)
    check_kafka_connectivity()
    
    print("OK: Ticker consumer health check passed")
    return 0


# ============================================================================
# TICKER CONSUMER
# ============================================================================

@dataclass
class ProcessingMetrics:
    """Metrics for ticker processing."""
    
    messages_received: int = 0
    messages_processed: int = 0
    messages_skipped_symbol: int = 0
    messages_skipped_validation: int = 0
    messages_skipped_late: int = 0
    messages_skipped_stale: int = 0
    messages_skipped_future: int = 0
    redis_write_success: int = 0
    redis_write_failure: int = 0
    total_processing_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "messages_received": self.messages_received,
            "messages_processed": self.messages_processed,
            "messages_skipped_symbol": self.messages_skipped_symbol,
            "messages_skipped_validation": self.messages_skipped_validation,
            "messages_skipped_late": self.messages_skipped_late,
            "messages_skipped_stale": self.messages_skipped_stale,
            "messages_skipped_future": self.messages_skipped_future,
            "redis_write_success": self.redis_write_success,
            "redis_write_failure": self.redis_write_failure,
            "total_processing_time_ms": round(self.total_processing_time_ms, 2),
            "avg_processing_time_ms": round(
                self.total_processing_time_ms / max(self.messages_processed, 1), 2
            ),
        }


class TickerConsumer:
    """
    Core ticker consumer service.
    
    Reads ticker messages from Kafka, validates them, handles late data,
    and writes to Redis for low-latency access.
    
    Properties validated:
    - Property 8: Non-configured symbols ignored
    - Property 9: Missing required fields rejected
    - Property 10: Invalid numeric values rejected
    - Property 12-15: Late data handling
    """
    
    def __init__(self, config: "TickerConsumerConfig"):
        """
        Initialize ticker consumer.
        
        Args:
            config: Ticker consumer configuration
        """
        # Import here to avoid circular imports
        from src.ticker_consumer.core import (
            TickerConsumerConfig,
            TickerValidator,
            ErrorHandler,
            ErrorType,
        )
        from src.storage import RedisTickerStorage
        
        self.config = config
        self.validator = TickerValidator()
        self.redis_storage: Optional["RedisTickerStorage"] = None
        self.kafka_consumer: Optional[Any] = None
        self.metrics = ProcessingMetrics()
        
        # Late data handler (Properties 12-15)
        self.late_data_handler = LateDataHandler(
            watermark_delay_ms=config.late_data.watermark_delay_ms,
            max_future_ms=config.late_data.max_future_ms,
            max_tracked_symbols=config.late_data.max_tracked_symbols,
            configured_symbols=set(s.upper() for s in config.symbols),
        )
        
        # Error handler (Requirements 1.4, 2.4)
        self.error_handler = ErrorHandler(
            initial_backoff_ms=config.kafka.reconnect_backoff_ms,
            max_backoff_ms=config.kafka.reconnect_backoff_max_ms,
            max_retries=config.redis.max_retries,
        )
        
        # Configured symbols set for fast lookup
        self._configured_symbols: Set[str] = set(s.upper() for s in config.symbols)
        
        # Shutdown flag
        self._shutdown_requested = False
        
        # Store ErrorType for later use
        self._ErrorType = ErrorType
        self._RedisTickerStorage = RedisTickerStorage
        
        logger.info(f"TickerConsumer initialized with {len(self._configured_symbols)} symbols")
    
    def _init_redis(self) -> None:
        """Initialize Redis connection."""
        logger.info(f"Connecting to Redis at {self.config.redis.host}:{self.config.redis.port}")
        
        self.redis_storage = self._RedisTickerStorage(
            host=self.config.redis.host,
            port=self.config.redis.port,
            db=self.config.redis.db,
            ttl_seconds=self.config.redis.ticker_ttl_seconds,
            max_retries=self.config.redis.max_retries,
            retry_delay=self.config.redis.retry_delay,
        )
        
        logger.info("Redis connection established")
    
    def _create_kafka_consumer(self) -> Any:
        """Create a new Kafka consumer instance."""
        from kafka import KafkaConsumer as KafkaConsumerClient
        
        kafka_config = self.config.kafka
        
        return KafkaConsumerClient(
            kafka_config.topic,
            bootstrap_servers=kafka_config.bootstrap_servers,
            group_id=kafka_config.consumer_group,
            auto_offset_reset=kafka_config.auto_offset_reset,
            enable_auto_commit=kafka_config.enable_auto_commit,
            auto_commit_interval_ms=kafka_config.auto_commit_interval_ms,
            session_timeout_ms=kafka_config.session_timeout_ms,
            heartbeat_interval_ms=kafka_config.heartbeat_interval_ms,
            max_poll_records=kafka_config.max_poll_records,
            max_poll_interval_ms=kafka_config.max_poll_interval_ms,
            reconnect_backoff_ms=kafka_config.reconnect_backoff_ms,
            reconnect_backoff_max_ms=kafka_config.reconnect_backoff_max_ms,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
    
    def _init_kafka(self) -> None:
        """Initialize Kafka consumer with exponential backoff retry (Requirement 2.4)."""
        from kafka.errors import NoBrokersAvailable
        
        kafka_config = self.config.kafka
        
        logger.info(f"Connecting to Kafka at {kafka_config.bootstrap_servers}")
        logger.info(f"Topic: {kafka_config.topic}, Consumer group: {kafka_config.consumer_group}")
        
        # Use error handler for retry with exponential backoff
        self.kafka_consumer = self.error_handler.with_retry(
            operation=self._create_kafka_consumer,
            error_type=self._ErrorType.KAFKA_CONNECTION,
            operation_name="Kafka connection",
        )
        
        if self.kafka_consumer is None:
            raise NoBrokersAvailable("Failed to connect to Kafka after retries")
        
        logger.info("Kafka consumer connected")
    
    def should_process_symbol(self, symbol: str) -> bool:
        """
        Check if symbol is in configured list.
        
        Requirement 5.3: Ignore messages for non-configured symbols
        Property 8: Non-configured symbols ignored
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if symbol should be processed
        """
        return symbol.upper() in self._configured_symbols
    
    def should_process_event_time(
        self, symbol: str, event_time: int
    ) -> Tuple[bool, str]:
        """
        Check if message should be processed based on event time.
        
        Delegates to LateDataHandler for hybrid watermark + per-symbol tracking:
        - Property 12: Watermark rejection (too old)
        - Property 13: Stale data rejection (older than last processed)
        - Property 14: Future data rejection (clock skew protection)
        - Property 15: Event time monotonicity per symbol
        
        Args:
            symbol: Trading pair symbol
            event_time: Event timestamp in milliseconds
            
        Returns:
            Tuple of (should_process, reason)
        """
        return self.late_data_handler.should_process(symbol, event_time)
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single ticker message.
        
        Args:
            message: Raw ticker message from Kafka
            
        Returns:
            True if message was successfully processed and written to Redis
        """
        start_time = time.time()
        self.metrics.messages_received += 1
        topic = self.config.kafka.topic
        
        try:
            # Extract ticker data from EnrichedMessage format
            # The message structure is: {original_data: {...}, symbol: ..., ...}
            original_data = message.get("original_data", message)
            symbol = message.get("symbol") or original_data.get("s", "")
            symbol = symbol.upper()
            
            # Check 1: Symbol filter (Requirement 5.3, Property 8)
            if not self.should_process_symbol(symbol):
                self.metrics.messages_skipped_symbol += 1
                # Track skipped message in Prometheus metrics
                record_message_processed("ticker_consumer", topic, "skipped_symbol")
                logger.debug(f"Skipping non-configured symbol: {symbol}")
                return False
            
            # Check 2: Validate message (Requirements 7.1-7.3, Properties 9-10)
            validation_result = self.validator.validate(original_data)
            if not validation_result.is_valid:
                self.metrics.messages_skipped_validation += 1
                # Track validation failure in Prometheus metrics
                record_message_processed("ticker_consumer", topic, "skipped_validation")
                logger.warning(
                    f"Validation failed for {symbol}: {validation_result.message}"
                )
                return False
            
            # Check 3: Late data handling (Properties 12-15)
            event_time = original_data.get("E", 0)
            if event_time:
                should_process, reason = self.should_process_event_time(symbol, event_time)
                if not should_process:
                    if reason == "outside_watermark":
                        self.metrics.messages_skipped_late += 1
                    elif reason == "stale_data":
                        self.metrics.messages_skipped_stale += 1
                    elif reason == "future_data":
                        self.metrics.messages_skipped_future += 1
                    
                    # Track late data skip in Prometheus metrics
                    record_message_processed("ticker_consumer", topic, f"skipped_{reason}")
                    logger.debug(f"Skipping {symbol} due to {reason}")
                    return False
            
            # Write to Redis (Requirement 1.1)
            success = self.redis_storage.write_ticker(symbol, original_data)
            
            if success:
                self.metrics.redis_write_success += 1
                self.metrics.messages_processed += 1
                # Track successful message processing in Prometheus metrics
                record_message_processed("ticker_consumer", topic, "success")
            else:
                self.metrics.redis_write_failure += 1
                # Track Redis write failure in Prometheus metrics
                record_message_processed("ticker_consumer", topic, "error")
                logger.error(f"Failed to write ticker for {symbol} to Redis")
                return False
            
            # Track processing time (both internal metrics and Prometheus)
            processing_time_ms = (time.time() - start_time) * 1000
            self.metrics.total_processing_time_ms += processing_time_ms
            
            # Record processing latency in Prometheus histogram
            PROCESSING_LATENCY.labels(
                service="ticker_consumer",
                operation="process_message"
            ).observe(processing_time_ms)
            
            if processing_time_ms > self.config.processing_timeout_ms:
                logger.warning(
                    f"Processing time {processing_time_ms:.2f}ms exceeded target "
                    f"{self.config.processing_timeout_ms}ms for {symbol}"
                )
            
            return True
            
        except Exception as e:
            # Track error in Prometheus metrics
            record_message_processed("ticker_consumer", topic, "error")
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False
    
    def process_batch(self, messages: List[Dict[str, Any]]) -> int:
        """
        Process a batch of ticker messages.
        
        Args:
            messages: List of ticker messages
            
        Returns:
            Number of successfully processed messages
        """
        success_count = 0
        
        for message in messages:
            if self._shutdown_requested:
                break
            
            if self.process_message(message):
                success_count += 1
        
        return success_count
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current processing metrics."""
        metrics = self.metrics.to_dict()
        metrics["validator_metrics"] = self.validator.get_metrics()
        metrics["late_data_metrics"] = self.late_data_handler.get_metrics()
        metrics["error_metrics"] = self.error_handler.get_metrics()
        return metrics
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics = ProcessingMetrics()
        self.validator.reset_metrics()
        self.late_data_handler.reset_metrics()
        self.error_handler.reset_metrics()
    
    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown_requested = True
    
    def is_healthy(self) -> bool:
        """
        Check if consumer is healthy.
        
        Returns:
            True if Kafka and Redis connections are alive
        """
        try:
            # Check Redis
            if not self.redis_storage or not self.redis_storage.ping():
                logger.warning("Redis health check failed")
                return False
            
            # Check Kafka (basic check - consumer exists)
            if not self.kafka_consumer:
                logger.warning("Kafka consumer not initialized")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def start(self) -> None:
        """
        Start the ticker consumer.
        
        Initializes connections and logs configuration.
        """
        logger.info("Starting Ticker Consumer...")
        
        # Log configuration (Requirement 5.4)
        self.config.log_config()
        
        # Initialize connections
        self._init_redis()
        self._init_kafka()
        
        logger.info("Ticker Consumer started successfully")
    
    def stop(self) -> None:
        """
        Stop the ticker consumer gracefully.
        
        Closes all connections and logs final metrics.
        """
        logger.info("Stopping Ticker Consumer...")
        
        self._shutdown_requested = True
        
        # Log final metrics
        logger.info(f"Final metrics: {self.get_metrics()}")
        
        # Close Kafka consumer
        if self.kafka_consumer:
            try:
                self.kafka_consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        
        logger.info("Ticker Consumer stopped")
    
    def _reconnect_kafka(self) -> bool:
        """
        Attempt to reconnect to Kafka with exponential backoff.
        
        Requirement 2.4: Exponential backoff reconnection
        
        Returns:
            True if reconnection successful
        """
        logger.info("Attempting Kafka reconnection...")
        
        # Close existing consumer
        if self.kafka_consumer:
            try:
                self.kafka_consumer.close()
            except Exception as e:
                logger.warning(f"Error closing existing consumer: {e}")
        
        # Reconnect with retry
        self.kafka_consumer = self.error_handler.with_retry(
            operation=self._create_kafka_consumer,
            error_type=self._ErrorType.KAFKA_CONNECTION,
            operation_name="Kafka reconnection",
        )
        
        return self.kafka_consumer is not None
    
    def run(self) -> None:
        """
        Run the ticker consumer main loop.
        
        Continuously polls Kafka for messages and processes them.
        Handles errors gracefully per requirements:
        - Requirement 1.4: Redis errors - log and continue
        - Requirement 2.4: Kafka errors - reconnect with exponential backoff
        """
        from kafka.errors import KafkaError
        
        try:
            self.start()
            
            logger.info("Entering main processing loop...")
            
            while not self._shutdown_requested:
                try:
                    # Poll for messages
                    records = self.kafka_consumer.poll(
                        timeout_ms=1000,
                        max_records=self.config.batch_size
                    )
                    
                    if not records:
                        continue
                    
                    # Process all messages from all partitions
                    for topic_partition, messages in records.items():
                        batch = [msg.value for msg in messages]
                        processed = self.process_batch(batch)
                        
                        logger.debug(
                            f"Processed {processed}/{len(batch)} messages "
                            f"from {topic_partition}"
                        )
                    
                except KafkaError as e:
                    # Requirement 2.4: Kafka error - attempt reconnection
                    self.error_handler.handle_error(
                        e, self._ErrorType.KAFKA_CONSUMER, "poll"
                    )
                    
                    if not self._reconnect_kafka():
                        logger.error("Failed to reconnect to Kafka, exiting...")
                        break
                    
                except Exception as e:
                    # Log error and continue (Requirement 1.4 spirit)
                    self.error_handler.handle_error(
                        e, self._ErrorType.PROCESSING, "main_loop"
                    )
                    time.sleep(1)
            
        finally:
            self.stop()


# Allow running health check directly
if __name__ == "__main__":
    sys.exit(health_check_main())
