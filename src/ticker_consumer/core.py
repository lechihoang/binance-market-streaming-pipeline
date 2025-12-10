"""
Core module for Ticker Consumer Service.

Contains configuration, validation, and error handling components.
Consolidates config.py, validator.py, and error_handler.py.

Table of Contents:
- Configuration Classes (line ~40)
  - KafkaConsumerConfig
  - RedisConfig
  - LateDataConfig
  - TickerConsumerConfig
- Validator (line ~220)
  - ValidationResult
  - TickerValidator
- Error Handler (line ~400)
  - ErrorType
  - ErrorMetrics
  - ErrorHandler (uses utils.retry)
  - KafkaReconnectionHandler

Requirements:
- 1.4: Redis unavailable - log error and continue without crashing
- 2.2: Dedicated consumer group for ticker consumer
- 2.3: Start consuming from latest offset
- 2.4: Kafka connection loss - attempt reconnection with exponential backoff
- 5.1: Support configuration of ticker symbols via environment variable
- 5.2: Default configuration includes 15 symbols
- 5.4: Log configured symbols on startup
- 7.1: Validate required fields are present
- 7.2: Log warning and skip messages with missing/null fields
- 7.3: Log warning and skip messages with invalid numeric values
- 7.4: Track validation failure counts
- 7.5: Determine completeness for API response

Refactored to use shared utilities from src.utils:
- ExponentialBackoff: imported from src.utils.retry
- retry_operation: used in ErrorHandler.with_retry
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

# Import shared utilities
from src.utils.retry import ExponentialBackoff, RetryConfig, retry_operation
from src.utils.config import (
    get_env_str,
    get_env_int,
    get_env_float,
    get_env_bool,
    get_env_list,
    get_env_optional,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ============================================================================
# CONFIGURATION
# ============================================================================

# Default 15 symbols with highest market cap (Requirement 5.2)
DEFAULT_TICKER_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "LTCUSDT", "UNIUSDT", "ATOMUSDT", "SHIBUSDT",
]


@dataclass
class KafkaConsumerConfig:
    """Kafka consumer configuration for ticker consumer."""
    
    bootstrap_servers: str = "localhost:9092"
    topic: str = "raw_tickers"
    consumer_group: str = "ticker-consumer-group"  # Requirement 2.2
    
    # Consumer settings
    auto_offset_reset: str = "latest"  # Requirement 2.3
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    
    # Reconnection settings (Requirement 2.4)
    reconnect_backoff_ms: int = 1000  # Initial backoff
    reconnect_backoff_max_ms: int = 60000  # Max backoff (60s)
    
    @classmethod
    def from_env(cls) -> "KafkaConsumerConfig":
        """Create configuration from environment variables.
        
        Uses utils config helpers for type-safe env loading.
        """
        return cls(
            bootstrap_servers=get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=get_env_str("KAFKA_TICKER_TOPIC", "raw_tickers"),
            consumer_group=get_env_str("TICKER_CONSUMER_GROUP", "ticker-consumer-group"),
            auto_offset_reset=get_env_str("KAFKA_AUTO_OFFSET_RESET", "latest"),
            enable_auto_commit=get_env_bool("KAFKA_ENABLE_AUTO_COMMIT", True),
            auto_commit_interval_ms=get_env_int("KAFKA_AUTO_COMMIT_INTERVAL_MS", 5000),
            session_timeout_ms=get_env_int("KAFKA_SESSION_TIMEOUT_MS", 30000),
            heartbeat_interval_ms=get_env_int("KAFKA_HEARTBEAT_INTERVAL_MS", 10000),
            max_poll_records=get_env_int("KAFKA_MAX_POLL_RECORDS", 500),
            max_poll_interval_ms=get_env_int("KAFKA_MAX_POLL_INTERVAL_MS", 300000),
            reconnect_backoff_ms=get_env_int("KAFKA_RECONNECT_BACKOFF_MS", 1000),
            reconnect_backoff_max_ms=get_env_int("KAFKA_RECONNECT_BACKOFF_MAX_MS", 60000),
        )


@dataclass
class RedisConfig:
    """Redis configuration for ticker storage."""
    
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    
    # TTL settings (Requirement 3.3)
    ticker_ttl_seconds: int = 60
    
    # Connection settings
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    max_retries: int = 3
    retry_delay: float = 1.0
    
    @classmethod
    def from_env(cls) -> "RedisConfig":
        """Create configuration from environment variables.
        
        Uses utils config helpers for type-safe env loading.
        """
        return cls(
            host=get_env_str("REDIS_HOST", "localhost"),
            port=get_env_int("REDIS_PORT", 6379),
            db=get_env_int("REDIS_DB", 0),
            password=get_env_optional("REDIS_PASSWORD"),
            ticker_ttl_seconds=get_env_int("TICKER_TTL_SECONDS", 60),
            socket_timeout=get_env_float("REDIS_SOCKET_TIMEOUT", 5.0),
            socket_connect_timeout=get_env_float("REDIS_SOCKET_CONNECT_TIMEOUT", 5.0),
            max_retries=get_env_int("REDIS_MAX_RETRIES", 3),
            retry_delay=get_env_float("REDIS_RETRY_DELAY", 1.0),
        )


@dataclass
class LateDataConfig:
    """Configuration for late data handling.
    
    Based on design document Late Data Handling section:
    - Watermark delay: 10 seconds (production safe)
    - Max future: 5 seconds (clock skew protection)
    - Max tracked symbols: 100 (memory limit)
    """
    
    watermark_delay_ms: int = 10000  # 10 seconds
    max_future_ms: int = 5000  # 5 seconds
    max_tracked_symbols: int = 100
    
    @classmethod
    def from_env(cls) -> "LateDataConfig":
        """Create configuration from environment variables.
        
        Uses utils config helpers for type-safe env loading.
        """
        return cls(
            watermark_delay_ms=get_env_int("TICKER_WATERMARK_DELAY_MS", 10000),
            max_future_ms=get_env_int("TICKER_MAX_FUTURE_MS", 5000),
            max_tracked_symbols=get_env_int("TICKER_MAX_TRACKED_SYMBOLS", 100),
        )


@dataclass
class TickerConsumerConfig:
    """Main configuration for ticker consumer service.
    
    Requirements:
    - 5.1: Support configuration of ticker symbols via environment variable
    - 5.2: Default configuration includes 15 symbols
    """
    
    kafka: KafkaConsumerConfig
    redis: RedisConfig
    late_data: LateDataConfig
    
    # Ticker symbols to track (Requirement 5.1, 5.2)
    symbols: List[str] = field(default_factory=lambda: DEFAULT_TICKER_SYMBOLS.copy())
    
    # Logging
    log_level: str = "INFO"
    
    # Processing settings
    batch_size: int = 100  # Process messages in batches
    processing_timeout_ms: int = 100  # Target latency (Requirement 1.1)
    
    # Health check settings
    health_check_interval_seconds: int = 30
    max_lag_threshold: int = 1000  # Max acceptable consumer lag
    
    @classmethod
    def from_env(cls) -> "TickerConsumerConfig":
        """Create complete configuration from environment variables.
        
        Requirement 5.1: Support configuration via TICKER_SYMBOLS env var
        Uses utils config helpers for type-safe env loading.
        """
        # Parse symbols from environment using get_env_list (Requirement 5.1)
        symbols_raw = get_env_list("TICKER_SYMBOLS", DEFAULT_TICKER_SYMBOLS.copy())
        symbols = [s.upper() for s in symbols_raw]
        
        return cls(
            kafka=KafkaConsumerConfig.from_env(),
            redis=RedisConfig.from_env(),
            late_data=LateDataConfig.from_env(),
            symbols=symbols,
            log_level=get_env_str("LOG_LEVEL", "INFO"),
            batch_size=get_env_int("TICKER_BATCH_SIZE", 100),
            processing_timeout_ms=get_env_int("TICKER_PROCESSING_TIMEOUT_MS", 100),
            health_check_interval_seconds=get_env_int("TICKER_HEALTH_CHECK_INTERVAL", 30),
            max_lag_threshold=get_env_int("TICKER_MAX_LAG_THRESHOLD", 1000),
        )
    
    def log_config(self) -> None:
        """Log configuration on startup (Requirement 5.4)."""
        logger.info("=" * 60)
        logger.info("Ticker Consumer Configuration")
        logger.info("=" * 60)
        logger.info(f"Kafka bootstrap servers: {self.kafka.bootstrap_servers}")
        logger.info(f"Kafka topic: {self.kafka.topic}")
        logger.info(f"Consumer group: {self.kafka.consumer_group}")
        logger.info(f"Auto offset reset: {self.kafka.auto_offset_reset}")
        logger.info(f"Redis host: {self.redis.host}:{self.redis.port}")
        logger.info(f"Ticker TTL: {self.redis.ticker_ttl_seconds}s")
        logger.info(f"Watermark delay: {self.late_data.watermark_delay_ms}ms")
        logger.info(f"Max future time: {self.late_data.max_future_ms}ms")
        logger.info(f"Configured symbols ({len(self.symbols)}): {', '.join(self.symbols)}")
        logger.info("=" * 60)
    
    def is_symbol_configured(self, symbol: str) -> bool:
        """Check if a symbol is in the configured list.
        
        Requirement 5.3: Non-configured symbols should be ignored
        """
        return symbol.upper() in self.symbols


# ============================================================================
# VALIDATOR
# ============================================================================

@dataclass
class ValidationResult:
    """Result of validating a ticker message."""
    
    is_valid: bool
    message: str
    missing_fields: List[str] = field(default_factory=list)
    invalid_fields: List[str] = field(default_factory=list)
    
    def __str__(self) -> str:
        if self.is_valid:
            return "Validation passed"
        return f"Validation failed: {self.message}"


class TickerValidator:
    """
    Validator for Binance ticker messages.
    
    Validates that ticker messages contain all required fields and that
    numeric fields contain valid numeric values.
    
    Binance ticker message field mapping:
    - s: symbol
    - c: last_price (current price)
    - p: price_change (24h change)
    - P: price_change_pct (24h change %)
    - o: open (24h open price)
    - h: high (24h high)
    - l: low (24h low)
    - v: volume (24h base volume)
    - q: quote_volume (24h quote volume)
    - n: trades_count (optional)
    - E: event_time (optional)
    
    Requirements: 7.1, 7.2, 7.3
    """
    
    # Required fields from Binance ticker message
    # These must be present and non-null
    REQUIRED_FIELDS: Set[str] = {
        's',   # symbol
        'c',   # last_price
        'p',   # price_change
        'P',   # price_change_pct
        'o',   # open
        'h',   # high
        'l',   # low
        'v',   # volume
        'q',   # quote_volume
    }
    
    # Numeric fields that must contain valid numeric strings
    NUMERIC_FIELDS: Set[str] = {
        'c',   # last_price
        'p',   # price_change
        'P',   # price_change_pct
        'o',   # open
        'h',   # high
        'l',   # low
        'v',   # volume
        'q',   # quote_volume
    }
    
    # Optional fields that contribute to completeness
    OPTIONAL_FIELDS: Set[str] = {
        'n',   # trades_count
        'E',   # event_time
        'e',   # event_type
        'w',   # weighted_avg_price
        'Q',   # last_quantity
    }
    
    def __init__(self):
        """Initialize the validator with counters for metrics."""
        self._validation_success_count = 0
        self._validation_failure_count = 0
        self._missing_field_counts: Dict[str, int] = {}
        self._invalid_field_counts: Dict[str, int] = {}
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate a ticker message against the schema.
        
        Args:
            data: Ticker message dictionary from Binance
            
        Returns:
            ValidationResult with validation status and details
        """
        missing_fields: List[str] = []
        invalid_fields: List[str] = []
        
        # Check for missing or null required fields (Requirement 7.1, 7.2)
        for field_name in self.REQUIRED_FIELDS:
            if field_name not in data:
                missing_fields.append(field_name)
            elif data[field_name] is None:
                missing_fields.append(field_name)
        
        # Check numeric fields for valid values (Requirement 7.3)
        for field_name in self.NUMERIC_FIELDS:
            if field_name in data and data[field_name] is not None:
                if not self._is_valid_numeric(data[field_name]):
                    invalid_fields.append(field_name)
        
        # Build result
        is_valid = len(missing_fields) == 0 and len(invalid_fields) == 0
        
        # Update metrics (Requirement 7.4)
        if is_valid:
            self._validation_success_count += 1
        else:
            self._validation_failure_count += 1
            for f in missing_fields:
                self._missing_field_counts[f] = self._missing_field_counts.get(f, 0) + 1
            for f in invalid_fields:
                self._invalid_field_counts[f] = self._invalid_field_counts.get(f, 0) + 1
        
        # Build message
        message = self._build_message(missing_fields, invalid_fields)
        
        # Log warnings for invalid messages (Requirement 7.2, 7.3)
        if not is_valid:
            symbol = data.get('s', 'unknown')
            logger.warning(f"Ticker validation failed for {symbol}: {message}")
        
        return ValidationResult(
            is_valid=is_valid,
            message=message,
            missing_fields=missing_fields,
            invalid_fields=invalid_fields,
        )
    
    def is_complete(self, data: Dict[str, Any]) -> bool:
        """
        Check if all optional fields are present and valid.
        
        Used to set the 'complete' field in API responses.
        
        Args:
            data: Ticker message dictionary
            
        Returns:
            True if all optional fields are present and valid
            
        Requirement: 7.5
        """
        # First check if required fields are valid
        result = self.validate(data)
        if not result.is_valid:
            return False
        
        # Check optional fields
        for field_name in self.OPTIONAL_FIELDS:
            if field_name not in data or data[field_name] is None:
                return False
        
        return True
    
    def _is_valid_numeric(self, value: Any) -> bool:
        """
        Check if a value is a valid numeric string.
        
        Args:
            value: Value to check
            
        Returns:
            True if value can be parsed as a number
        """
        if value is None:
            return False
        
        # Handle numeric types directly
        if isinstance(value, (int, float)):
            return True
        
        # Handle string values
        if isinstance(value, str):
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        
        return False
    
    def _build_message(
        self, 
        missing_fields: List[str], 
        invalid_fields: List[str]
    ) -> str:
        """Build a descriptive message for the validation result."""
        if not missing_fields and not invalid_fields:
            return "Validation passed"
        
        parts = []
        if missing_fields:
            parts.append(f"Missing required fields: {', '.join(sorted(missing_fields))}")
        if invalid_fields:
            parts.append(f"Invalid numeric values in fields: {', '.join(sorted(invalid_fields))}")
        
        return "; ".join(parts)
    
    @property
    def success_count(self) -> int:
        """Get the count of successful validations."""
        return self._validation_success_count
    
    @property
    def failure_count(self) -> int:
        """Get the count of failed validations."""
        return self._validation_failure_count
    
    @property
    def missing_field_counts(self) -> Dict[str, int]:
        """Get counts of missing fields by field name."""
        return self._missing_field_counts.copy()
    
    @property
    def invalid_field_counts(self) -> Dict[str, int]:
        """Get counts of invalid fields by field name."""
        return self._invalid_field_counts.copy()
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get validation metrics for monitoring.
        
        Returns:
            Dictionary with validation statistics
            
        Requirement: 7.4
        """
        total = self._validation_success_count + self._validation_failure_count
        success_rate = 0.0
        if total > 0:
            success_rate = round(self._validation_success_count / total * 100, 2)
        
        return {
            "total_validations": total,
            "success_count": self._validation_success_count,
            "failure_count": self._validation_failure_count,
            "success_rate_pct": success_rate,
            "missing_field_counts": self._missing_field_counts.copy(),
            "invalid_field_counts": self._invalid_field_counts.copy(),
        }
    
    def reset_metrics(self) -> None:
        """Reset all validation metrics."""
        self._validation_success_count = 0
        self._validation_failure_count = 0
        self._missing_field_counts.clear()
        self._invalid_field_counts.clear()


# ============================================================================
# ERROR HANDLER
# ============================================================================

class ErrorType(Enum):
    """Types of errors that can occur."""
    
    KAFKA_CONNECTION = "kafka_connection"
    KAFKA_CONSUMER = "kafka_consumer"
    REDIS_CONNECTION = "redis_connection"
    REDIS_WRITE = "redis_write"
    VALIDATION = "validation"
    PROCESSING = "processing"
    UNKNOWN = "unknown"


@dataclass
class ErrorMetrics:
    """Metrics for error tracking."""
    
    kafka_connection_errors: int = 0
    kafka_consumer_errors: int = 0
    redis_connection_errors: int = 0
    redis_write_errors: int = 0
    validation_errors: int = 0
    processing_errors: int = 0
    unknown_errors: int = 0
    reconnection_attempts: int = 0
    successful_reconnections: int = 0
    
    def to_dict(self) -> dict:
        """Convert metrics to dictionary."""
        return {
            "kafka_connection_errors": self.kafka_connection_errors,
            "kafka_consumer_errors": self.kafka_consumer_errors,
            "redis_connection_errors": self.redis_connection_errors,
            "redis_write_errors": self.redis_write_errors,
            "validation_errors": self.validation_errors,
            "processing_errors": self.processing_errors,
            "unknown_errors": self.unknown_errors,
            "reconnection_attempts": self.reconnection_attempts,
            "successful_reconnections": self.successful_reconnections,
        }
    
    def increment(self, error_type: ErrorType) -> None:
        """Increment error count for given type."""
        if error_type == ErrorType.KAFKA_CONNECTION:
            self.kafka_connection_errors += 1
        elif error_type == ErrorType.KAFKA_CONSUMER:
            self.kafka_consumer_errors += 1
        elif error_type == ErrorType.REDIS_CONNECTION:
            self.redis_connection_errors += 1
        elif error_type == ErrorType.REDIS_WRITE:
            self.redis_write_errors += 1
        elif error_type == ErrorType.VALIDATION:
            self.validation_errors += 1
        elif error_type == ErrorType.PROCESSING:
            self.processing_errors += 1
        else:
            self.unknown_errors += 1


# ExponentialBackoff is now imported from src.utils.retry
# Keeping this comment for backward compatibility reference


class ErrorHandler:
    """
    Central error handler for ticker consumer.
    
    Provides:
    - Error classification and logging
    - Metrics tracking
    - Reconnection with exponential backoff
    - Graceful error recovery
    
    Requirements:
    - 1.4: Redis unavailable - log error and continue
    - 2.4: Kafka connection loss - exponential backoff reconnection
    """
    
    def __init__(
        self,
        initial_backoff_ms: int = 1000,
        max_backoff_ms: int = 60000,
        max_retries: int = 10,
    ):
        """
        Initialize error handler.
        
        Args:
            initial_backoff_ms: Initial backoff delay
            max_backoff_ms: Maximum backoff delay
            max_retries: Maximum retry attempts before giving up
        """
        self.backoff = ExponentialBackoff(
            initial_delay_ms=initial_backoff_ms,
            max_delay_ms=max_backoff_ms,
        )
        self.max_retries = max_retries
        self.metrics = ErrorMetrics()
        
        logger.info(
            f"ErrorHandler initialized: initial_backoff={initial_backoff_ms}ms, "
            f"max_backoff={max_backoff_ms}ms, max_retries={max_retries}"
        )
    
    def handle_error(
        self,
        error: Exception,
        error_type: ErrorType,
        context: Optional[str] = None,
    ) -> None:
        """
        Handle an error by logging and tracking metrics.
        
        Args:
            error: The exception that occurred
            error_type: Type of error
            context: Additional context for logging
        """
        self.metrics.increment(error_type)
        
        context_str = f" [{context}]" if context else ""
        
        if error_type in (ErrorType.KAFKA_CONNECTION, ErrorType.REDIS_CONNECTION):
            logger.error(
                f"{error_type.value} error{context_str}: {error}",
                exc_info=True
            )
        elif error_type == ErrorType.REDIS_WRITE:
            # Requirement 1.4: Log error and continue
            logger.error(f"Redis write error{context_str}: {error}")
        elif error_type == ErrorType.VALIDATION:
            logger.warning(f"Validation error{context_str}: {error}")
        else:
            logger.error(f"Error{context_str}: {error}", exc_info=True)
    
    def with_retry(
        self,
        operation: Callable[[], T],
        error_type: ErrorType,
        operation_name: str = "operation",
        on_retry: Optional[Callable[[int, int], None]] = None,
    ) -> Optional[T]:
        """
        Execute operation with retry and exponential backoff.
        
        Uses retry_operation from src.utils.retry for the core retry logic.
        
        Args:
            operation: Callable to execute
            error_type: Type of error for metrics
            operation_name: Name for logging
            on_retry: Optional callback(attempt, delay_ms) before each retry
            
        Returns:
            Result of operation, or None if all retries failed
        """
        # Track metrics and call user callback on retry
        def _on_retry_callback(attempt: int, delay_ms: int, exc: Exception) -> None:
            self.metrics.increment(error_type)
            self.metrics.reconnection_attempts += 1
            if on_retry:
                on_retry(attempt, delay_ms)
        
        # Create retry config from our settings
        config = RetryConfig(
            max_retries=self.max_retries,
            initial_delay_ms=self.backoff.initial_delay_ms,
            max_delay_ms=self.backoff.max_delay_ms,
            multiplier=self.backoff.multiplier,
            jitter_factor=self.backoff.jitter_factor,
        )
        
        try:
            result = retry_operation(
                operation=operation,
                config=config,
                operation_name=operation_name,
                on_retry=_on_retry_callback,
            )
            # Track successful reconnection if there were retries
            if self.metrics.reconnection_attempts > 0:
                self.metrics.successful_reconnections += 1
            return result
        except Exception:
            # All retries exhausted, return None to maintain backward compatibility
            return None
    
    def should_continue_on_error(self, error_type: ErrorType) -> bool:
        """
        Determine if processing should continue after an error.
        
        Requirement 1.4: Redis unavailable - continue without crashing
        
        Args:
            error_type: Type of error
            
        Returns:
            True if processing should continue
        """
        # Always continue for these error types
        continue_errors = {
            ErrorType.REDIS_WRITE,
            ErrorType.VALIDATION,
            ErrorType.PROCESSING,
        }
        
        return error_type in continue_errors
    
    def get_metrics(self) -> dict:
        """Get error metrics."""
        return self.metrics.to_dict()
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics = ErrorMetrics()


class KafkaReconnectionHandler:
    """
    Specialized handler for Kafka reconnection.
    
    Requirement 2.4: Attempt reconnection with exponential backoff
    """
    
    def __init__(
        self,
        error_handler: ErrorHandler,
        create_consumer: Callable[[], Any],
    ):
        """
        Initialize Kafka reconnection handler.
        
        Args:
            error_handler: Error handler instance
            create_consumer: Factory function to create Kafka consumer
        """
        self.error_handler = error_handler
        self.create_consumer = create_consumer
        self._consumer: Optional[Any] = None
        self._connected = False
    
    def connect(self) -> bool:
        """
        Connect to Kafka with retry.
        
        Returns:
            True if connection successful
        """
        def _connect():
            self._consumer = self.create_consumer()
            self._connected = True
            return self._consumer
        
        result = self.error_handler.with_retry(
            operation=_connect,
            error_type=ErrorType.KAFKA_CONNECTION,
            operation_name="Kafka connection",
        )
        
        return result is not None
    
    def reconnect(self) -> bool:
        """
        Reconnect to Kafka after connection loss.
        
        Returns:
            True if reconnection successful
        """
        logger.info("Attempting Kafka reconnection...")
        
        # Close existing consumer if any
        if self._consumer:
            try:
                self._consumer.close()
            except Exception as e:
                logger.warning(f"Error closing existing consumer: {e}")
        
        self._connected = False
        return self.connect()
    
    @property
    def consumer(self) -> Optional[Any]:
        """Get current consumer instance."""
        return self._consumer
    
    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
