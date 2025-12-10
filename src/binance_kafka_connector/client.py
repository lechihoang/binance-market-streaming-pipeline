"""
Client module for Binance-Kafka Connector.

Contains WebSocket client and message processor consolidated from:
- websocket_client.py: Async WebSocket client with reconnection logic
- processor.py: Message parsing, validation, and enrichment

Table of Contents:
- Message Processor (line ~40)
- WebSocket Client (line ~180)
"""

import asyncio
import json
import logging
import time
from typing import AsyncIterator, Dict, Any, List, Optional, Union, TYPE_CHECKING

import websockets
from pydantic import ValidationError

if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection

from .core import (
    config,
    TradeMessage,
    KlineMessage,
    TickerMessage,
    EnrichedMessage,
)

# Import retry utilities from utils module (Requirement 9.1)
from src.utils.retry import ExponentialBackoff

# Import metrics utilities from utils module (Requirement 5.3)
from src.utils.metrics import record_error, record_message_processed

logger = logging.getLogger(__name__)


# ============================================================================
# MESSAGE PROCESSOR
# ============================================================================

class MessageProcessor:
    """Processes Binance WebSocket messages through parsing, validation, and enrichment."""

    def parse_message(self, raw_json: str) -> Optional[Dict[str, Any]]:
        """Parse a JSON string into a dictionary.
        
        Args:
            raw_json: Raw JSON string from WebSocket
            
        Returns:
            Parsed dictionary if successful, None if parsing fails
        """
        try:
            data = json.loads(raw_json)
            return data
        except json.JSONDecodeError as e:
            logger.error(
                "Failed to parse JSON message",
                extra={
                    "error_type": "JSONDecodeError",
                    "error_details": str(e),
                    "raw_message": raw_json[:500]
                }
            )
            return None
        except Exception as e:
            logger.error(
                "Unexpected error parsing message",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "raw_message": raw_json[:500]
                },
                exc_info=True
            )
            return None

    def validate_message(self, data: Dict[str, Any]) -> tuple[bool, Optional[Union[TradeMessage, KlineMessage, TickerMessage]], Optional[str], Optional[str]]:
        """Validate a parsed message against Pydantic schemas.
        
        Detects the stream type from message structure and validates accordingly.
        
        Args:
            data: Parsed message dictionary (may be wrapped in {"stream": "...", "data": {...}} format)
            
        Returns:
            Tuple of (is_valid, validated_message, stream_type, error_message)
        """
        try:
            # Extract actual data if message is wrapped in Binance stream format
            if 'stream' in data and 'data' in data:
                actual_data = data['data']
            else:
                actual_data = data
            
            event_type = actual_data.get('e')
            
            if event_type == 'trade':
                message = TradeMessage(**actual_data)
                return True, message, 'trade', None
            elif event_type == 'kline':
                message = KlineMessage(**actual_data)
                return True, message, 'kline', None
            elif event_type == '24hrTicker':
                message = TickerMessage(**actual_data)
                return True, message, 'ticker', None
            else:
                error_msg = f"Unknown event type: {event_type}"
                logger.error(
                    "Validation failed: unknown event type",
                    extra={
                        "error_type": "UnknownEventType",
                        "error_details": error_msg,
                        "data": str(actual_data)[:500]
                    }
                )
                return False, None, None, error_msg
                
        except ValidationError as e:
            error_msg = str(e)
            logger.error(
                "Validation error: schema mismatch",
                extra={
                    "error_type": "ValidationError",
                    "error_details": error_msg,
                    "data": str(data)[:500]
                }
            )
            return False, None, None, error_msg
        except Exception as e:
            error_msg = f"Unexpected validation error: {str(e)}"
            logger.error(
                "Unexpected validation error",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data": str(data)[:500]
                },
                exc_info=True
            )
            return False, None, None, error_msg

    def enrich_message(self, data: Dict[str, Any], stream_type: str, topic: str) -> EnrichedMessage:
        """Enrich a validated message with metadata.
        
        Args:
            data: Original parsed message data
            stream_type: Detected stream type ('trade', 'kline', 'ticker')
            topic: Target Kafka topic
            
        Returns:
            EnrichedMessage with all metadata fields populated
        """
        if 'stream' in data and 'data' in data:
            actual_data = data['data']
        else:
            actual_data = data
        
        symbol = actual_data.get('s', '').upper()
        ingestion_timestamp = time.time_ns() // 1_000_000
        
        enriched = EnrichedMessage(
            original_data=actual_data,
            ingestion_timestamp=ingestion_timestamp,
            source="binance",
            data_version="v1",
            symbol=symbol,
            stream_type=stream_type,
            topic=topic
        )
        
        return enriched

    def determine_topic(self, stream_type: str) -> Optional[str]:
        """Determine the Kafka topic based on stream type.
        
        Args:
            stream_type: Stream type ('trade', 'kline', 'ticker')
            
        Returns:
            Kafka topic name, or None if stream type is unknown
        """
        topic_mapping = {
            'trade': 'raw_trades',
            'kline': 'raw_klines',
            'ticker': 'raw_tickers'
        }
        
        topic = topic_mapping.get(stream_type)
        
        if topic is None:
            logger.error(
                "Unknown stream type: cannot determine topic",
                extra={
                    "error_type": "UnknownStreamType",
                    "error_details": f"Unknown stream type: {stream_type}",
                    "stream_type": stream_type
                }
            )
        
        return topic


# ============================================================================
# WEBSOCKET CLIENT
# ============================================================================

class BinanceWebSocketClient:
    """Async WebSocket client for Binance streaming API.
    
    Features:
    - Automatic reconnection with exponential backoff
    - Keepalive mechanism (ping/pong)
    - Multi-stream subscription support
    """

    def __init__(self, url: str, streams: List[str]):
        """Initialize the WebSocket client.
        
        Args:
            url: WebSocket URL (e.g., wss://stream.binance.com:9443/stream)
            streams: List of stream names to subscribe to
        """
        self.url = url
        self.streams = streams
        
        # Connection state
        self.websocket: Optional["ClientConnection"] = None
        self.is_connected = False
        
        # Reconnection backoff using utils ExponentialBackoff (Requirement 9.1)
        self._backoff = ExponentialBackoff(
            initial_delay_ms=1000,  # 1 second initial delay
            max_delay_ms=config.RECONNECT_MAX_DELAY_SECONDS * 1000,  # Convert to ms
            multiplier=2.0,
            jitter_factor=0.1,
        )
        
        # Keepalive state
        self.keepalive_task: Optional[asyncio.Task] = None
        self.last_pong_time: Optional[float] = None
        self.ping_interval = config.PING_INTERVAL_SECONDS
        
        logger.info(f"Initialized BinanceWebSocketClient with URL: {url}, streams: {streams}")

    async def connect(self) -> None:
        """Connect to the Binance WebSocket API."""
        try:
            logger.info(f"Connecting to WebSocket: {self.url}")
            self.websocket = await asyncio.wait_for(
                websockets.connect(self.url),
                timeout=config.WS_CONNECTION_TIMEOUT
            )
            self.is_connected = True
            self.last_pong_time = time.time()
            logger.info("WebSocket connection established successfully")
            # Reset backoff on successful connection
            self._backoff.reset()
            
        except asyncio.TimeoutError:
            logger.error(
                f"Connection timeout after {config.WS_CONNECTION_TIMEOUT} seconds",
                extra={
                    "url": self.url,
                    "error_type": "TimeoutError",
                    "error_details": f"Connection timeout after {config.WS_CONNECTION_TIMEOUT} seconds"
                }
            )
            self.is_connected = False
            # Record connection timeout error (Requirement 5.3)
            record_error("binance_connector", "connection_timeout", "error")
            raise
        except Exception as e:
            logger.error(
                f"Connection error to WebSocket",
                extra={
                    "url": self.url,
                    "error_type": type(e).__name__,
                    "error_details": str(e)
                },
                exc_info=True
            )
            self.is_connected = False
            # Record connection error (Requirement 5.3)
            record_error("binance_connector", "connection_error", "error")
            raise

    async def subscribe(self) -> None:
        """Subscribe to the configured streams."""
        if not self.websocket or not self.is_connected:
            raise RuntimeError("WebSocket is not connected. Call connect() first.")
        
        subscription_message = {
            "method": "SUBSCRIBE",
            "params": self.streams,
            "id": 1
        }
        
        try:
            await self.websocket.send(json.dumps(subscription_message))
            logger.info(f"Successfully subscribed to streams: {self.streams}")
        except Exception as e:
            logger.error(f"Failed to send subscription message: {type(e).__name__}: {e}")
            raise

    async def send_ping(self) -> None:
        """Send a ping message to keep the connection alive."""
        if not self.websocket or not self.is_connected:
            logger.warning("Cannot send ping: WebSocket is not connected")
            return
        
        try:
            await self.websocket.ping()
            logger.debug("Sent ping message")
        except Exception as e:
            logger.error(f"Failed to send ping: {type(e).__name__}: {e}")
            raise
    
    async def _keepalive_loop(self) -> None:
        """Background task that sends ping messages periodically."""
        logger.info(f"Starting keepalive loop with interval: {self.ping_interval}s")
        
        while self.is_connected:
            try:
                await asyncio.sleep(self.ping_interval)
                if self.is_connected:
                    await self.send_ping()
            except asyncio.CancelledError:
                logger.info("Keepalive loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in keepalive loop: {type(e).__name__}: {e}")
    
    def start_keepalive(self) -> None:
        """Start the keepalive background task."""
        if self.keepalive_task is None or self.keepalive_task.done():
            self.keepalive_task = asyncio.create_task(self._keepalive_loop())
            logger.info("Keepalive task started")
    
    def _on_pong(self) -> None:
        """Callback when pong is received."""
        self.last_pong_time = time.time()
        logger.debug(f"Received pong at {self.last_pong_time}")

    async def reconnect_with_backoff(self) -> None:
        """Reconnect to WebSocket with exponential backoff strategy.
        
        Uses ExponentialBackoff from utils for consistent backoff behavior
        across the codebase (Requirement 9.1).
        """
        while True:
            try:
                # Get delay from ExponentialBackoff (handles multiplier and jitter)
                delay_ms = self._backoff.next_delay_ms()
                delay_seconds = delay_ms / 1000.0
                reconnect_attempt = self._backoff.attempt_count
                
                logger.info(
                    f"Attempting reconnection",
                    extra={
                        "reconnect_attempt": reconnect_attempt,
                        "delay_seconds": delay_seconds,
                        "url": self.url
                    }
                )
                await asyncio.sleep(delay_seconds)
                await self.connect()
                await self.subscribe()
                
                logger.info(
                    "Reconnection successful",
                    extra={
                        "reconnect_attempt": reconnect_attempt,
                        "url": self.url
                    }
                )
                break
                
            except Exception as e:
                reconnect_attempt = self._backoff.attempt_count
                # Calculate next delay for logging (peek at what next delay would be)
                next_delay_ms = self._backoff.initial_delay_ms * (self._backoff.multiplier ** self._backoff.attempt_count)
                next_delay_ms = min(next_delay_ms, self._backoff.max_delay_ms)
                next_delay_seconds = next_delay_ms / 1000.0
                
                logger.error(
                    f"Reconnection attempt failed",
                    extra={
                        "reconnect_attempt": reconnect_attempt,
                        "url": self.url,
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "delay_seconds": next_delay_seconds
                    }
                )
                # Record reconnection failure (Requirement 5.3)
                record_error("binance_connector", "reconnection_failed", "warning")
                
                logger.info(
                    f"Next reconnection attempt scheduled",
                    extra={
                        "reconnect_attempt": reconnect_attempt + 1,
                        "delay_seconds": next_delay_seconds,
                        "url": self.url
                    }
                )

    async def receive_messages(self) -> AsyncIterator[str]:
        """Async generator that yields raw JSON strings from WebSocket."""
        while True:
            try:
                if not self.websocket or not self.is_connected:
                    logger.warning("WebSocket not connected, attempting reconnection...")
                    await self.reconnect_with_backoff()
                    self.start_keepalive()
                
                message = await self.websocket.recv()
                self._on_pong()
                
                if isinstance(message, str):
                    yield message
                else:
                    logger.warning(f"Received non-string message: {type(message)}")
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.error(
                    "WebSocket connection closed",
                    extra={
                        "url": self.url,
                        "error_type": "ConnectionClosed",
                        "error_details": str(e)
                    }
                )
                self.is_connected = False
                # Record connection closed error (Requirement 5.3)
                record_error("binance_connector", "connection_closed", "warning")
                
            except Exception as e:
                logger.error(
                    "Error receiving message from WebSocket",
                    extra={
                        "url": self.url,
                        "error_type": type(e).__name__,
                        "error_details": str(e)
                    },
                    exc_info=True
                )
                self.is_connected = False
                # Record receive error (Requirement 5.3)
                record_error("binance_connector", "receive_error", "error")

    async def close(self) -> None:
        """Close the WebSocket connection gracefully."""
        logger.info("Closing WebSocket connection...")
        
        if self.keepalive_task and not self.keepalive_task.done():
            self.keepalive_task.cancel()
            try:
                await self.keepalive_task
            except asyncio.CancelledError:
                logger.info("Keepalive task cancelled successfully")
        
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("WebSocket connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {type(e).__name__}: {e}")
        
        self.is_connected = False
        self.websocket = None
