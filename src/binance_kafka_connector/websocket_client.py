"""WebSocket client for connecting to Binance streaming API.

This module provides an async WebSocket client with reconnection logic,
keepalive mechanism, and exponential backoff strategy.
"""

import asyncio
import json
import logging
import time
from typing import AsyncIterator, List, Optional, TYPE_CHECKING

import websockets

if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection

from .config import config

logger = logging.getLogger(__name__)


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
        
        # Reconnection backoff state
        self.current_delay = 1  # Start with 1 second
        self.max_delay = config.RECONNECT_MAX_DELAY_SECONDS
        
        # Keepalive state
        self.keepalive_task: Optional[asyncio.Task] = None
        self.last_pong_time: Optional[float] = None
        self.ping_interval = config.PING_INTERVAL_SECONDS
        
        logger.info(f"Initialized BinanceWebSocketClient with URL: {url}, streams: {streams}")

    async def connect(self) -> None:
        """Connect to the Binance WebSocket API.
        
        Establishes a WebSocket connection with a 10-second timeout.
        Handles connection errors and logs them.
        
        Raises:
            Exception: If connection fails after timeout or other errors
        """
        try:
            logger.info(f"Connecting to WebSocket: {self.url}")
            self.websocket = await asyncio.wait_for(
                websockets.connect(self.url),
                timeout=config.WS_CONNECTION_TIMEOUT
            )
            self.is_connected = True
            self.last_pong_time = time.time()
            logger.info("WebSocket connection established successfully")
            
            # Reset backoff delay on successful connection
            self.current_delay = 1
            
        except asyncio.TimeoutError:
            # Log with structured context
            logger.error(
                f"Connection timeout after {config.WS_CONNECTION_TIMEOUT} seconds",
                extra={
                    "url": self.url,
                    "error_type": "TimeoutError",
                    "error_details": f"Connection timeout after {config.WS_CONNECTION_TIMEOUT} seconds"
                }
            )
            self.is_connected = False
            raise
        except Exception as e:
            # Log with structured context
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
            raise

    async def subscribe(self) -> None:
        """Subscribe to the configured streams.
        
        Sends a subscription message to the WebSocket with the list of streams.
        Logs subscription success.
        
        Raises:
            Exception: If websocket is not connected or send fails
        """
        if not self.websocket or not self.is_connected:
            raise RuntimeError("WebSocket is not connected. Call connect() first.")
        
        # Build subscription message according to Binance API format
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
        """Send a ping message to keep the connection alive.
        
        Raises:
            Exception: If websocket is not connected or send fails
        """
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
        """Background task that sends ping messages periodically.
        
        Runs every PING_INTERVAL_SECONDS (default 180 seconds).
        Tracks last pong time and resets timer on pong received.
        """
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
                # Continue the loop even on error
    
    def start_keepalive(self) -> None:
        """Start the keepalive background task."""
        if self.keepalive_task is None or self.keepalive_task.done():
            self.keepalive_task = asyncio.create_task(self._keepalive_loop())
            logger.info("Keepalive task started")
    
    def _on_pong(self) -> None:
        """Callback when pong is received. Resets the keepalive timer."""
        self.last_pong_time = time.time()
        logger.debug(f"Received pong at {self.last_pong_time}")

    async def reconnect_with_backoff(self) -> None:
        """Reconnect to WebSocket with exponential backoff strategy.
        
        Implements exponential backoff:
        - Starts with 1 second delay
        - Doubles delay on each failure (1s → 2s → 4s → 8s...)
        - Caps maximum delay at 60 seconds (configurable)
        - Resets delay to 1s on successful connection
        """
        reconnect_attempt = 0
        
        while True:
            try:
                reconnect_attempt += 1
                logger.info(
                    f"Attempting reconnection",
                    extra={
                        "reconnect_attempt": reconnect_attempt,
                        "delay_seconds": self.current_delay,
                        "url": self.url
                    }
                )
                await asyncio.sleep(self.current_delay)
                
                # Attempt to connect
                await self.connect()
                
                # If successful, subscribe to streams
                await self.subscribe()
                
                # Connection successful, delay is already reset in connect()
                logger.info(
                    "Reconnection successful",
                    extra={
                        "reconnect_attempt": reconnect_attempt,
                        "url": self.url
                    }
                )
                break
                
            except Exception as e:
                # Log reconnection failure with structured context
                logger.error(
                    f"Reconnection attempt failed",
                    extra={
                        "reconnect_attempt": reconnect_attempt,
                        "url": self.url,
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "delay_seconds": self.current_delay
                    }
                )
                
                # Double the delay for next attempt
                self.current_delay = min(self.current_delay * 2, self.max_delay)
                logger.info(
                    f"Next reconnection attempt scheduled",
                    extra={
                        "reconnect_attempt": reconnect_attempt + 1,
                        "delay_seconds": self.current_delay,
                        "url": self.url
                    }
                )

    async def receive_messages(self) -> AsyncIterator[str]:
        """Async generator that yields raw JSON strings from WebSocket.
        
        Handles WebSocket errors and triggers reconnection when needed.
        Continues yielding messages after successful reconnection.
        
        Yields:
            str: Raw JSON message strings from the WebSocket
        """
        while True:
            try:
                if not self.websocket or not self.is_connected:
                    logger.warning("WebSocket not connected, attempting reconnection...")
                    await self.reconnect_with_backoff()
                    self.start_keepalive()
                
                # Receive message from WebSocket
                message = await self.websocket.recv()
                
                # Update pong time (websocket library handles pong automatically)
                self._on_pong()
                
                # Yield the raw JSON string
                if isinstance(message, str):
                    yield message
                else:
                    logger.warning(f"Received non-string message: {type(message)}")
                    
            except websockets.exceptions.ConnectionClosed as e:
                # Log connection closed with structured context
                logger.error(
                    "WebSocket connection closed",
                    extra={
                        "url": self.url,
                        "error_type": "ConnectionClosed",
                        "error_details": str(e)
                    }
                )
                self.is_connected = False
                # Loop will trigger reconnection on next iteration
                
            except Exception as e:
                # Log error receiving message with structured context
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
                # Loop will trigger reconnection on next iteration

    async def close(self) -> None:
        """Close the WebSocket connection gracefully.
        
        Stops the keepalive task and closes the WebSocket connection.
        """
        logger.info("Closing WebSocket connection...")
        
        # Stop keepalive task
        if self.keepalive_task and not self.keepalive_task.done():
            self.keepalive_task.cancel()
            try:
                await self.keepalive_task
            except asyncio.CancelledError:
                logger.info("Keepalive task cancelled successfully")
        
        # Close WebSocket connection
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("WebSocket connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {type(e).__name__}: {e}")
        
        self.is_connected = False
        self.websocket = None
