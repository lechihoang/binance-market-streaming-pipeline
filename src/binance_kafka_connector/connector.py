"""
Binance-Kafka Connector

Real-time cryptocurrency data ingestion from Binance WebSocket to Kafka.
"""

import asyncio
import json
import time
from typing import Any, AsyncIterator, Dict, List, Optional

import websockets
from pydantic import BaseModel
from websockets.asyncio.client import ClientConnection

from src.utils.config import get_env_str, get_env_int, get_env_list
from src.utils.logging import setup_logging, get_logger
from src.utils.retry import ExponentialBackoff
from src.utils.metrics import record_error

logger = get_logger(__name__)

# Default streams (trade + ticker only, kline removed as unused)
_DEFAULT_STREAMS = [
    # Trade streams - used by TradeAggregationJob and AnomalyDetectionJob
    "btcusdt@trade", "ethusdt@trade", "bnbusdt@trade", "solusdt@trade", "xrpusdt@trade",
    # Ticker streams - used by TickerConsumer
    "btcusdt@ticker", "ethusdt@ticker", "bnbusdt@ticker", "solusdt@ticker", "xrpusdt@ticker",
    "adausdt@ticker", "dogeusdt@ticker", "avaxusdt@ticker", "dotusdt@ticker", "maticusdt@ticker",
    "linkusdt@ticker", "ltcusdt@ticker", "uniusdt@ticker", "atomusdt@ticker", "shibusdt@ticker",
]

# Configuration
BINANCE_WS_URL = get_env_str("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
BINANCE_STREAMS = get_env_list("BINANCE_STREAMS", _DEFAULT_STREAMS)
RECONNECT_MAX_DELAY_SECONDS = get_env_int("RECONNECT_MAX_DELAY_SECONDS", 60)
WS_CONNECTION_TIMEOUT = get_env_int("WS_CONNECTION_TIMEOUT", 10)
KAFKA_BOOTSTRAP_SERVERS = get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LOG_LEVEL = get_env_str("LOG_LEVEL", "INFO")

# Event type mapping: event -> (stream_type, topic)
# Note: kline removed as it was not being consumed by any downstream processor
EVENT_MAPPING = {
    'trade': ('trade', 'raw_trades'),
    '24hrTicker': ('ticker', 'raw_tickers'),
}


class EnrichedMessage(BaseModel):
    """Enriched message with metadata."""
    original_data: Dict[str, Any]
    ingestion_timestamp: int
    symbol: str
    stream_type: str
    topic: str


def process_message(raw_json: str) -> Optional[EnrichedMessage]:
    """Parse and enrich a raw WebSocket message."""
    try:
        data = json.loads(raw_json)
        actual_data = data['data']
        event_type = actual_data.get('e')
        
        if event_type not in EVENT_MAPPING:
            return None
        
        stream_type, topic = EVENT_MAPPING[event_type]
        return EnrichedMessage(
            original_data=actual_data,
            ingestion_timestamp=time.time_ns() // 1_000_000,
            symbol=actual_data.get('s', '').upper(),
            stream_type=stream_type,
            topic=topic
        )
    except Exception:
        return None


class BinanceWebSocketClient:
    """Async WebSocket client for Binance with auto-reconnection."""

    def __init__(self, url: str, streams: List[str]):
        self.url = url
        self.streams = streams
        self.websocket: Optional[ClientConnection] = None
        self.is_connected = False
        self._backoff = ExponentialBackoff(
            initial_delay_ms=1000,
            max_delay_ms=RECONNECT_MAX_DELAY_SECONDS * 1000,
            multiplier=2.0,
            jitter_factor=0.1,
        )

    async def connect(self) -> None:
        """Connect to Binance WebSocket API."""
        try:
            logger.info(f"Connecting to {self.url}")
            self.websocket = await asyncio.wait_for(
                websockets.connect(self.url),
                timeout=WS_CONNECTION_TIMEOUT
            )
            self.is_connected = True
            self._backoff.reset()
            logger.info("WebSocket connected")
        except asyncio.TimeoutError:
            logger.error(f"Connection timeout after {WS_CONNECTION_TIMEOUT}s")
            self.is_connected = False
            record_error("binance_connector", "connection_timeout", "error")
            raise
        except Exception as e:
            logger.error(f"Connection error: {e}", exc_info=True)
            self.is_connected = False
            record_error("binance_connector", "connection_error", "error")
            raise

    async def subscribe(self) -> None:
        """Subscribe to configured streams."""
        if not self.websocket or not self.is_connected:
            raise RuntimeError("WebSocket not connected")
        
        msg = {"method": "SUBSCRIBE", "params": self.streams, "id": 1}
        await self.websocket.send(json.dumps(msg))

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        while True:
            try:
                delay = self._backoff.next_delay_ms() / 1000.0
                logger.info(f"Reconnecting in {delay:.1f}s (attempt {self._backoff.attempt_count})")
                await asyncio.sleep(delay)
                await self.connect()
                await self.subscribe()
                logger.info("Reconnected successfully")
                break
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                record_error("binance_connector", "reconnection_failed", "warning")

    async def receive_messages(self) -> AsyncIterator[str]:
        """Yield messages from WebSocket."""
        while True:
            try:
                if not self.websocket or not self.is_connected:
                    await self._reconnect()
                
                message = await self.websocket.recv()
                if isinstance(message, str):
                    yield message
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Connection closed, reconnecting...")
                self.is_connected = False
                record_error("binance_connector", "connection_closed", "warning")
            except Exception as e:
                logger.error(f"Receive error: {e}", exc_info=True)
                self.is_connected = False
                record_error("binance_connector", "receive_error", "error")

    async def close(self) -> None:
        """Close WebSocket connection."""
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("WebSocket closed")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        self.is_connected = False
        self.websocket = None


class BinanceKafkaConnector:
    """Main connector: WebSocket -> Kafka."""
    
    def __init__(self):
        from src.utils.kafka import KafkaConnector
        
        self.ws_client = BinanceWebSocketClient(BINANCE_WS_URL, BINANCE_STREAMS)
        self.kafka = KafkaConnector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            linger_ms=100,
            batch_size=16384,
            value_serializer=lambda m: json.dumps(m.model_dump()).encode('utf-8'),
        )

    async def run(self) -> None:
        """Main loop: receive, process, send to Kafka."""
        await self.ws_client.connect()
        await self.ws_client.subscribe()
        logger.info(f"Started with {len(BINANCE_STREAMS)} streams")
        
        async for raw_message in self.ws_client.receive_messages():
            enriched = process_message(raw_message)
            if enriched:
                self.kafka.send(topic=enriched.topic, value=enriched, key=enriched.symbol)


def main():
    """Entry point."""
    setup_logging(level=LOG_LEVEL, json_output=True)
    logger.info("Starting Binance-Kafka Connector...")
    connector = BinanceKafkaConnector()
    asyncio.run(connector.run())


if __name__ == "__main__":
    main()
