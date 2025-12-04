"""
Realtime Router - WebSocket endpoints for real-time data streaming.

Provides WebSocket endpoints for trades, klines, and ticker data
using Redis pub/sub subscriptions.
"""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, Set, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, status
from starlette.websockets import WebSocketState

from src.api.dependencies import get_redis
from src.storage.redis_storage import RedisStorage


logger = logging.getLogger(__name__)

router = APIRouter(tags=["realtime"])


class ConnectionManager:
    """Manages WebSocket connections with per-IP limits and heartbeat."""
    
    MAX_CONNECTIONS_PER_IP = 10
    HEARTBEAT_INTERVAL = 30  # seconds
    
    def __init__(self):
        # Track active connections: {websocket: ip_address}
        self.active_connections: Dict[WebSocket, str] = {}
        # Track connections per IP: {ip_address: set of websockets}
        self.connections_by_ip: Dict[str, Set[WebSocket]] = defaultdict(set)
        # Track heartbeat tasks: {websocket: task}
        self.heartbeat_tasks: Dict[WebSocket, asyncio.Task] = {}
    
    def get_client_ip(self, websocket: WebSocket) -> str:
        """Extract client IP from WebSocket connection."""
        # Try X-Forwarded-For header first (for proxied connections)
        forwarded = websocket.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        # Fall back to direct client host
        client = websocket.client
        return client.host if client else "unknown"
    
    def can_connect(self, ip_address: str) -> bool:
        """Check if IP can accept new connection."""
        return len(self.connections_by_ip[ip_address]) < self.MAX_CONNECTIONS_PER_IP
    
    async def connect(self, websocket: WebSocket) -> bool:
        """Accept WebSocket connection if within limits.
        
        Returns:
            True if connection accepted, False if rejected
        """
        ip_address = self.get_client_ip(websocket)
        
        if not self.can_connect(ip_address):
            # Reject connection - policy violation
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            logger.warning(f"Rejected WebSocket from {ip_address}: max connections exceeded")
            return False
        
        await websocket.accept()
        self.active_connections[websocket] = ip_address
        self.connections_by_ip[ip_address].add(websocket)
        
        # Start heartbeat task
        self.heartbeat_tasks[websocket] = asyncio.create_task(
            self._heartbeat_loop(websocket)
        )
        
        logger.info(f"WebSocket connected from {ip_address}")
        return True
    
    def disconnect(self, websocket: WebSocket) -> None:
        """Remove WebSocket connection from tracking."""
        ip_address = self.active_connections.pop(websocket, None)
        if ip_address:
            self.connections_by_ip[ip_address].discard(websocket)
            if not self.connections_by_ip[ip_address]:
                del self.connections_by_ip[ip_address]
        
        # Cancel heartbeat task
        task = self.heartbeat_tasks.pop(websocket, None)
        if task:
            task.cancel()
        
        logger.info(f"WebSocket disconnected from {ip_address}")
    
    async def _heartbeat_loop(self, websocket: WebSocket) -> None:
        """Send periodic heartbeat messages."""
        try:
            while True:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                if websocket.client_state == WebSocketState.CONNECTED:
                    await self.send_message(websocket, {
                        "type": "heartbeat",
                        "timestamp": datetime.now().isoformat()
                    })
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Heartbeat loop ended: {e}")
    
    async def send_message(self, websocket: WebSocket, message: dict) -> bool:
        """Send JSON message to WebSocket.
        
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json(message)
                return True
        except Exception as e:
            logger.debug(f"Failed to send message: {e}")
        return False
    
    async def broadcast(self, message: dict, connections: Set[WebSocket]) -> None:
        """Broadcast message to multiple connections."""
        for websocket in list(connections):
            await self.send_message(websocket, message)
    
    def get_connection_count(self, ip_address: Optional[str] = None) -> int:
        """Get number of active connections."""
        if ip_address:
            return len(self.connections_by_ip.get(ip_address, set()))
        return len(self.active_connections)


# Global connection manager instance
manager = ConnectionManager()


def get_connection_manager() -> ConnectionManager:
    """Dependency to get connection manager."""
    return manager


@router.websocket("/trades/{symbol}")
async def websocket_trades(
    websocket: WebSocket,
    symbol: str,
    redis: RedisStorage = Depends(get_redis),
):
    """WebSocket endpoint for real-time trade updates.
    
    Subscribes to Redis pub/sub channel for trade updates.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
    """
    if not await manager.connect(websocket):
        return
    
    pubsub = None
    try:
        # Subscribe to Redis pub/sub channel
        channel = f"trades:{symbol}"
        pubsub = redis.client.pubsub()
        pubsub.subscribe(channel)
        
        # Send initial confirmation
        await manager.send_message(websocket, {
            "type": "subscribed",
            "channel": channel,
            "symbol": symbol,
            "timestamp": datetime.now().isoformat()
        })
        
        # Listen for messages
        while True:
            # Check for incoming client messages (for graceful disconnect)
            try:
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.1
                )
                # Handle ping/pong or close
                if client_msg == "close":
                    break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            
            # Check for pub/sub messages
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message and message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await manager.send_message(websocket, {
                        "type": "trade",
                        "symbol": symbol,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in pub/sub message: {message['data']}")
            
            # Small sleep to prevent busy loop
            await asyncio.sleep(0.01)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for trades/{symbol}")
    except Exception as e:
        logger.error(f"Error in trades WebSocket: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "message": f"Data source error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
    finally:
        if pubsub:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass
        manager.disconnect(websocket)


@router.websocket("/klines/{symbol}/{interval}")
async def websocket_klines(
    websocket: WebSocket,
    symbol: str,
    interval: str,
    redis: RedisStorage = Depends(get_redis),
):
    """WebSocket endpoint for real-time kline/candlestick updates.
    
    Subscribes to Redis pub/sub channel for kline updates.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Kline interval (e.g., 1m, 5m, 1h)
    """
    if not await manager.connect(websocket):
        return
    
    pubsub = None
    try:
        # Subscribe to Redis pub/sub channel
        channel = f"klines:{symbol}:{interval}"
        pubsub = redis.client.pubsub()
        pubsub.subscribe(channel)
        
        # Send initial confirmation
        await manager.send_message(websocket, {
            "type": "subscribed",
            "channel": channel,
            "symbol": symbol,
            "interval": interval,
            "timestamp": datetime.now().isoformat()
        })
        
        # Listen for messages
        while True:
            # Check for incoming client messages
            try:
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.1
                )
                if client_msg == "close":
                    break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            
            # Check for pub/sub messages
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message and message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await manager.send_message(websocket, {
                        "type": "kline",
                        "symbol": symbol,
                        "interval": interval,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in pub/sub message: {message['data']}")
            
            await asyncio.sleep(0.01)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for klines/{symbol}/{interval}")
    except Exception as e:
        logger.error(f"Error in klines WebSocket: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "message": f"Data source error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
    finally:
        if pubsub:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass
        manager.disconnect(websocket)


@router.websocket("/ticker")
async def websocket_ticker(
    websocket: WebSocket,
    redis: RedisStorage = Depends(get_redis),
):
    """WebSocket endpoint for all tickers broadcast.
    
    Broadcasts all symbol ticker data every 1 second.
    """
    if not await manager.connect(websocket):
        return
    
    BROADCAST_INTERVAL = 1.0  # seconds
    
    try:
        # Send initial confirmation
        await manager.send_message(websocket, {
            "type": "subscribed",
            "channel": "ticker",
            "timestamp": datetime.now().isoformat()
        })
        
        while True:
            # Check for incoming client messages
            try:
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.1
                )
                if client_msg == "close":
                    break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            
            # Collect all tickers from Redis
            try:
                tickers = []
                # Scan for all ticker keys
                cursor = 0
                while True:
                    cursor, keys = redis.client.scan(
                        cursor=cursor,
                        match="latest_ticker:*",
                        count=100
                    )
                    for key in keys:
                        symbol = key.replace("latest_ticker:", "")
                        data = redis.get_latest_ticker(symbol)
                        if data:
                            tickers.append({
                                "symbol": symbol,
                                **data
                            })
                    if cursor == 0:
                        break
                
                # Broadcast tickers
                await manager.send_message(websocket, {
                    "type": "ticker",
                    "data": tickers,
                    "count": len(tickers),
                    "timestamp": datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error fetching tickers: {e}")
                await manager.send_message(websocket, {
                    "type": "error",
                    "message": f"Data source error: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
            
            # Wait for next broadcast interval
            await asyncio.sleep(BROADCAST_INTERVAL)
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for ticker")
    except Exception as e:
        logger.error(f"Error in ticker WebSocket: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "message": f"Data source error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
    finally:
        manager.disconnect(websocket)
