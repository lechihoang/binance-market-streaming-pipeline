"""
API Routers - Consolidated router modules.

Exports:
- market_routes: Market data, ticker, and realtime WebSocket endpoints
- analytics_routes: Analytics, klines, indicators, and alerts endpoints
- system: System health, metrics, and status endpoints
"""

from src.api.routers import market_routes, analytics_routes, system

# Re-export routers for backward compatibility
market_router = market_routes.router
analytics_router = analytics_routes.router
system_router = system.router

__all__ = [
    "market_routes",
    "analytics_routes",
    "system",
    "market_router",
    "analytics_router",
    "system_router",
]
