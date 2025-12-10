"""
Graceful Shutdown Module

Provides graceful shutdown capabilities for Spark streaming jobs.
Ensures that in-progress batches complete before job termination.

NOTE: This module re-exports from src.utils.shutdown for backward compatibility.
New code should import directly from src.utils.shutdown.
"""

# Re-export from utils for backward compatibility
from src.utils.shutdown import (
    GracefulShutdown,
    ShutdownState,
    ShutdownEvent,
)

__all__ = [
    "GracefulShutdown",
    "ShutdownState",
    "ShutdownEvent",
]
