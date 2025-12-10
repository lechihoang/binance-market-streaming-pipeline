"""Main entry point for running the Binance-Kafka Connector.

This module provides a simple CLI interface to start the connector.
"""

import asyncio
import logging
import sys

from .connector import BinanceKafkaConnector
from .core import config, setup_structured_logging


def setup_logging():
    """Set up logging configuration."""
    # Use structured JSON logging
    setup_structured_logging(log_level=config.LOG_LEVEL)


async def main():
    """Main entry point for the application."""
    # Set up logging
    setup_logging()
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Binance-Kafka Connector...")
    
    try:
        # Create and run the connector
        connector = BinanceKafkaConnector()
        await connector.run()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
