"""
Ticker Consumer Entrypoint

Main entry point for the real-time ticker consumer service.
Provides CLI interface and signal handling for graceful shutdown.

Requirements:
- 2.1: Operate as standalone service
- 2.4: Graceful shutdown with signal handling
"""

import argparse
import logging
import signal
import sys
from typing import Optional

from src.ticker_consumer.core import TickerConsumerConfig
from src.ticker_consumer.service import TickerConsumer

# Global consumer instance for signal handling
_consumer: Optional[TickerConsumer] = None


def setup_logging(log_level: str) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    
    # Reduce noise from kafka library
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("kafka.conn").setLevel(logging.WARNING)


def signal_handler(signum: int, frame) -> None:
    """
    Handle shutdown signals gracefully.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    signal_name = signal.Signals(signum).name
    logging.info(f"Received signal {signal_name}, initiating graceful shutdown...")
    
    if _consumer:
        _consumer.request_shutdown()


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Real-time Ticker Consumer Service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    
    parser.add_argument(
        "--kafka-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (overrides KAFKA_BOOTSTRAP_SERVERS env var)",
    )
    
    parser.add_argument(
        "--redis-host",
        type=str,
        default=None,
        help="Redis host (overrides REDIS_HOST env var)",
    )
    
    parser.add_argument(
        "--redis-port",
        type=int,
        default=None,
        help="Redis port (overrides REDIS_PORT env var)",
    )
    
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to track (overrides TICKER_SYMBOLS env var)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print configuration and exit without starting consumer",
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Main entry point for ticker consumer.
    
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    global _consumer
    
    # Parse arguments
    args = parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("Ticker Consumer Service Starting")
    logger.info("=" * 60)
    
    try:
        # Load configuration from environment
        config = TickerConsumerConfig.from_env()
        
        # Override with command line arguments
        if args.kafka_servers:
            config.kafka.bootstrap_servers = args.kafka_servers
        
        if args.redis_host:
            config.redis.host = args.redis_host
        
        if args.redis_port:
            config.redis.port = args.redis_port
        
        if args.symbols:
            config.symbols = [s.strip().upper() for s in args.symbols.split(",")]
        
        config.log_level = args.log_level
        
        # Dry run - just print config and exit
        if args.dry_run:
            config.log_config()
            logger.info("Dry run complete, exiting...")
            return 0
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("Signal handlers registered for SIGINT and SIGTERM")
        
        # Create and run consumer
        _consumer = TickerConsumer(config)
        _consumer.run()
        
        logger.info("Ticker Consumer Service stopped successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        if _consumer:
            _consumer.request_shutdown()
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
