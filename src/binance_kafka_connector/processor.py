"""Message processor for parsing, validating, and enriching Binance messages.

This module handles the transformation of raw WebSocket messages into
validated and enriched messages ready for Kafka production.
"""

import json
import time
import logging
from typing import Optional, Dict, Any, Union
from pydantic import ValidationError

from .models import (
    TradeMessage,
    KlineMessage,
    TickerMessage,
    EnrichedMessage,
)


logger = logging.getLogger(__name__)


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
            # Log parsing error with raw message content
            logger.error(
                "Failed to parse JSON message",
                extra={
                    "error_type": "JSONDecodeError",
                    "error_details": str(e),
                    "raw_message": raw_json[:500]  # Truncate to avoid huge logs
                }
            )
            return None
        except Exception as e:
            # Log unexpected parsing error with raw message content
            logger.error(
                "Unexpected error parsing message",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "raw_message": raw_json[:500]  # Truncate to avoid huge logs
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
            - is_valid: True if validation passed
            - validated_message: The validated Pydantic model instance, or None if validation failed
            - stream_type: Detected stream type ('trade', 'kline', 'ticker'), or None if unknown
            - error_message: Error details if validation failed, None otherwise
        """
        try:
            # Extract actual data if message is wrapped in Binance stream format
            # Binance WebSocket returns: {"stream": "btcusdt@trade", "data": {...}}
            if 'stream' in data and 'data' in data:
                actual_data = data['data']
            else:
                actual_data = data
            
            # Detect stream type from event field
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
                # Log validation error with message data
                logger.error(
                    "Validation failed: unknown event type",
                    extra={
                        "error_type": "UnknownEventType",
                        "error_details": error_msg,
                        "data": str(actual_data)[:500]  # Truncate to avoid huge logs
                    }
                )
                return False, None, None, error_msg
                
        except ValidationError as e:
            error_msg = str(e)
            # Log validation error with message data
            logger.error(
                "Validation error: schema mismatch",
                extra={
                    "error_type": "ValidationError",
                    "error_details": error_msg,
                    "data": str(data)[:500]  # Truncate to avoid huge logs
                }
            )
            return False, None, None, error_msg
        except Exception as e:
            error_msg = f"Unexpected validation error: {str(e)}"
            # Log unexpected validation error with message data
            logger.error(
                "Unexpected validation error",
                extra={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data": str(data)[:500]  # Truncate to avoid huge logs
                },
                exc_info=True
            )
            return False, None, None, error_msg

    def enrich_message(self, data: Dict[str, Any], stream_type: str, topic: str) -> EnrichedMessage:
        """Enrich a validated message with metadata.
        
        Args:
            data: Original parsed message data (may be wrapped in {"stream": "...", "data": {...}} format)
            stream_type: Detected stream type ('trade', 'kline', 'ticker')
            topic: Target Kafka topic
            
        Returns:
            EnrichedMessage with all metadata fields populated
        """
        # Extract actual data if message is wrapped in Binance stream format
        if 'stream' in data and 'data' in data:
            actual_data = data['data']
        else:
            actual_data = data
        
        # Extract symbol from message
        symbol = actual_data.get('s', '').upper()
        
        # Get current timestamp in milliseconds
        ingestion_timestamp = time.time_ns() // 1_000_000
        
        # Create enriched message (store the actual data, not the wrapper)
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
            # Log unknown stream type error
            logger.error(
                "Unknown stream type: cannot determine topic",
                extra={
                    "error_type": "UnknownStreamType",
                    "error_details": f"Unknown stream type: {stream_type}",
                    "stream_type": stream_type
                }
            )
        
        return topic
