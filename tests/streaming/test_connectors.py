"""
Unit tests for connector utilities.

Tests Kafka, Redis, and DuckDB write operations with mocked connections
and error handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json


class TestRedisConnector:
    """Test Redis connector operations."""
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_write_hash_success(self, mock_pool, mock_redis):
        """Test Redis write with hash type."""
        from src.pyspark_streaming_processor.connectors import RedisConnector
        
        # Setup mocks
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        
        # Create connector and write
        connector = RedisConnector(host="localhost", port=6379)
        connector.write_to_redis(
            key="test:key",
            value={"field1": "value1", "field2": "value2"},
            ttl_seconds=3600,
            value_type="hash"
        )
        
        # Verify hset was called
        mock_client.hset.assert_called_once_with(
            "test:key",
            mapping={"field1": "value1", "field2": "value2"}
        )
        
        # Verify TTL was set
        mock_client.expire.assert_called_once_with("test:key", 3600)
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_write_list_success(self, mock_pool, mock_redis):
        """Test Redis write with list type."""
        from src.pyspark_streaming_processor.connectors import RedisConnector
        
        # Setup mocks
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        
        # Create connector and write
        connector = RedisConnector(host="localhost", port=6379)
        connector.write_to_redis(
            key="test:list",
            value={"alert": "data"},
            value_type="list",
            list_max_size=1000
        )
        
        # Verify lpush was called
        mock_client.lpush.assert_called_once()
        
        # Verify ltrim was called with max size
        mock_client.ltrim.assert_called_once_with("test:list", 0, 999)
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_write_string_success(self, mock_pool, mock_redis):
        """Test Redis write with string type."""
        from src.pyspark_streaming_processor.connectors import RedisConnector
        
        # Setup mocks
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        
        # Create connector and write
        connector = RedisConnector(host="localhost", port=6379)
        connector.write_to_redis(
            key="test:string",
            value={"data": "value"},
            value_type="string"
        )
        
        # Verify set was called with JSON string
        mock_client.set.assert_called_once()
        call_args = mock_client.set.call_args[0]
        assert call_args[0] == "test:string"
        assert json.loads(call_args[1]) == {"data": "value"}
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_connection_failure(self, mock_pool, mock_redis):
        """Test Redis connection failure handling."""
        from src.pyspark_streaming_processor.connectors import RedisConnector
        
        # Setup mock to fail on ping
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.side_effect = Exception("Connection refused")
        
        # Create connector - should raise exception
        connector = RedisConnector(host="localhost", port=6379)
        
        with pytest.raises(Exception) as exc_info:
            _ = connector.client
        
        assert "Connection refused" in str(exc_info.value)
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_write_failure(self, mock_pool, mock_redis):
        """Test Redis write failure handling."""
        from src.pyspark_streaming_processor.connectors import RedisConnector
        
        # Setup mocks
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        mock_client.hset.side_effect = Exception("Write failed")
        
        # Create connector and attempt write
        connector = RedisConnector(host="localhost", port=6379)
        
        with pytest.raises(Exception) as exc_info:
            connector.write_to_redis(
                key="test:key",
                value={"field": "value"},
                value_type="hash"
            )
        
        assert "Write failed" in str(exc_info.value)


class TestDuckDBConnector:
    """Test DuckDB connector operations."""
    
    def test_duckdb_write_success(self):
        """Test DuckDB write with in-memory database."""
        from src.pyspark_streaming_processor.connectors import DuckDBConnector
        
        # Create in-memory connector
        connector = DuckDBConnector(database_path=":memory:")
        
        # Create table
        connector.create_candles_table()
        
        # Write data
        data = [
            {
                "window_start": "2024-01-01 00:00:00",
                "window_end": "2024-01-01 00:01:00",
                "window_duration": "1m",
                "symbol": "BTCUSDT",
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 100.0,
                "quote_volume": 5000000.0,
                "trade_count": 500,
                "vwap": 50000.0,
                "price_change_pct": 0.1,
                "buy_sell_ratio": 1.2,
                "large_order_count": 5,
                "price_stddev": 50.0
            }
        ]
        
        connector.write_to_duckdb("candles", data)
        
        # Verify data was written
        result = connector.connection.execute("SELECT COUNT(*) FROM candles").fetchone()
        assert result[0] == 1
        
        # Verify data content
        result = connector.connection.execute("SELECT symbol, open, close FROM candles").fetchone()
        assert result[0] == "BTCUSDT"
        assert result[1] == 50000.0
        assert result[2] == 50050.0
        
        connector.close()
    
    def test_duckdb_write_indicators(self):
        """Test DuckDB write for indicators table."""
        from src.pyspark_streaming_processor.connectors import DuckDBConnector
        
        # Create in-memory connector
        connector = DuckDBConnector(database_path=":memory:")
        
        # Create table
        connector.create_indicators_table()
        
        # Write data
        data = [
            {
                "timestamp": "2024-01-01 00:00:00",
                "symbol": "BTCUSDT",
                "sma_5": 50000.0,
                "sma_10": 49900.0,
                "sma_20": 49800.0,
                "sma_50": 49500.0,
                "ema_12": 50050.0,
                "ema_26": 49950.0,
                "rsi_14": 55.0,
                "macd_line": 100.0,
                "macd_signal": 90.0,
                "macd_histogram": 10.0,
                "bb_middle": 50000.0,
                "bb_upper": 51000.0,
                "bb_lower": 49000.0,
                "atr_14": 200.0
            }
        ]
        
        connector.write_to_duckdb("indicators", data)
        
        # Verify data was written
        result = connector.connection.execute("SELECT COUNT(*) FROM indicators").fetchone()
        assert result[0] == 1
        
        connector.close()
    
    def test_duckdb_write_alerts(self):
        """Test DuckDB write for alerts table."""
        from src.pyspark_streaming_processor.connectors import DuckDBConnector
        
        # Create in-memory connector
        connector = DuckDBConnector(database_path=":memory:")
        
        # Create table
        connector.create_alerts_table()
        
        # Write data
        data = [
            {
                "timestamp": "2024-01-01 00:00:00",
                "symbol": "BTCUSDT",
                "alert_type": "WHALE_ALERT",
                "alert_level": "HIGH",
                "details": '{"price": 50000.0, "quantity": 3.0, "value": 150000.0}',
                "alert_id": "test-alert-123",
                "created_at": "2024-01-01 00:00:01"
            }
        ]
        
        connector.write_to_duckdb("alerts", data)
        
        # Verify data was written
        result = connector.connection.execute("SELECT COUNT(*) FROM alerts").fetchone()
        assert result[0] == 1
        
        # Verify alert content
        result = connector.connection.execute(
            "SELECT alert_type, alert_level FROM alerts"
        ).fetchone()
        assert result[0] == "WHALE_ALERT"
        assert result[1] == "HIGH"
        
        connector.close()
    
    def test_duckdb_write_empty_data(self):
        """Test DuckDB write with empty data."""
        from src.pyspark_streaming_processor.connectors import DuckDBConnector
        
        # Create in-memory connector
        connector = DuckDBConnector(database_path=":memory:")
        
        # Create table
        connector.create_candles_table()
        
        # Write empty data (should not raise error)
        connector.write_to_duckdb("candles", [])
        
        # Verify no data was written
        result = connector.connection.execute("SELECT COUNT(*) FROM candles").fetchone()
        assert result[0] == 0
        
        connector.close()
    
    def test_duckdb_multiple_writes(self):
        """Test multiple writes to DuckDB."""
        from src.pyspark_streaming_processor.connectors import DuckDBConnector
        
        # Create in-memory connector
        connector = DuckDBConnector(database_path=":memory:")
        
        # Create table
        connector.create_alerts_table()
        
        # Write first batch
        data1 = [
            {
                "timestamp": "2024-01-01 00:00:00",
                "symbol": "BTCUSDT",
                "alert_type": "WHALE_ALERT",
                "alert_level": "HIGH",
                "details": "{}",
                "alert_id": "alert-1",
                "created_at": "2024-01-01 00:00:00"
            }
        ]
        connector.write_to_duckdb("alerts", data1)
        
        # Write second batch
        data2 = [
            {
                "timestamp": "2024-01-01 00:01:00",
                "symbol": "ETHUSDT",
                "alert_type": "PRICE_SPIKE",
                "alert_level": "HIGH",
                "details": "{}",
                "alert_id": "alert-2",
                "created_at": "2024-01-01 00:01:00"
            }
        ]
        connector.write_to_duckdb("alerts", data2, create_table=False)
        
        # Verify both records exist
        result = connector.connection.execute("SELECT COUNT(*) FROM alerts").fetchone()
        assert result[0] == 2
        
        connector.close()


class TestKafkaConnector:
    """Test Kafka connector operations (mocked)."""
    
    @patch('kafka.KafkaProducer')
    def test_kafka_producer_creation(self, mock_producer_class):
        """Test Kafka producer can be created."""
        # This is a basic test since we can't easily test Spark streaming without full setup
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Import kafka to trigger the mock
        from kafka import KafkaProducer
        
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        
        assert producer is not None
        mock_producer_class.assert_called_once()
