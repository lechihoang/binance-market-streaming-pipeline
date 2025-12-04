"""
Property-based tests for StorageWriter.

Feature: three-tier-storage
Tests correctness properties for multi-tier write coordinator.
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from hypothesis import given, settings, strategies as st, HealthCheck
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.storage.redis_storage import RedisStorage
from src.storage.duckdb_storage import DuckDBStorage
from src.storage.parquet_storage import ParquetStorage
from src.storage.storage_writer import StorageWriter


# =============================================================================
# Redis availability check
# =============================================================================

def is_redis_available():
    """Check if Redis is available for testing."""
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379, db=15, socket_connect_timeout=1)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


REDIS_AVAILABLE = is_redis_available()
skip_if_no_redis = pytest.mark.skipif(
    not REDIS_AVAILABLE,
    reason="Redis server not available at localhost:6379"
)


# =============================================================================
# Helper to create fresh storage instances for each test
# =============================================================================

@contextmanager
def fresh_storage_writer():
    """Create fresh storage instances for each test example."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create DuckDB storage
        db_path = os.path.join(tmpdir, "test_crypto.duckdb")
        duckdb_storage = DuckDBStorage(db_path=db_path)
        
        # Create Parquet storage
        parquet_path = os.path.join(tmpdir, "parquet")
        parquet_storage = ParquetStorage(base_path=parquet_path)
        
        # Create Redis storage (use db=15 for tests)
        redis_storage = RedisStorage(host="localhost", port=6379, db=15)
        redis_storage.flush_db()
        
        # Create StorageWriter
        writer = StorageWriter(
            redis=redis_storage,
            duckdb=duckdb_storage,
            parquet=parquet_storage
        )
        
        try:
            yield writer, redis_storage, duckdb_storage, parquet_storage
        finally:
            redis_storage.flush_db()
            duckdb_storage.close()


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Symbol strategy - realistic crypto trading pairs
symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"
])

# Price strategy - positive floats with reasonable precision
price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)

# Volume strategy - positive floats
volume_strategy = st.floats(min_value=0.0, max_value=1000000000.0, allow_nan=False, allow_infinity=False)

# Trades count strategy
trades_count_strategy = st.integers(min_value=0, max_value=1000000)

# Interval strategy for aggregations
interval_strategy = st.sampled_from(["1m", "5m", "15m", "1h", "4h", "1d"])

# RSI strategy (0-100)
rsi_strategy = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)

# MACD strategy
macd_strategy = st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False)

# ATR strategy
atr_strategy = st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False)


# Timestamp strategy - recent timestamps (within last 30 days to avoid retention issues)
def recent_timestamp_strategy():
    """Generate timestamps within the last 30 days as Unix milliseconds."""
    now = datetime.now()
    return st.integers(
        min_value=int((now - timedelta(days=30)).timestamp() * 1000),
        max_value=int(now.timestamp() * 1000)
    )


# Aggregation data strategy
aggregation_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "interval": interval_strategy,
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
    "trades_count": trades_count_strategy,
})

# Indicators data strategy
indicators_data_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "rsi": rsi_strategy,
    "macd": macd_strategy,
    "macd_signal": macd_strategy,
    "sma_20": price_strategy,
    "ema_12": price_strategy,
    "ema_26": price_strategy,
    "bb_upper": price_strategy,
    "bb_lower": price_strategy,
    "atr": atr_strategy,
})

# Alert data strategy
alert_data_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "alert_type": st.sampled_from(["price_spike", "volume_spike", "whale_alert", "volatility"]),
    "severity": st.sampled_from(["low", "medium", "high", "critical"]),
    "message": st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'Z'))),
    "metadata": st.none() | st.fixed_dictionaries({
        "threshold": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        "actual": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    }),
})


# =============================================================================
# Property 7: Multi-tier write completeness
# Feature: three-tier-storage, Property 7: Multi-tier write completeness
# Validates: Requirements 5.1, 5.2, 5.3
# =============================================================================

@skip_if_no_redis
class TestMultiTierWriteCompleteness:
    """Property tests for multi-tier write completeness."""
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_aggregation_write_completeness(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 7: Multi-tier write completeness
        Validates: Requirements 5.1
        
        For any aggregation data written through StorageWriter,
        the data should be present in all 3 tiers (Redis, DuckDB, Parquet).
        """
        with fresh_storage_writer() as (writer, redis, duckdb, parquet):
            # Add timestamp to aggregation
            data = {**aggregation, "timestamp": timestamp}
            symbol = data["symbol"]
            interval = data["interval"]
            
            # Write through StorageWriter
            results = writer.write_aggregation(data)
            
            # Verify all tiers succeeded
            assert results["redis"] is True, "Redis write should succeed"
            assert results["warm"] is True, "Warm path (DuckDB) write should succeed"
            assert results["cold"] is True, "Cold path (Parquet) write should succeed"
            
            # Verify data in Redis
            redis_data = redis.get_aggregation(symbol, interval)
            assert redis_data is not None, "Data should be in Redis"
            assert abs(redis_data["open"] - data["open"]) < 1e-9
            assert abs(redis_data["close"] - data["close"]) < 1e-9
            
            # Verify data in DuckDB
            ts_datetime = datetime.fromtimestamp(timestamp / 1000)
            start = ts_datetime - timedelta(minutes=1)
            end = ts_datetime + timedelta(minutes=1)
            duckdb_data = duckdb.query_candles(symbol, start, end)
            assert len(duckdb_data) == 1, "Data should be in DuckDB"
            assert abs(duckdb_data[0]["open"] - data["open"]) < 1e-9
            assert abs(duckdb_data[0]["close"] - data["close"]) < 1e-9
            
            # Verify data in Parquet
            parquet_data = parquet.read_klines(symbol, start, end)
            assert len(parquet_data) >= 1, "Data should be in Parquet"
    
    @given(indicators=indicators_data_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_indicators_write_completeness(self, indicators, timestamp):
        """
        Feature: three-tier-storage, Property 7: Multi-tier write completeness
        Validates: Requirements 5.2
        
        For any indicators data written through StorageWriter,
        the data should be present in all 3 tiers (Redis, DuckDB, Parquet).
        """
        with fresh_storage_writer() as (writer, redis, duckdb, parquet):
            # Add timestamp to indicators
            data = {**indicators, "timestamp": timestamp}
            symbol = data["symbol"]
            
            # Write through StorageWriter
            results = writer.write_indicators(data)
            
            # Verify all tiers succeeded
            assert results["redis"] is True, "Redis write should succeed"
            assert results["warm"] is True, "Warm path (DuckDB) write should succeed"
            assert results["cold"] is True, "Cold path (Parquet) write should succeed"
            
            # Verify data in Redis
            redis_data = redis.get_indicators(symbol)
            assert redis_data is not None, "Data should be in Redis"
            assert abs(redis_data["rsi"] - data["rsi"]) < 1e-9
            assert abs(redis_data["macd"] - data["macd"]) < 1e-9
            
            # Verify data in DuckDB
            ts_datetime = datetime.fromtimestamp(timestamp / 1000)
            start = ts_datetime - timedelta(minutes=1)
            end = ts_datetime + timedelta(minutes=1)
            duckdb_data = duckdb.query_indicators(symbol, start, end)
            assert len(duckdb_data) == 1, "Data should be in DuckDB"
            assert abs(duckdb_data[0]["rsi"] - data["rsi"]) < 1e-9
            assert abs(duckdb_data[0]["macd"] - data["macd"]) < 1e-9
            
            # Verify data in Parquet
            parquet_data = parquet.read_indicators(symbol, start, end)
            assert len(parquet_data) >= 1, "Data should be in Parquet"
    
    @given(alert=alert_data_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_alert_write_completeness(self, alert, timestamp):
        """
        Feature: three-tier-storage, Property 7: Multi-tier write completeness
        Validates: Requirements 5.3
        
        For any alert data written through StorageWriter,
        the data should be present in all 3 tiers (Redis, DuckDB, Parquet).
        """
        with fresh_storage_writer() as (writer, redis, duckdb, parquet):
            # Add timestamp to alert
            data = {**alert, "timestamp": timestamp}
            symbol = data["symbol"]
            
            # Write through StorageWriter
            results = writer.write_alert(data)
            
            # Verify all tiers succeeded
            assert results["redis"] is True, "Redis write should succeed"
            assert results["warm"] is True, "Warm path (DuckDB) write should succeed"
            assert results["cold"] is True, "Cold path (Parquet) write should succeed"
            
            # Verify data in Redis
            redis_alerts = redis.get_recent_alerts(limit=10)
            assert len(redis_alerts) >= 1, "Alert should be in Redis"
            # Find our alert
            found_in_redis = any(
                a.get("symbol") == symbol and a.get("alert_type") == data["alert_type"]
                for a in redis_alerts
            )
            assert found_in_redis, "Our alert should be in Redis"
            
            # Verify data in DuckDB
            ts_datetime = datetime.fromtimestamp(timestamp / 1000)
            start = ts_datetime - timedelta(minutes=1)
            end = ts_datetime + timedelta(minutes=1)
            duckdb_alerts = duckdb.query_alerts(symbol, start, end)
            assert len(duckdb_alerts) >= 1, "Alert should be in DuckDB"
            assert duckdb_alerts[0]["alert_type"] == data["alert_type"]
            assert duckdb_alerts[0]["severity"] == data["severity"]
            
            # Verify data in Parquet
            parquet_alerts = parquet.read_alerts(symbol, start, end)
            assert len(parquet_alerts) >= 1, "Alert should be in Parquet"



# =============================================================================
# Property 8: Partial failure resilience
# Feature: three-tier-storage, Property 8: Partial failure resilience
# Validates: Requirements 5.4
# =============================================================================

@skip_if_no_redis
class TestPartialFailureResilience:
    """Property tests for partial failure resilience."""
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_aggregation_redis_failure_continues_to_others(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        
        For any write operation where Redis fails, DuckDB and Parquet
        should still receive the data successfully.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create DuckDB storage
            db_path = os.path.join(tmpdir, "test_crypto.duckdb")
            duckdb_storage = DuckDBStorage(db_path=db_path)
            
            # Create Parquet storage
            parquet_path = os.path.join(tmpdir, "parquet")
            parquet_storage = ParquetStorage(base_path=parquet_path)
            
            # Create a mock Redis that always fails
            mock_redis = MagicMock(spec=RedisStorage)
            mock_redis.write_aggregation.side_effect = Exception("Redis connection failed")
            
            # Create StorageWriter with mock Redis
            writer = StorageWriter(
                redis=mock_redis,
                duckdb=duckdb_storage,
                parquet=parquet_storage
            )
            
            try:
                # Add timestamp to aggregation
                data = {**aggregation, "timestamp": timestamp}
                symbol = data["symbol"]
                
                # Write through StorageWriter
                results = writer.write_aggregation(data)
                
                # Verify Redis failed but others succeeded
                assert results["redis"] is False, "Redis should fail"
                assert results["warm"] is True, "Warm path (DuckDB) should succeed despite Redis failure"
                assert results["cold"] is True, "Cold path (Parquet) should succeed despite Redis failure"
                
                # Verify data in DuckDB
                ts_datetime = datetime.fromtimestamp(timestamp / 1000)
                start = ts_datetime - timedelta(minutes=1)
                end = ts_datetime + timedelta(minutes=1)
                duckdb_data = duckdb_storage.query_candles(symbol, start, end)
                assert len(duckdb_data) == 1, "Data should be in DuckDB"
                
                # Verify data in Parquet
                parquet_data = parquet_storage.read_klines(symbol, start, end)
                assert len(parquet_data) >= 1, "Data should be in Parquet"
            finally:
                duckdb_storage.close()
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_aggregation_duckdb_failure_continues_to_others(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        
        For any write operation where DuckDB fails, Redis and Parquet
        should still receive the data successfully.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Redis storage
            redis_storage = RedisStorage(host="localhost", port=6379, db=15)
            redis_storage.flush_db()
            
            # Create Parquet storage
            parquet_path = os.path.join(tmpdir, "parquet")
            parquet_storage = ParquetStorage(base_path=parquet_path)
            
            # Create a mock DuckDB that always fails
            mock_duckdb = MagicMock(spec=DuckDBStorage)
            mock_duckdb.upsert_candle.side_effect = Exception("DuckDB write failed")
            
            # Create StorageWriter with mock DuckDB
            writer = StorageWriter(
                redis=redis_storage,
                duckdb=mock_duckdb,
                parquet=parquet_storage
            )
            
            try:
                # Add timestamp to aggregation
                data = {**aggregation, "timestamp": timestamp}
                symbol = data["symbol"]
                interval = data["interval"]
                
                # Write through StorageWriter
                results = writer.write_aggregation(data)
                
                # Verify DuckDB failed but others succeeded
                assert results["redis"] is True, "Redis should succeed despite DuckDB failure"
                assert results["warm"] is False, "Warm path (DuckDB) should fail"
                assert results["cold"] is True, "Cold path (Parquet) should succeed despite DuckDB failure"
                
                # Verify data in Redis
                redis_data = redis_storage.get_aggregation(symbol, interval)
                assert redis_data is not None, "Data should be in Redis"
                
                # Verify data in Parquet
                ts_datetime = datetime.fromtimestamp(timestamp / 1000)
                start = ts_datetime - timedelta(minutes=1)
                end = ts_datetime + timedelta(minutes=1)
                parquet_data = parquet_storage.read_klines(symbol, start, end)
                assert len(parquet_data) >= 1, "Data should be in Parquet"
            finally:
                redis_storage.flush_db()
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_aggregation_parquet_failure_continues_to_others(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        
        For any write operation where Parquet fails, Redis and DuckDB
        should still receive the data successfully.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Redis storage
            redis_storage = RedisStorage(host="localhost", port=6379, db=15)
            redis_storage.flush_db()
            
            # Create DuckDB storage
            db_path = os.path.join(tmpdir, "test_crypto.duckdb")
            duckdb_storage = DuckDBStorage(db_path=db_path)
            
            # Create a mock Parquet that always fails
            mock_parquet = MagicMock(spec=ParquetStorage)
            mock_parquet.write_klines.side_effect = Exception("Parquet write failed")
            
            # Create StorageWriter with mock Parquet
            writer = StorageWriter(
                redis=redis_storage,
                duckdb=duckdb_storage,
                parquet=mock_parquet
            )
            
            try:
                # Add timestamp to aggregation
                data = {**aggregation, "timestamp": timestamp}
                symbol = data["symbol"]
                interval = data["interval"]
                
                # Write through StorageWriter
                results = writer.write_aggregation(data)
                
                # Verify Parquet failed but others succeeded
                assert results["redis"] is True, "Redis should succeed despite Parquet failure"
                assert results["warm"] is True, "Warm path (DuckDB) should succeed despite Parquet failure"
                assert results["cold"] is False, "Cold path (Parquet) should fail"
                
                # Verify data in Redis
                redis_data = redis_storage.get_aggregation(symbol, interval)
                assert redis_data is not None, "Data should be in Redis"
                
                # Verify data in DuckDB
                ts_datetime = datetime.fromtimestamp(timestamp / 1000)
                start = ts_datetime - timedelta(minutes=1)
                end = ts_datetime + timedelta(minutes=1)
                duckdb_data = duckdb_storage.query_candles(symbol, start, end)
                assert len(duckdb_data) == 1, "Data should be in DuckDB"
            finally:
                redis_storage.flush_db()
                duckdb_storage.close()
    
    @given(indicators=indicators_data_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_indicators_partial_failure(self, indicators, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        
        For any indicators write where one sink fails, the other sinks
        should still receive the data successfully.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Redis storage
            redis_storage = RedisStorage(host="localhost", port=6379, db=15)
            redis_storage.flush_db()
            
            # Create Parquet storage
            parquet_path = os.path.join(tmpdir, "parquet")
            parquet_storage = ParquetStorage(base_path=parquet_path)
            
            # Create a mock DuckDB that always fails
            mock_duckdb = MagicMock(spec=DuckDBStorage)
            mock_duckdb.upsert_indicators.side_effect = Exception("DuckDB write failed")
            
            # Create StorageWriter with mock DuckDB
            writer = StorageWriter(
                redis=redis_storage,
                duckdb=mock_duckdb,
                parquet=parquet_storage
            )
            
            try:
                # Add timestamp to indicators
                data = {**indicators, "timestamp": timestamp}
                symbol = data["symbol"]
                
                # Write through StorageWriter
                results = writer.write_indicators(data)
                
                # Verify DuckDB failed but others succeeded
                assert results["redis"] is True, "Redis should succeed"
                assert results["warm"] is False, "Warm path (DuckDB) should fail"
                assert results["cold"] is True, "Cold path (Parquet) should succeed"
                
                # Verify data in Redis
                redis_data = redis_storage.get_indicators(symbol)
                assert redis_data is not None, "Data should be in Redis"
                
                # Verify data in Parquet
                ts_datetime = datetime.fromtimestamp(timestamp / 1000)
                start = ts_datetime - timedelta(minutes=1)
                end = ts_datetime + timedelta(minutes=1)
                parquet_data = parquet_storage.read_indicators(symbol, start, end)
                assert len(parquet_data) >= 1, "Data should be in Parquet"
            finally:
                redis_storage.flush_db()
    
    @given(alert=alert_data_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_alert_partial_failure(self, alert, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        
        For any alert write where one sink fails, the other sinks
        should still receive the data successfully.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Redis storage
            redis_storage = RedisStorage(host="localhost", port=6379, db=15)
            redis_storage.flush_db()
            
            # Create DuckDB storage
            db_path = os.path.join(tmpdir, "test_crypto.duckdb")
            duckdb_storage = DuckDBStorage(db_path=db_path)
            
            # Create a mock Parquet that always fails
            mock_parquet = MagicMock(spec=ParquetStorage)
            mock_parquet.write_alerts.side_effect = Exception("Parquet write failed")
            
            # Create StorageWriter with mock Parquet
            writer = StorageWriter(
                redis=redis_storage,
                duckdb=duckdb_storage,
                parquet=mock_parquet
            )
            
            try:
                # Add timestamp to alert
                data = {**alert, "timestamp": timestamp}
                symbol = data["symbol"]
                
                # Write through StorageWriter
                results = writer.write_alert(data)
                
                # Verify Parquet failed but others succeeded
                assert results["redis"] is True, "Redis should succeed"
                assert results["warm"] is True, "Warm path (DuckDB) should succeed"
                assert results["cold"] is False, "Cold path (Parquet) should fail"
                
                # Verify data in Redis
                redis_alerts = redis_storage.get_recent_alerts(limit=10)
                assert len(redis_alerts) >= 1, "Alert should be in Redis"
                
                # Verify data in DuckDB
                ts_datetime = datetime.fromtimestamp(timestamp / 1000)
                start = ts_datetime - timedelta(minutes=1)
                end = ts_datetime + timedelta(minutes=1)
                duckdb_alerts = duckdb_storage.query_alerts(symbol, start, end)
                assert len(duckdb_alerts) >= 1, "Alert should be in DuckDB"
            finally:
                redis_storage.flush_db()
                duckdb_storage.close()
