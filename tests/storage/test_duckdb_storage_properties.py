"""
Property-based tests for DuckDBStorage.

Feature: three-tier-storage
Tests correctness properties for DuckDB warm path storage.
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta, date
from hypothesis import given, settings, strategies as st, HealthCheck
from contextlib import contextmanager

from src.storage.duckdb_storage import DuckDBStorage


# =============================================================================
# Helper to create fresh storage for each test
# =============================================================================

@contextmanager
def fresh_duckdb_storage():
    """Create a fresh DuckDBStorage instance for each test example."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_crypto.duckdb")
        storage = DuckDBStorage(db_path=db_path)
        try:
            yield storage
        finally:
            storage.close()


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


# RSI strategy (0-100)
rsi_strategy = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)

# MACD strategy
macd_strategy = st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False)

# ATR strategy
atr_strategy = st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False)

# Volatility strategy
volatility_strategy = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)

# Timestamp strategy - datetime within last 90 days
def recent_timestamp_strategy():
    """Generate timestamps within the last 89 days."""
    now = datetime.now()
    return st.integers(
        min_value=int((now - timedelta(days=89)).timestamp()),
        max_value=int(now.timestamp())
    ).map(lambda ts: datetime.fromtimestamp(ts))

# Date strategy - dates within last 2 years
def recent_date_strategy():
    """Generate dates within the last 729 days."""
    today = date.today()
    return st.integers(
        min_value=0,
        max_value=729
    ).map(lambda days: today - timedelta(days=days))


# Candle strategy
candle_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
    "trades_count": trades_count_strategy,
})

# Indicators strategy
indicators_strategy = st.fixed_dictionaries({
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

# Alert strategy - use simple ASCII text to avoid encoding issues
alert_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "alert_type": st.sampled_from(["price_spike", "volume_spike", "whale_alert", "volatility"]),
    "severity": st.sampled_from(["low", "medium", "high", "critical"]),
    "message": st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'Z'))),
    "metadata": st.none() | st.fixed_dictionaries({
        "threshold": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        "actual": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    }),
})

# Daily stats strategy
daily_stats_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "volatility": volatility_strategy,
})



# =============================================================================
# Property 3: DuckDB insert/query round-trip consistency
# Feature: three-tier-storage, Property 3: DuckDB insert/query round-trip consistency
# Validates: Requirements 2.1, 2.2, 2.3, 2.4
# =============================================================================

class TestDuckDBRoundTrip:
    """Property tests for DuckDB round-trip consistency."""
    
    @given(candle=candle_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_candle_round_trip(self, candle, timestamp):
        """
        Feature: three-tier-storage, Property 3: DuckDB insert/query round-trip consistency
        Validates: Requirements 2.1
        
        For any valid candle record, inserting into DuckDB and querying back
        should return equivalent values.
        """
        with fresh_duckdb_storage() as storage:
            # Add timestamp to candle
            candle_with_ts = {**candle, "timestamp": timestamp}
            
            # Write
            storage.upsert_candle(candle_with_ts)
            
            # Read - query with time range that includes the timestamp
            start = timestamp - timedelta(minutes=1)
            end = timestamp + timedelta(minutes=1)
            results = storage.query_candles(candle["symbol"], start, end)
            
            # Verify round-trip
            assert len(results) == 1
            result = results[0]
            assert result["symbol"] == candle["symbol"]
            assert abs(result["open"] - candle["open"]) < 1e-9
            assert abs(result["high"] - candle["high"]) < 1e-9
            assert abs(result["low"] - candle["low"]) < 1e-9
            assert abs(result["close"] - candle["close"]) < 1e-9
            assert abs(result["volume"] - candle["volume"]) < 1e-9
            assert abs(result["quote_volume"] - candle["quote_volume"]) < 1e-9
            assert result["trades_count"] == candle["trades_count"]
    
    @given(indicators=indicators_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_indicators_round_trip(self, indicators, timestamp):
        """
        Feature: three-tier-storage, Property 3: DuckDB insert/query round-trip consistency
        Validates: Requirements 2.2
        
        For any valid indicators record, inserting into DuckDB and querying back
        should return equivalent values.
        """
        with fresh_duckdb_storage() as storage:
            # Add timestamp to indicators
            indicators_with_ts = {**indicators, "timestamp": timestamp}
            
            # Write
            storage.upsert_indicators(indicators_with_ts)
            
            # Read
            start = timestamp - timedelta(minutes=1)
            end = timestamp + timedelta(minutes=1)
            results = storage.query_indicators(indicators["symbol"], start, end)
            
            # Verify round-trip
            assert len(results) == 1
            result = results[0]
            assert result["symbol"] == indicators["symbol"]
            assert abs(result["rsi"] - indicators["rsi"]) < 1e-9
            assert abs(result["macd"] - indicators["macd"]) < 1e-9
            assert abs(result["macd_signal"] - indicators["macd_signal"]) < 1e-9
            assert abs(result["sma_20"] - indicators["sma_20"]) < 1e-9
            assert abs(result["ema_12"] - indicators["ema_12"]) < 1e-9
            assert abs(result["ema_26"] - indicators["ema_26"]) < 1e-9
            assert abs(result["bb_upper"] - indicators["bb_upper"]) < 1e-9
            assert abs(result["bb_lower"] - indicators["bb_lower"]) < 1e-9
            assert abs(result["atr"] - indicators["atr"]) < 1e-9
    
    @given(alert=alert_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=100, deadline=None)
    def test_alert_round_trip(self, alert, timestamp):
        """
        Feature: three-tier-storage, Property 3: DuckDB insert/query round-trip consistency
        Validates: Requirements 2.3
        
        For any valid alert record, inserting into DuckDB and querying back
        should return equivalent values.
        """
        with fresh_duckdb_storage() as storage:
            # Add timestamp to alert
            alert_with_ts = {**alert, "timestamp": timestamp}
            
            # Write
            storage.insert_alert(alert_with_ts)
            
            # Read
            start = timestamp - timedelta(minutes=1)
            end = timestamp + timedelta(minutes=1)
            results = storage.query_alerts(alert["symbol"], start, end)
            
            # Verify round-trip - should have exactly one alert
            assert len(results) == 1
            result = results[0]
            assert result["symbol"] == alert["symbol"]
            assert result["alert_type"] == alert["alert_type"]
            assert result["severity"] == alert["severity"]
            assert result["message"] == alert["message"]
    
    @given(stats=daily_stats_strategy, day=recent_date_strategy())
    @settings(max_examples=100, deadline=None)
    def test_daily_stats_round_trip(self, stats, day):
        """
        Feature: three-tier-storage, Property 3: DuckDB insert/query round-trip consistency
        Validates: Requirements 2.4
        
        For any valid daily stats record, inserting into DuckDB and querying back
        should return equivalent values.
        """
        with fresh_duckdb_storage() as storage:
            # Add date to stats
            stats_with_date = {**stats, "date": day}
            
            # Write
            storage.upsert_daily_stats(stats_with_date)
            
            # Read - convert date to datetime for query
            start = datetime.combine(day, datetime.min.time())
            end = datetime.combine(day, datetime.max.time())
            results = storage.query_daily_stats(stats["symbol"], start, end)
            
            # Verify round-trip
            assert len(results) == 1
            result = results[0]
            assert result["symbol"] == stats["symbol"]
            assert abs(result["open"] - stats["open"]) < 1e-9
            assert abs(result["high"] - stats["high"]) < 1e-9
            assert abs(result["low"] - stats["low"]) < 1e-9
            assert abs(result["close"] - stats["close"]) < 1e-9
            assert abs(result["volume"] - stats["volume"]) < 1e-9
            assert abs(result["volatility"] - stats["volatility"]) < 1e-9



# =============================================================================
# Property 9: DuckDB upsert idempotence
# Feature: three-tier-storage, Property 9: DuckDB upsert idempotence
# Validates: Requirements 5.5
# =============================================================================

class TestDuckDBUpsertIdempotence:
    """Property tests for DuckDB upsert idempotence."""
    
    @given(
        candle1=candle_strategy,
        candle2=candle_strategy,
        timestamp=recent_timestamp_strategy()
    )
    @settings(max_examples=100, deadline=None)
    def test_candle_upsert_idempotence(self, candle1, candle2, timestamp):
        """
        Feature: three-tier-storage, Property 9: DuckDB upsert idempotence
        Validates: Requirements 5.5
        
        For any record with same (symbol, timestamp) key, writing twice
        should result in exactly one record with the latest values.
        """
        with fresh_duckdb_storage() as storage:
            symbol = candle1["symbol"]  # Use same symbol for both
            
            # First write
            candle1_with_ts = {**candle1, "timestamp": timestamp}
            storage.upsert_candle(candle1_with_ts)
            
            # Second write with same key but different values
            candle2_with_ts = {**candle2, "symbol": symbol, "timestamp": timestamp}
            storage.upsert_candle(candle2_with_ts)
            
            # Query
            start = timestamp - timedelta(minutes=1)
            end = timestamp + timedelta(minutes=1)
            results = storage.query_candles(symbol, start, end)
            
            # Verify exactly one record with latest values
            assert len(results) == 1
            result = results[0]
            assert abs(result["open"] - candle2["open"]) < 1e-9
            assert abs(result["high"] - candle2["high"]) < 1e-9
            assert abs(result["low"] - candle2["low"]) < 1e-9
            assert abs(result["close"] - candle2["close"]) < 1e-9
    
    @given(
        indicators1=indicators_strategy,
        indicators2=indicators_strategy,
        timestamp=recent_timestamp_strategy()
    )
    @settings(max_examples=100, deadline=None)
    def test_indicators_upsert_idempotence(self, indicators1, indicators2, timestamp):
        """
        Feature: three-tier-storage, Property 9: DuckDB upsert idempotence
        Validates: Requirements 5.5
        
        For any indicators with same (symbol, timestamp) key, writing twice
        should result in exactly one record with the latest values.
        """
        with fresh_duckdb_storage() as storage:
            symbol = indicators1["symbol"]  # Use same symbol for both
            
            # First write
            ind1_with_ts = {**indicators1, "timestamp": timestamp}
            storage.upsert_indicators(ind1_with_ts)
            
            # Second write with same key but different values
            ind2_with_ts = {**indicators2, "symbol": symbol, "timestamp": timestamp}
            storage.upsert_indicators(ind2_with_ts)
            
            # Query
            start = timestamp - timedelta(minutes=1)
            end = timestamp + timedelta(minutes=1)
            results = storage.query_indicators(symbol, start, end)
            
            # Verify exactly one record with latest values
            assert len(results) == 1
            result = results[0]
            assert abs(result["rsi"] - indicators2["rsi"]) < 1e-9
            assert abs(result["macd"] - indicators2["macd"]) < 1e-9
    
    @given(
        stats1=daily_stats_strategy,
        stats2=daily_stats_strategy,
        day=recent_date_strategy()
    )
    @settings(max_examples=100, deadline=None)
    def test_daily_stats_upsert_idempotence(self, stats1, stats2, day):
        """
        Feature: three-tier-storage, Property 9: DuckDB upsert idempotence
        Validates: Requirements 5.5
        
        For any daily stats with same (symbol, date) key, writing twice
        should result in exactly one record with the latest values.
        """
        with fresh_duckdb_storage() as storage:
            symbol = stats1["symbol"]  # Use same symbol for both
            
            # First write
            stats1_with_date = {**stats1, "date": day}
            storage.upsert_daily_stats(stats1_with_date)
            
            # Second write with same key but different values
            stats2_with_date = {**stats2, "symbol": symbol, "date": day}
            storage.upsert_daily_stats(stats2_with_date)
            
            # Query
            start = datetime.combine(day, datetime.min.time())
            end = datetime.combine(day, datetime.max.time())
            results = storage.query_daily_stats(symbol, start, end)
            
            # Verify exactly one record with latest values
            assert len(results) == 1
            result = results[0]
            assert abs(result["open"] - stats2["open"]) < 1e-9
            assert abs(result["close"] - stats2["close"]) < 1e-9
            assert abs(result["volume"] - stats2["volume"]) < 1e-9



# =============================================================================
# Property 4: DuckDB retention cleanup correctness
# Feature: three-tier-storage, Property 4: DuckDB retention cleanup correctness
# Validates: Requirements 2.5
# =============================================================================

class TestDuckDBRetentionCleanup:
    """Property tests for DuckDB retention cleanup correctness."""
    
    @given(
        symbol=symbol_strategy,
        old_days_offset=st.integers(min_value=91, max_value=180),
        new_days_offset=st.integers(min_value=1, max_value=89)
    )
    @settings(max_examples=100, deadline=None)
    def test_trades_retention_cleanup(self, symbol, old_days_offset, new_days_offset):
        """
        Feature: three-tier-storage, Property 4: DuckDB retention cleanup correctness
        Validates: Requirements 2.5
        
        For any data older than 90 days for trades_1m, cleanup should delete
        those records while preserving newer data.
        """
        with fresh_duckdb_storage() as storage:
            now = datetime.now()
            old_timestamp = now - timedelta(days=old_days_offset)
            new_timestamp = now - timedelta(days=new_days_offset)
            
            # Insert old record (should be deleted)
            old_candle = {
                "timestamp": old_timestamp,
                "symbol": symbol,
                "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
                "volume": 1000.0, "quote_volume": 100000.0, "trades_count": 100
            }
            storage.upsert_candle(old_candle)
            
            # Insert new record (should be preserved)
            new_candle = {
                "timestamp": new_timestamp,
                "symbol": symbol,
                "open": 200.0, "high": 210.0, "low": 190.0, "close": 205.0,
                "volume": 2000.0, "quote_volume": 200000.0, "trades_count": 200
            }
            storage.upsert_candle(new_candle)
            
            # Run cleanup
            storage.cleanup_old_data()
            
            # Verify old record is deleted
            old_results = storage.query_candles(
                symbol, 
                old_timestamp - timedelta(minutes=1),
                old_timestamp + timedelta(minutes=1)
            )
            assert len(old_results) == 0, "Old record should be deleted"
            
            # Verify new record is preserved
            new_results = storage.query_candles(
                symbol,
                new_timestamp - timedelta(minutes=1),
                new_timestamp + timedelta(minutes=1)
            )
            assert len(new_results) == 1, "New record should be preserved"
    
    @given(
        symbol=symbol_strategy,
        old_days_offset=st.integers(min_value=91, max_value=180),
        new_days_offset=st.integers(min_value=1, max_value=89)
    )
    @settings(max_examples=100, deadline=None)
    def test_indicators_retention_cleanup(self, symbol, old_days_offset, new_days_offset):
        """
        Feature: three-tier-storage, Property 4: DuckDB retention cleanup correctness
        Validates: Requirements 2.5
        
        For any data older than 90 days for indicators, cleanup should delete
        those records while preserving newer data.
        """
        with fresh_duckdb_storage() as storage:
            now = datetime.now()
            old_timestamp = now - timedelta(days=old_days_offset)
            new_timestamp = now - timedelta(days=new_days_offset)
            
            # Insert old record
            old_indicators = {
                "timestamp": old_timestamp,
                "symbol": symbol,
                "rsi": 50.0, "macd": 100.0, "macd_signal": 90.0,
                "sma_20": 1000.0, "ema_12": 1010.0, "ema_26": 990.0,
                "bb_upper": 1100.0, "bb_lower": 900.0, "atr": 50.0
            }
            storage.upsert_indicators(old_indicators)
            
            # Insert new record
            new_indicators = {
                "timestamp": new_timestamp,
                "symbol": symbol,
                "rsi": 60.0, "macd": 110.0, "macd_signal": 100.0,
                "sma_20": 1100.0, "ema_12": 1110.0, "ema_26": 1090.0,
                "bb_upper": 1200.0, "bb_lower": 1000.0, "atr": 60.0
            }
            storage.upsert_indicators(new_indicators)
            
            # Run cleanup
            storage.cleanup_old_data()
            
            # Verify old record is deleted
            old_results = storage.query_indicators(
                symbol,
                old_timestamp - timedelta(minutes=1),
                old_timestamp + timedelta(minutes=1)
            )
            assert len(old_results) == 0, "Old indicators should be deleted"
            
            # Verify new record is preserved
            new_results = storage.query_indicators(
                symbol,
                new_timestamp - timedelta(minutes=1),
                new_timestamp + timedelta(minutes=1)
            )
            assert len(new_results) == 1, "New indicators should be preserved"
    
    @given(
        symbol=symbol_strategy,
        old_days_offset=st.integers(min_value=181, max_value=365),
        new_days_offset=st.integers(min_value=1, max_value=179)
    )
    @settings(max_examples=100, deadline=None)
    def test_alerts_retention_cleanup(self, symbol, old_days_offset, new_days_offset):
        """
        Feature: three-tier-storage, Property 4: DuckDB retention cleanup correctness
        Validates: Requirements 2.5
        
        For any data older than 180 days for alerts, cleanup should delete
        those records while preserving newer data.
        """
        with fresh_duckdb_storage() as storage:
            now = datetime.now()
            old_timestamp = now - timedelta(days=old_days_offset)
            new_timestamp = now - timedelta(days=new_days_offset)
            
            # Insert old alert
            old_alert = {
                "timestamp": old_timestamp,
                "symbol": symbol,
                "alert_type": "price_spike",
                "severity": "high",
                "message": "Old alert",
                "metadata": None
            }
            storage.insert_alert(old_alert)
            
            # Insert new alert
            new_alert = {
                "timestamp": new_timestamp,
                "symbol": symbol,
                "alert_type": "volume_spike",
                "severity": "medium",
                "message": "New alert",
                "metadata": None
            }
            storage.insert_alert(new_alert)
            
            # Run cleanup
            storage.cleanup_old_data()
            
            # Verify old alert is deleted
            old_results = storage.query_alerts(
                symbol,
                old_timestamp - timedelta(minutes=1),
                old_timestamp + timedelta(minutes=1)
            )
            assert len(old_results) == 0, "Old alert should be deleted"
            
            # Verify new alert is preserved
            new_results = storage.query_alerts(
                symbol,
                new_timestamp - timedelta(minutes=1),
                new_timestamp + timedelta(minutes=1)
            )
            assert len(new_results) == 1, "New alert should be preserved"
    
    @given(
        symbol=symbol_strategy,
        old_days_offset=st.integers(min_value=731, max_value=1000),
        new_days_offset=st.integers(min_value=1, max_value=729)
    )
    @settings(max_examples=100, deadline=None)
    def test_daily_stats_retention_cleanup(self, symbol, old_days_offset, new_days_offset):
        """
        Feature: three-tier-storage, Property 4: DuckDB retention cleanup correctness
        Validates: Requirements 2.5
        
        For any data older than 2 years (730 days) for daily_stats, cleanup should
        delete those records while preserving newer data.
        """
        with fresh_duckdb_storage() as storage:
            today = date.today()
            old_date = today - timedelta(days=old_days_offset)
            new_date = today - timedelta(days=new_days_offset)
            
            # Insert old stats
            old_stats = {
                "date": old_date,
                "symbol": symbol,
                "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
                "volume": 1000000.0, "volatility": 0.05
            }
            storage.upsert_daily_stats(old_stats)
            
            # Insert new stats
            new_stats = {
                "date": new_date,
                "symbol": symbol,
                "open": 200.0, "high": 210.0, "low": 190.0, "close": 205.0,
                "volume": 2000000.0, "volatility": 0.06
            }
            storage.upsert_daily_stats(new_stats)
            
            # Run cleanup
            storage.cleanup_old_data()
            
            # Verify old stats is deleted
            old_results = storage.query_daily_stats(
                symbol,
                datetime.combine(old_date, datetime.min.time()),
                datetime.combine(old_date, datetime.max.time())
            )
            assert len(old_results) == 0, "Old daily stats should be deleted"
            
            # Verify new stats is preserved
            new_results = storage.query_daily_stats(
                symbol,
                datetime.combine(new_date, datetime.min.time()),
                datetime.combine(new_date, datetime.max.time())
            )
            assert len(new_results) == 1, "New daily stats should be preserved"
