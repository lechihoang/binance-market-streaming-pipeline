"""
Property-based tests for ParquetStorage.

Feature: three-tier-storage
Tests correctness properties for Parquet cold path storage.
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from hypothesis import given, settings, strategies as st
from contextlib import contextmanager

from src.storage.parquet_storage import ParquetStorage


# =============================================================================
# Helper to create fresh storage for each test
# =============================================================================

@contextmanager
def fresh_parquet_storage():
    """Create a fresh ParquetStorage instance for each test example."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(base_path=tmpdir)
        yield storage


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Symbol strategy - realistic crypto trading pairs
symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"
])

# Data type strategy
data_type_strategy = st.sampled_from(["trades", "klines_1m", "indicators", "alerts"])

# Timestamp strategy - datetime within reasonable range
timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31)
)

# Price strategy - positive floats with reasonable precision
price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)

# Volume strategy - positive floats
volume_strategy = st.floats(min_value=0.0, max_value=1000000000.0, allow_nan=False, allow_infinity=False)

# RSI strategy (0-100)
rsi_strategy = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)

# MACD strategy
macd_strategy = st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False)


# =============================================================================
# Property 5: Parquet partition path correctness
# Feature: three-tier-storage, Property 5: Parquet partition path correctness
# Validates: Requirements 3.1, 3.2, 3.3, 3.4
# =============================================================================

class TestParquetPartitionPaths:
    """Property tests for Parquet partition path correctness."""
    
    @given(
        data_type=data_type_strategy,
        timestamp=timestamp_strategy,
        symbol=symbol_strategy
    )
    @settings(max_examples=100, deadline=None)
    def test_partition_path_contains_correct_components(self, data_type, timestamp, symbol):
        """
        Feature: three-tier-storage, Property 5: Parquet partition path correctness
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        
        For any data with timestamp, the Parquet file path should contain correct
        year, month, day, and symbol partitions matching the data's timestamp.
        """
        with fresh_parquet_storage() as storage:
            path = storage._get_partition_path(data_type, timestamp, symbol)
            
            # Verify path contains correct year
            expected_year = f"year={timestamp.year}"
            assert expected_year in path, f"Path should contain {expected_year}"
            
            # Verify path contains correct month (zero-padded)
            expected_month = f"month={str(timestamp.month).zfill(2)}"
            assert expected_month in path, f"Path should contain {expected_month}"
            
            # Verify path contains correct day (zero-padded)
            expected_day = f"day={str(timestamp.day).zfill(2)}"
            assert expected_day in path, f"Path should contain {expected_day}"
            
            # Verify path contains correct symbol
            expected_symbol = f"symbol={symbol}"
            assert expected_symbol in path, f"Path should contain {expected_symbol}"
            
            # Verify path contains data type
            assert data_type in path, f"Path should contain data type {data_type}"
    
    @given(
        data_type=data_type_strategy,
        timestamp=timestamp_strategy,
        symbol=symbol_strategy
    )
    @settings(max_examples=100, deadline=None)
    def test_partition_path_order(self, data_type, timestamp, symbol):
        """
        Feature: three-tier-storage, Property 5: Parquet partition path correctness
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        
        Partition path components should be in correct order:
        base_path/data_type/year/month/day/symbol
        """
        with fresh_parquet_storage() as storage:
            path = storage._get_partition_path(data_type, timestamp, symbol)
            
            # Find positions of each component
            data_type_pos = path.find(data_type)
            year_pos = path.find(f"year={timestamp.year}")
            month_pos = path.find(f"month={str(timestamp.month).zfill(2)}")
            day_pos = path.find(f"day={str(timestamp.day).zfill(2)}")
            symbol_pos = path.find(f"symbol={symbol}")
            
            # Verify order: data_type < year < month < day < symbol
            assert data_type_pos < year_pos, "data_type should come before year"
            assert year_pos < month_pos, "year should come before month"
            assert month_pos < day_pos, "month should come before day"
            assert day_pos < symbol_pos, "day should come before symbol"
