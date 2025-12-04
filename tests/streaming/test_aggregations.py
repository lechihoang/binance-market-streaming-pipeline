"""
Unit tests for aggregation logic.

Tests OHLCV calculation, VWAP, price change percentage, and buy-sell ratio
with sample trade data without requiring Spark.
"""

import pytest
from typing import List, Dict, Any


def calculate_ohlcv(trades: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Calculate OHLCV from list of trades.
    
    Args:
        trades: List of trade dictionaries with 'price' and 'quantity'
        
    Returns:
        Dictionary with OHLCV values
    """
    if not trades:
        return {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": 0.0
        }
    
    prices = [t["price"] for t in trades]
    quantities = [t["quantity"] for t in trades]
    
    return {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(quantities)
    }


def calculate_vwap(trades: List[Dict[str, Any]]) -> float:
    """
    Calculate Volume Weighted Average Price.
    
    Args:
        trades: List of trade dictionaries with 'price' and 'quantity'
        
    Returns:
        VWAP value
    """
    if not trades:
        return 0.0
    
    total_value = sum(t["price"] * t["quantity"] for t in trades)
    total_volume = sum(t["quantity"] for t in trades)
    
    if total_volume == 0:
        return 0.0
    
    return total_value / total_volume


def calculate_price_change_pct(open_price: float, close_price: float) -> float:
    """
    Calculate price change percentage.
    
    Args:
        open_price: Opening price
        close_price: Closing price
        
    Returns:
        Price change percentage
    """
    if open_price == 0:
        return 0.0
    
    return ((close_price - open_price) / open_price) * 100


def calculate_buy_sell_ratio(buy_count: int, sell_count: int) -> float:
    """
    Calculate buy-sell ratio.
    
    Args:
        buy_count: Number of buy orders
        sell_count: Number of sell orders
        
    Returns:
        Buy-sell ratio (or inf if sell_count is 0)
    """
    if sell_count == 0:
        return float('inf')
    
    return buy_count / sell_count


class TestOHLCV:
    """Test OHLCV calculation."""
    
    def test_ohlcv_single_trade(self):
        """Test OHLCV calculation with single trade."""
        trades = [{"price": 100.0, "quantity": 1.0}]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 100.0
        assert ohlcv["low"] == 100.0
        assert ohlcv["close"] == 100.0
        assert ohlcv["volume"] == 1.0
    
    def test_ohlcv_multiple_trades(self):
        """Test OHLCV calculation with multiple trades."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 105.0, "quantity": 2.0},
            {"price": 95.0, "quantity": 1.5},
            {"price": 102.0, "quantity": 1.0},
        ]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 105.0
        assert ohlcv["low"] == 95.0
        assert ohlcv["close"] == 102.0
        assert ohlcv["volume"] == 5.5
    
    def test_ohlcv_empty_trades(self):
        """Test OHLCV calculation with empty trades."""
        trades = []
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] is None
        assert ohlcv["high"] is None
        assert ohlcv["low"] is None
        assert ohlcv["close"] is None
        assert ohlcv["volume"] == 0.0
    
    def test_ohlcv_constant_price(self):
        """Test OHLCV with constant price."""
        trades = [
            {"price": 50.0, "quantity": 1.0},
            {"price": 50.0, "quantity": 2.0},
            {"price": 50.0, "quantity": 1.5},
        ]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 50.0
        assert ohlcv["high"] == 50.0
        assert ohlcv["low"] == 50.0
        assert ohlcv["close"] == 50.0
        assert ohlcv["volume"] == 4.5
    
    def test_ohlcv_descending_prices(self):
        """Test OHLCV with descending prices."""
        trades = [
            {"price": 110.0, "quantity": 1.0},
            {"price": 105.0, "quantity": 1.0},
            {"price": 100.0, "quantity": 1.0},
            {"price": 95.0, "quantity": 1.0},
        ]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 110.0
        assert ohlcv["high"] == 110.0
        assert ohlcv["low"] == 95.0
        assert ohlcv["close"] == 95.0


class TestVWAP:
    """Test VWAP calculation."""
    
    def test_vwap_calculation(self):
        """Test VWAP calculation."""
        trades = [
            {"price": 100.0, "quantity": 1.0},  # 100
            {"price": 110.0, "quantity": 2.0},  # 220
        ]
        # VWAP = (100 + 220) / (1 + 2) = 320 / 3 = 106.67
        vwap = calculate_vwap(trades)
        assert abs(vwap - 106.67) < 0.01
    
    def test_vwap_single_trade(self):
        """Test VWAP with single trade."""
        trades = [{"price": 50.0, "quantity": 2.0}]
        vwap = calculate_vwap(trades)
        assert vwap == 50.0
    
    def test_vwap_equal_quantities(self):
        """Test VWAP with equal quantities (should equal simple average)."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 200.0, "quantity": 1.0},
        ]
        vwap = calculate_vwap(trades)
        assert vwap == 150.0
    
    def test_vwap_empty_trades(self):
        """Test VWAP with empty trades."""
        trades = []
        vwap = calculate_vwap(trades)
        assert vwap == 0.0
    
    def test_vwap_zero_volume(self):
        """Test VWAP with zero total volume."""
        trades = [
            {"price": 100.0, "quantity": 0.0},
            {"price": 200.0, "quantity": 0.0},
        ]
        vwap = calculate_vwap(trades)
        assert vwap == 0.0
    
    def test_vwap_weighted_heavily(self):
        """Test VWAP with heavily weighted trade."""
        trades = [
            {"price": 100.0, "quantity": 1.0},   # 100
            {"price": 200.0, "quantity": 99.0},  # 19800
        ]
        # VWAP = 19900 / 100 = 199.0 (heavily weighted toward 200)
        vwap = calculate_vwap(trades)
        assert abs(vwap - 199.0) < 0.01


class TestPriceChange:
    """Test price change percentage calculation."""
    
    def test_price_change_positive(self):
        """Test positive price change."""
        pct = calculate_price_change_pct(100.0, 110.0)
        assert pct == 10.0
    
    def test_price_change_negative(self):
        """Test negative price change."""
        pct = calculate_price_change_pct(100.0, 90.0)
        assert pct == -10.0
    
    def test_price_change_zero(self):
        """Test zero price change."""
        pct = calculate_price_change_pct(100.0, 100.0)
        assert pct == 0.0
    
    def test_price_change_small(self):
        """Test small price change."""
        pct = calculate_price_change_pct(100.0, 100.5)
        assert abs(pct - 0.5) < 0.01
    
    def test_price_change_large(self):
        """Test large price change."""
        pct = calculate_price_change_pct(100.0, 200.0)
        assert pct == 100.0
    
    def test_price_change_zero_open(self):
        """Test price change with zero open price."""
        pct = calculate_price_change_pct(0.0, 100.0)
        assert pct == 0.0
    
    def test_price_change_fractional(self):
        """Test price change with fractional values."""
        pct = calculate_price_change_pct(50.25, 51.50)
        expected = ((51.50 - 50.25) / 50.25) * 100
        assert abs(pct - expected) < 0.01


class TestBuySellRatio:
    """Test buy-sell ratio calculation."""
    
    def test_buy_sell_ratio_equal(self):
        """Test buy-sell ratio with equal counts."""
        ratio = calculate_buy_sell_ratio(10, 10)
        assert ratio == 1.0
    
    def test_buy_sell_ratio_more_buys(self):
        """Test buy-sell ratio with more buys."""
        ratio = calculate_buy_sell_ratio(20, 10)
        assert ratio == 2.0
    
    def test_buy_sell_ratio_more_sells(self):
        """Test buy-sell ratio with more sells."""
        ratio = calculate_buy_sell_ratio(10, 20)
        assert ratio == 0.5
    
    def test_buy_sell_ratio_zero_sells(self):
        """Test buy-sell ratio with zero sells."""
        ratio = calculate_buy_sell_ratio(10, 0)
        assert ratio == float('inf')
    
    def test_buy_sell_ratio_zero_buys(self):
        """Test buy-sell ratio with zero buys."""
        ratio = calculate_buy_sell_ratio(0, 10)
        assert ratio == 0.0
    
    def test_buy_sell_ratio_both_zero(self):
        """Test buy-sell ratio with both zero."""
        ratio = calculate_buy_sell_ratio(0, 0)
        assert ratio == float('inf')
    
    def test_buy_sell_ratio_fractional(self):
        """Test buy-sell ratio with fractional result."""
        ratio = calculate_buy_sell_ratio(7, 3)
        assert abs(ratio - 2.333) < 0.01


class TestIntegratedAggregation:
    """Test integrated aggregation workflow."""
    
    def test_full_aggregation_workflow(self):
        """Test complete aggregation workflow with all metrics."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 105.0, "quantity": 2.0},
            {"price": 95.0, "quantity": 1.5},
            {"price": 102.0, "quantity": 1.0},
        ]
        
        # Calculate OHLCV
        ohlcv = calculate_ohlcv(trades)
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 105.0
        assert ohlcv["low"] == 95.0
        assert ohlcv["close"] == 102.0
        assert ohlcv["volume"] == 5.5
        
        # Calculate VWAP
        vwap = calculate_vwap(trades)
        # (100*1 + 105*2 + 95*1.5 + 102*1) / 5.5 = (100 + 210 + 142.5 + 102) / 5.5 = 554.5 / 5.5 = 100.818
        assert abs(vwap - 100.818) < 0.01
        
        # Calculate price change
        price_change = calculate_price_change_pct(ohlcv["open"], ohlcv["close"])
        # (102 - 100) / 100 * 100 = 2%
        assert price_change == 2.0
    
    def test_aggregation_with_extreme_values(self):
        """Test aggregation with extreme price movements."""
        trades = [
            {"price": 1000.0, "quantity": 0.1},
            {"price": 500.0, "quantity": 0.2},
            {"price": 2000.0, "quantity": 0.05},
        ]
        
        ohlcv = calculate_ohlcv(trades)
        assert ohlcv["open"] == 1000.0
        assert ohlcv["high"] == 2000.0
        assert ohlcv["low"] == 500.0
        assert ohlcv["close"] == 2000.0
        assert abs(ohlcv["volume"] - 0.35) < 0.01
        
        # Price change: (2000 - 1000) / 1000 * 100 = 100%
        price_change = calculate_price_change_pct(ohlcv["open"], ohlcv["close"])
        assert price_change == 100.0
