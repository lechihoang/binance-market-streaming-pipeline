"""
Unit tests for anomaly detection logic.

Tests whale alert, volume spike, price spike, RSI extreme, BB breakout,
and MACD crossover detection with known thresholds and scenarios.
"""

import pytest


# Helper functions that extract the detection logic
def is_whale_alert(price: float, quantity: float, threshold: float = 100000.0) -> bool:
    """
    Check if trade qualifies as whale alert.
    
    Args:
        price: Trade price
        quantity: Trade quantity
        threshold: Whale alert threshold in USD (default $100k)
        
    Returns:
        True if trade value exceeds threshold
    """
    value = price * quantity
    return value > threshold


def is_volume_spike(current_volume: float, historical_volumes: list, multiplier: float = 3.0) -> bool:
    """
    Check if current volume is a spike.
    
    Args:
        current_volume: Current period volume
        historical_volumes: List of historical volumes (last 20 periods)
        multiplier: Spike threshold multiplier (default 3x)
        
    Returns:
        True if current volume exceeds multiplier * average
    """
    if not historical_volumes:
        return False
    
    avg_volume = sum(historical_volumes) / len(historical_volumes)
    return current_volume > (avg_volume * multiplier)


def is_price_spike(open_price: float, close_price: float, threshold_pct: float = 2.0) -> bool:
    """
    Check if price change is a spike.
    
    Args:
        open_price: Opening price
        close_price: Closing price
        threshold_pct: Spike threshold in percentage (default 2%)
        
    Returns:
        True if absolute price change exceeds threshold
    """
    if open_price == 0:
        return False
    
    price_change_pct = abs(((close_price - open_price) / open_price) * 100)
    return price_change_pct > threshold_pct


def is_rsi_extreme(rsi: float, overbought: float = 70.0, oversold: float = 30.0) -> bool:
    """
    Check if RSI is in extreme territory.
    
    Args:
        rsi: RSI value (0-100)
        overbought: Overbought threshold (default 70)
        oversold: Oversold threshold (default 30)
        
    Returns:
        True if RSI is overbought or oversold
    """
    return rsi > overbought or rsi < oversold


def is_bb_breakout(price: float, bb_upper: float, bb_lower: float) -> bool:
    """
    Check if price breaks out of Bollinger Bands.
    
    Args:
        price: Current price
        bb_upper: Upper Bollinger Band
        bb_lower: Lower Bollinger Band
        
    Returns:
        True if price is outside bands
    """
    return price > bb_upper or price < bb_lower


def is_macd_crossover(prev_macd: float, prev_signal: float, 
                      curr_macd: float, curr_signal: float) -> bool:
    """
    Check if MACD crosses signal line.
    
    Args:
        prev_macd: Previous MACD value
        prev_signal: Previous signal line value
        curr_macd: Current MACD value
        curr_signal: Current signal line value
        
    Returns:
        True if crossover occurred
    """
    # Bullish crossover: MACD crosses above signal
    bullish = (prev_macd <= prev_signal) and (curr_macd > curr_signal)
    
    # Bearish crossover: MACD crosses below signal
    bearish = (prev_macd >= prev_signal) and (curr_macd < curr_signal)
    
    return bullish or bearish


class TestWhaleAlert:
    """Test whale alert detection logic."""
    
    def test_whale_alert_at_exact_threshold(self):
        """Test whale alert at exact $100k threshold."""
        # Exactly $100k should NOT trigger (> not >=)
        assert is_whale_alert(50000.0, 2.0) == False
        
        # Just above $100k should trigger
        assert is_whale_alert(50000.0, 2.001) == True
    
    def test_whale_alert_above_threshold(self):
        """Test whale alert above threshold."""
        assert is_whale_alert(50000.0, 3.0) == True  # $150k
        assert is_whale_alert(100000.0, 2.0) == True  # $200k
    
    def test_whale_alert_below_threshold(self):
        """Test whale alert below threshold."""
        assert is_whale_alert(50000.0, 1.0) == False  # $50k
        assert is_whale_alert(10000.0, 5.0) == False  # $50k
    
    def test_whale_alert_edge_cases(self):
        """Test whale alert edge cases."""
        # Very large price, small quantity
        assert is_whale_alert(1000000.0, 0.2) == True  # $200k
        
        # Small price, very large quantity
        assert is_whale_alert(0.01, 20000000.0) == True  # $200k


class TestVolumeSpike:
    """Test volume spike detection logic."""
    
    def test_volume_spike_at_exact_threshold(self):
        """Test volume spike at exact 3x threshold."""
        volumes = [100.0] * 20  # Average = 100
        
        # Exactly 3x should NOT trigger (> not >=)
        assert is_volume_spike(300.0, volumes) == False
        
        # Just above 3x should trigger
        assert is_volume_spike(300.1, volumes) == True
    
    def test_volume_spike_above_threshold(self):
        """Test volume spike above threshold."""
        volumes = [100.0] * 20
        assert is_volume_spike(400.0, volumes) == True  # 4x
        assert is_volume_spike(500.0, volumes) == True  # 5x
    
    def test_volume_spike_below_threshold(self):
        """Test volume spike below threshold."""
        volumes = [100.0] * 20
        assert is_volume_spike(299.0, volumes) == False  # Just under 3x
        assert is_volume_spike(200.0, volumes) == False  # 2x
    
    def test_volume_spike_varying_history(self):
        """Test volume spike with varying historical volumes."""
        volumes = [50.0, 100.0, 150.0, 100.0, 50.0] * 4  # Average = 90
        
        # 3x average = 270
        assert is_volume_spike(270.0, volumes) == False
        assert is_volume_spike(271.0, volumes) == True
    
    def test_volume_spike_empty_history(self):
        """Test volume spike with empty history."""
        assert is_volume_spike(1000.0, []) == False


class TestPriceSpike:
    """Test price spike detection logic."""
    
    def test_price_spike_at_exact_threshold(self):
        """Test price spike at exact 2% threshold."""
        # Exactly 2% should NOT trigger (> not >=)
        assert is_price_spike(100.0, 102.0) == False  # +2%
        assert is_price_spike(100.0, 98.0) == False   # -2%
        
        # Just above 2% should trigger
        assert is_price_spike(100.0, 102.01) == True  # +2.01%
        assert is_price_spike(100.0, 97.99) == True   # -2.01%
    
    def test_price_spike_above_threshold(self):
        """Test price spike above threshold."""
        assert is_price_spike(100.0, 105.0) == True   # +5%
        assert is_price_spike(100.0, 95.0) == True    # -5%
        assert is_price_spike(100.0, 110.0) == True   # +10%
    
    def test_price_spike_below_threshold(self):
        """Test price spike below threshold."""
        assert is_price_spike(100.0, 101.9) == False  # +1.9%
        assert is_price_spike(100.0, 98.1) == False   # -1.9%
        assert is_price_spike(100.0, 101.0) == False  # +1%
    
    def test_price_spike_zero_open(self):
        """Test price spike with zero open price."""
        assert is_price_spike(0.0, 100.0) == False


class TestRSIExtreme:
    """Test RSI extreme detection logic."""
    
    def test_rsi_extreme_at_boundaries(self):
        """Test RSI extreme at exact boundaries."""
        # Exactly at boundaries should NOT trigger (> and <, not >= and <=)
        assert is_rsi_extreme(70.0) == False   # Exactly 70, not extreme
        assert is_rsi_extreme(30.0) == False   # Exactly 30, not extreme
        
        # Just above/below boundaries should trigger
        assert is_rsi_extreme(70.1) == True   # Just above overbought
        assert is_rsi_extreme(29.9) == True   # Just below oversold
    
    def test_rsi_extreme_overbought(self):
        """Test RSI extreme in overbought territory."""
        assert is_rsi_extreme(71.0) == True
        assert is_rsi_extreme(80.0) == True
        assert is_rsi_extreme(100.0) == True
    
    def test_rsi_extreme_oversold(self):
        """Test RSI extreme in oversold territory."""
        assert is_rsi_extreme(29.0) == True
        assert is_rsi_extreme(20.0) == True
        assert is_rsi_extreme(0.0) == True
    
    def test_rsi_extreme_normal(self):
        """Test RSI extreme in normal range."""
        assert is_rsi_extreme(50.0) == False
        assert is_rsi_extreme(60.0) == False
        assert is_rsi_extreme(40.0) == False
        assert is_rsi_extreme(69.9) == False
        assert is_rsi_extreme(30.1) == False


class TestBBBreakout:
    """Test Bollinger Band breakout detection logic."""
    
    def test_bb_breakout_above_upper(self):
        """Test BB breakout above upper band."""
        bb_upper = 110.0
        bb_lower = 90.0
        
        assert is_bb_breakout(111.0, bb_upper, bb_lower) == True
        assert is_bb_breakout(120.0, bb_upper, bb_lower) == True
    
    def test_bb_breakout_below_lower(self):
        """Test BB breakout below lower band."""
        bb_upper = 110.0
        bb_lower = 90.0
        
        assert is_bb_breakout(89.0, bb_upper, bb_lower) == True
        assert is_bb_breakout(80.0, bb_upper, bb_lower) == True
    
    def test_bb_breakout_inside_bands(self):
        """Test BB breakout inside bands."""
        bb_upper = 110.0
        bb_lower = 90.0
        
        assert is_bb_breakout(100.0, bb_upper, bb_lower) == False
        assert is_bb_breakout(105.0, bb_upper, bb_lower) == False
        assert is_bb_breakout(95.0, bb_upper, bb_lower) == False
    
    def test_bb_breakout_at_bands(self):
        """Test BB breakout at exact band values."""
        bb_upper = 110.0
        bb_lower = 90.0
        
        # At bands should NOT trigger (> and <, not >= and <=)
        assert is_bb_breakout(110.0, bb_upper, bb_lower) == False
        assert is_bb_breakout(90.0, bb_upper, bb_lower) == False


class TestMACDCrossover:
    """Test MACD crossover detection logic."""
    
    def test_macd_crossover_bullish(self):
        """Test bullish MACD crossover (MACD crosses above signal)."""
        # MACD crosses from below to above signal
        assert is_macd_crossover(
            prev_macd=-1.0, prev_signal=0.0,
            curr_macd=1.0, curr_signal=0.0
        ) == True
        
        # MACD crosses from equal to above
        assert is_macd_crossover(
            prev_macd=0.0, prev_signal=0.0,
            curr_macd=1.0, curr_signal=0.0
        ) == True
    
    def test_macd_crossover_bearish(self):
        """Test bearish MACD crossover (MACD crosses below signal)."""
        # MACD crosses from above to below signal
        assert is_macd_crossover(
            prev_macd=1.0, prev_signal=0.0,
            curr_macd=-1.0, curr_signal=0.0
        ) == True
        
        # MACD crosses from equal to below
        assert is_macd_crossover(
            prev_macd=0.0, prev_signal=0.0,
            curr_macd=-1.0, curr_signal=0.0
        ) == True
    
    def test_macd_crossover_no_cross(self):
        """Test no MACD crossover."""
        # MACD stays above signal
        assert is_macd_crossover(
            prev_macd=1.0, prev_signal=0.0,
            curr_macd=2.0, curr_signal=0.0
        ) == False
        
        # MACD stays below signal
        assert is_macd_crossover(
            prev_macd=-2.0, prev_signal=0.0,
            curr_macd=-1.0, curr_signal=0.0
        ) == False
        
        # Both move together
        assert is_macd_crossover(
            prev_macd=1.0, prev_signal=2.0,
            curr_macd=2.0, curr_signal=3.0
        ) == False
    
    def test_macd_crossover_realistic_scenario(self):
        """Test MACD crossover with realistic values."""
        # Bullish crossover with realistic MACD values
        assert is_macd_crossover(
            prev_macd=-0.5, prev_signal=-0.3,
            curr_macd=0.2, curr_signal=0.1
        ) == True
        
        # Bearish crossover with realistic MACD values
        assert is_macd_crossover(
            prev_macd=0.5, prev_signal=0.3,
            curr_macd=-0.2, curr_signal=-0.1
        ) == True
