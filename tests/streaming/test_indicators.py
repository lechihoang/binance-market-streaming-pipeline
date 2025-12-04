"""
Unit tests for technical indicator calculations.

Tests SMA, EMA, RSI, MACD, Bollinger Bands, and ATR calculations
with known inputs and expected outputs.
"""

import pytest
from src.pyspark_streaming_processor.technical_indicators_job import IndicatorCalculator


class TestSMA:
    """Test Simple Moving Average calculations."""
    
    def test_sma_calculation_period_3(self):
        """Test SMA with known inputs, period=3."""
        prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        # SMA(3) of last 3 prices: (3+4+5)/3 = 4.0
        assert IndicatorCalculator.sma(prices, 3) == 4.0
    
    def test_sma_calculation_period_5(self):
        """Test SMA with known inputs, period=5."""
        prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        # SMA(5) of all prices: (1+2+3+4+5)/5 = 3.0
        assert IndicatorCalculator.sma(prices, 5) == 3.0
    
    def test_sma_insufficient_data(self):
        """Test SMA returns None when insufficient data."""
        prices = [1.0, 2.0]
        assert IndicatorCalculator.sma(prices, 3) is None
    
    def test_sma_exact_period_length(self):
        """Test SMA with exact period length."""
        prices = [10.0, 20.0, 30.0]
        # SMA(3) = (10+20+30)/3 = 20.0
        assert IndicatorCalculator.sma(prices, 3) == 20.0
    
    def test_sma_constant_prices(self):
        """Test SMA with constant prices."""
        prices = [50.0] * 10
        assert IndicatorCalculator.sma(prices, 5) == 50.0


class TestEMA:
    """Test Exponential Moving Average calculations."""
    
    def test_ema_calculation(self):
        """Test EMA with known inputs."""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        # period=3, k=2/(3+1)=0.5
        # First EMA = SMA of first 3 = (10+11+12)/3 = 11.0
        # Second EMA = (13 * 0.5) + (11 * 0.5) = 12.0
        # Third EMA = (14 * 0.5) + (12 * 0.5) = 13.0
        result = IndicatorCalculator.ema(prices, 3)
        assert abs(result - 13.0) < 0.01
    
    def test_ema_insufficient_data(self):
        """Test EMA returns None when insufficient data."""
        prices = [10.0, 11.0]
        assert IndicatorCalculator.ema(prices, 3) is None
    
    def test_ema_with_prev_ema(self):
        """Test EMA with previous EMA value."""
        # Need at least 'period' prices for EMA calculation
        prices = [10.0, 11.0, 12.0, 14.0]
        prev_ema = 12.0
        period = 3
        # The function will calculate EMA from scratch when given a list
        result = IndicatorCalculator.ema(prices, period, prev_ema)
        assert result is not None
    
    def test_ema_constant_prices(self):
        """Test EMA with constant prices equals the constant."""
        prices = [50.0] * 20
        result = IndicatorCalculator.ema(prices, 10)
        assert abs(result - 50.0) < 0.01


class TestRSI:
    """Test Relative Strength Index calculations."""
    
    def test_rsi_all_gains(self):
        """Test RSI with all gains returns 100."""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 
                 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
        rsi = IndicatorCalculator.rsi(prices, 14)
        assert rsi == 100.0
    
    def test_rsi_all_losses(self):
        """Test RSI with all losses returns 0."""
        prices = [24.0, 23.0, 22.0, 21.0, 20.0, 19.0, 18.0, 17.0, 
                 16.0, 15.0, 14.0, 13.0, 12.0, 11.0, 10.0]
        rsi = IndicatorCalculator.rsi(prices, 14)
        assert rsi == 0.0
    
    def test_rsi_bounds(self):
        """Test RSI is always between 0 and 100."""
        prices = [50.0, 51.0, 49.0, 52.0, 48.0, 53.0, 47.0, 54.0, 
                 46.0, 55.0, 45.0, 56.0, 44.0, 57.0, 43.0]
        rsi = IndicatorCalculator.rsi(prices, 14)
        assert 0 <= rsi <= 100
    
    def test_rsi_insufficient_data(self):
        """Test RSI returns None when insufficient data."""
        prices = [50.0, 51.0, 52.0]
        assert IndicatorCalculator.rsi(prices, 14) is None
    
    def test_rsi_mixed_scenario(self):
        """Test RSI with mixed gains and losses."""
        # Alternating gains and losses
        prices = [50.0, 51.0, 50.5, 51.5, 51.0, 52.0, 51.5, 52.5, 
                 52.0, 53.0, 52.5, 53.5, 53.0, 54.0, 53.5]
        rsi = IndicatorCalculator.rsi(prices, 14)
        # RSI should be valid (between 0 and 100)
        assert 0 <= rsi <= 100


class TestMACD:
    """Test MACD calculations."""
    
    def test_macd_calculation(self):
        """Test MACD calculation with sufficient data."""
        # Create trending prices
        prices = list(range(50, 80))  # 30 prices
        macd_line, signal_line, histogram = IndicatorCalculator.macd(prices)
        
        # MACD line should be positive for uptrend
        assert macd_line is not None
        assert macd_line > 0
        
        # Signal line should exist
        assert signal_line is not None
        
        # Histogram should exist
        assert histogram is not None
    
    def test_macd_insufficient_data(self):
        """Test MACD returns None when insufficient data."""
        prices = [50.0, 51.0, 52.0]
        macd_line, signal_line, histogram = IndicatorCalculator.macd(prices)
        assert macd_line is None
        assert signal_line is None
        assert histogram is None
    
    def test_macd_histogram_calculation(self):
        """Test MACD histogram equals MACD - signal."""
        prices = list(range(50, 80))
        macd_line, signal_line, histogram = IndicatorCalculator.macd(prices)
        
        # Histogram should equal MACD - signal
        assert abs(histogram - (macd_line - signal_line)) < 0.01


class TestBollingerBands:
    """Test Bollinger Bands calculations."""
    
    def test_bollinger_bands_constant_prices(self):
        """Test Bollinger Bands with zero variance."""
        prices = [10.0] * 20
        middle, upper, lower = IndicatorCalculator.bollinger_bands(prices, 20)
        
        assert middle == 10.0
        assert upper == 10.0  # stddev = 0
        assert lower == 10.0
    
    def test_bollinger_bands_normal_variance(self):
        """Test Bollinger Bands with normal variance."""
        prices = [10.0, 11.0, 9.0, 12.0, 8.0, 13.0, 7.0, 14.0, 
                 6.0, 15.0, 10.0, 11.0, 9.0, 12.0, 8.0, 13.0, 
                 7.0, 14.0, 6.0, 15.0]
        middle, upper, lower = IndicatorCalculator.bollinger_bands(prices, 20)
        
        # Middle should be the SMA
        assert middle is not None
        
        # Upper should be above middle
        assert upper > middle
        
        # Lower should be below middle
        assert lower < middle
        
        # Bands should be symmetric around middle
        assert abs((upper - middle) - (middle - lower)) < 0.01
    
    def test_bollinger_bands_insufficient_data(self):
        """Test Bollinger Bands returns None when insufficient data."""
        prices = [10.0, 11.0, 12.0]
        middle, upper, lower = IndicatorCalculator.bollinger_bands(prices, 20)
        assert middle is None
        assert upper is None
        assert lower is None


class TestATR:
    """Test Average True Range calculations."""
    
    def test_atr_calculation(self):
        """Test ATR with sample OHLC data."""
        highs = [11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 
                19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0]
        lows = [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 
               17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0]
        closes = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 
                 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
        
        atr = IndicatorCalculator.atr(highs, lows, closes, 14)
        # True range is consistently 2.0 (high - low)
        assert abs(atr - 2.0) < 0.01
    
    def test_atr_insufficient_data(self):
        """Test ATR returns None when insufficient data."""
        highs = [11.0, 12.0]
        lows = [9.0, 10.0]
        closes = [10.0, 11.0]
        
        assert IndicatorCalculator.atr(highs, lows, closes, 14) is None
    
    def test_atr_with_gaps(self):
        """Test ATR with price gaps."""
        highs = [11.0, 12.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 
                26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0]
        lows = [9.0, 10.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 
               24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0]
        closes = [10.0, 11.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 
                 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0]
        
        atr = IndicatorCalculator.atr(highs, lows, closes, 14)
        # ATR should account for the gap
        assert atr is not None
        assert atr > 2.0  # Should be higher due to gap
    
    def test_atr_with_prev_atr(self):
        """Test ATR with previous ATR value (EMA smoothing)."""
        # Need at least period+1 values for ATR
        highs = [25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 
                33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0]
        lows = [23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 
               31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0]
        closes = [24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 
                 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0]
        prev_atr = 2.0
        
        atr = IndicatorCalculator.atr(highs, lows, closes, 14, prev_atr)
        # Should use EMA smoothing with prev_atr
        assert atr is not None
        assert atr > 0
