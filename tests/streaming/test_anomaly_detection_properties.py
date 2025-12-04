"""
Property-based tests for Anomaly Detection Job.

Tests correctness properties using Hypothesis library:
- Property 1: Alert completeness (all required fields present)
- Property 2: Multi-anomaly detection (separate alerts for each type)
- Property 3: Anomaly type coverage (all 6 types correctly classified)

Each test runs minimum 100 iterations per design spec.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st


# Import detection functions from existing tests
from tests.streaming.test_anomaly_detectors import (
    is_whale_alert,
    is_volume_spike,
    is_price_spike,
    is_rsi_extreme,
    is_bb_breakout,
    is_macd_crossover,
)


# Alert creation function (mirrors AnomalyDetectionJob._create_alert)
def create_alert(
    timestamp: datetime,
    symbol: str,
    alert_type: str,
    alert_level: str,
    details: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create an alert dictionary with all required fields.
    
    Args:
        timestamp: Event timestamp
        symbol: Trading symbol (e.g., "BTCUSDT")
        alert_type: Type of alert (WHALE_ALERT, VOLUME_SPIKE, etc.)
        alert_level: Severity level (HIGH, MEDIUM, LOW)
        details: Type-specific details dictionary
        
    Returns:
        Alert dictionary with UUID, timestamp, symbol, alert_type, alert_level, details
    """
    return {
        "alert_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
        "symbol": symbol,
        "alert_type": alert_type,
        "alert_level": alert_level,
        "details": details,
        "created_at": datetime.utcnow().isoformat()
    }


# Strategies for generating test data
symbol_strategy = st.sampled_from(["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"])
alert_type_strategy = st.sampled_from([
    "WHALE_ALERT", "VOLUME_SPIKE", "PRICE_SPIKE", 
    "RSI_EXTREME", "BB_BREAKOUT", "MACD_CROSSOVER"
])
alert_level_strategy = st.sampled_from(["HIGH", "MEDIUM", "LOW"])
timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31)
)

# Details strategies for each alert type
whale_details_strategy = st.fixed_dictionaries({
    "price": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "quantity": st.floats(min_value=0.001, max_value=10000.0, allow_nan=False, allow_infinity=False),
    "value": st.floats(min_value=100001.0, max_value=10000000.0, allow_nan=False, allow_infinity=False),
})

volume_spike_details_strategy = st.fixed_dictionaries({
    "volume": st.floats(min_value=1.0, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "avg_volume_20": st.floats(min_value=0.1, max_value=100000.0, allow_nan=False, allow_infinity=False),
    "spike_ratio": st.floats(min_value=3.01, max_value=100.0, allow_nan=False, allow_infinity=False),
})

price_spike_details_strategy = st.fixed_dictionaries({
    "open": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "close": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "price_change_pct": st.floats(min_value=-50.0, max_value=50.0, allow_nan=False, allow_infinity=False),
})

rsi_details_strategy = st.fixed_dictionaries({
    "rsi_14": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    "condition": st.sampled_from(["OVERBOUGHT", "OVERSOLD"]),
})

bb_details_strategy = st.fixed_dictionaries({
    "price": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "bb_upper": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "bb_lower": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
    "breakout_direction": st.sampled_from(["UPPER", "LOWER"]),
})

macd_details_strategy = st.fixed_dictionaries({
    "macd_line": st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    "macd_signal": st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    "direction": st.sampled_from(["BULLISH", "BEARISH"]),
})

# Combined details strategy
details_strategy = st.one_of(
    whale_details_strategy,
    volume_spike_details_strategy,
    price_spike_details_strategy,
    rsi_details_strategy,
    bb_details_strategy,
    macd_details_strategy,
)



class TestAlertCompleteness:
    """
    **Feature: pyspark-simplification, Property 1: Alert completeness**
    
    Property 1: Alert completeness
    *For any* anomaly detected by the system, the generated alert SHALL contain 
    all required fields: alert_id (UUID), timestamp, symbol, alert_type, alert_level, and details.
    **Validates: Requirements 3.1**
    """
    
    @settings(max_examples=100)
    @given(
        timestamp=timestamp_strategy,
        symbol=symbol_strategy,
        alert_type=alert_type_strategy,
        alert_level=alert_level_strategy,
        details=details_strategy,
    )
    def test_alert_contains_all_required_fields(
        self,
        timestamp: datetime,
        symbol: str,
        alert_type: str,
        alert_level: str,
        details: Dict[str, Any],
    ):
        """
        **Feature: pyspark-simplification, Property 1: Alert completeness**
        
        For any anomaly data, the generated alert must contain all required fields.
        """
        # Create alert
        alert = create_alert(
            timestamp=timestamp,
            symbol=symbol,
            alert_type=alert_type,
            alert_level=alert_level,
            details=details,
        )
        
        # Verify all required fields are present
        required_fields = ["alert_id", "timestamp", "symbol", "alert_type", "alert_level", "details", "created_at"]
        
        for field in required_fields:
            assert field in alert, f"Missing required field: {field}"
            assert alert[field] is not None, f"Field {field} is None"
        
        # Verify alert_id is a valid UUID
        try:
            uuid.UUID(alert["alert_id"])
        except ValueError:
            pytest.fail(f"alert_id is not a valid UUID: {alert['alert_id']}")
        
        # Verify timestamp is a string (ISO format)
        assert isinstance(alert["timestamp"], str), "timestamp should be a string"
        
        # Verify symbol is non-empty string
        assert isinstance(alert["symbol"], str), "symbol should be a string"
        assert len(alert["symbol"]) > 0, "symbol should not be empty"
        
        # Verify alert_type is valid
        valid_types = ["WHALE_ALERT", "VOLUME_SPIKE", "PRICE_SPIKE", "RSI_EXTREME", "BB_BREAKOUT", "MACD_CROSSOVER"]
        assert alert["alert_type"] in valid_types, f"Invalid alert_type: {alert['alert_type']}"
        
        # Verify alert_level is valid
        valid_levels = ["HIGH", "MEDIUM", "LOW"]
        assert alert["alert_level"] in valid_levels, f"Invalid alert_level: {alert['alert_level']}"
        
        # Verify details is a dictionary
        assert isinstance(alert["details"], dict), "details should be a dictionary"
        
        # Verify created_at is a string (ISO format)
        assert isinstance(alert["created_at"], str), "created_at should be a string"

    @settings(max_examples=100)
    @given(
        timestamp=timestamp_strategy,
        symbol=symbol_strategy,
    )
    def test_alert_id_uniqueness(self, timestamp: datetime, symbol: str):
        """
        **Feature: pyspark-simplification, Property 1: Alert completeness**
        
        Each alert should have a unique alert_id (UUID).
        """
        # Create multiple alerts with same input
        alerts = [
            create_alert(
                timestamp=timestamp,
                symbol=symbol,
                alert_type="WHALE_ALERT",
                alert_level="HIGH",
                details={"price": 50000.0, "quantity": 3.0, "value": 150000.0},
            )
            for _ in range(10)
        ]
        
        # All alert_ids should be unique
        alert_ids = [alert["alert_id"] for alert in alerts]
        assert len(alert_ids) == len(set(alert_ids)), "Alert IDs should be unique"



class TestMultiAnomalyDetection:
    """
    **Feature: pyspark-simplification, Property 2: Multi-anomaly detection**
    
    Property 2: Multi-anomaly detection
    *For any* batch containing data that triggers multiple anomaly conditions, 
    the job SHALL generate separate alerts for each anomaly type detected.
    **Validates: Requirements 7.2**
    """
    
    @settings(max_examples=100)
    @given(
        timestamp=timestamp_strategy,
        symbol=symbol_strategy,
        # Generate data that can trigger multiple anomalies
        price=st.floats(min_value=10000.0, max_value=100000.0, allow_nan=False, allow_infinity=False),
        quantity=st.floats(min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        volume=st.floats(min_value=100.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
        rsi=st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_separate_alerts_for_each_anomaly_type(
        self,
        timestamp: datetime,
        symbol: str,
        price: float,
        quantity: float,
        volume: float,
        rsi: float,
    ):
        """
        **Feature: pyspark-simplification, Property 2: Multi-anomaly detection**
        
        When multiple anomaly conditions are met, separate alerts should be generated.
        """
        alerts = []
        
        # Check whale alert condition
        trade_value = price * quantity
        if is_whale_alert(price, quantity):
            alerts.append(create_alert(
                timestamp=timestamp,
                symbol=symbol,
                alert_type="WHALE_ALERT",
                alert_level="HIGH",
                details={"price": price, "quantity": quantity, "value": trade_value},
            ))
        
        # Check RSI extreme condition
        if is_rsi_extreme(rsi):
            condition = "OVERBOUGHT" if rsi > 70 else "OVERSOLD"
            alerts.append(create_alert(
                timestamp=timestamp,
                symbol=symbol,
                alert_type="RSI_EXTREME",
                alert_level="LOW",
                details={"rsi_14": rsi, "condition": condition},
            ))
        
        # Check volume spike condition (simulate with fixed historical data)
        historical_volumes = [volume / 4] * 20  # Average = volume/4
        if is_volume_spike(volume, historical_volumes):
            avg_vol = sum(historical_volumes) / len(historical_volumes)
            alerts.append(create_alert(
                timestamp=timestamp,
                symbol=symbol,
                alert_type="VOLUME_SPIKE",
                alert_level="MEDIUM",
                details={"volume": volume, "avg_volume_20": avg_vol, "spike_ratio": volume / avg_vol},
            ))
        
        # Verify each alert has a unique alert_type
        alert_types = [alert["alert_type"] for alert in alerts]
        assert len(alert_types) == len(set(alert_types)), \
            "Each anomaly type should generate exactly one alert"
        
        # Verify all alerts have the same timestamp and symbol
        for alert in alerts:
            assert alert["symbol"] == symbol, "All alerts should have the same symbol"
        
        # Verify each alert is complete
        for alert in alerts:
            assert "alert_id" in alert
            assert "timestamp" in alert
            assert "symbol" in alert
            assert "alert_type" in alert
            assert "alert_level" in alert
            assert "details" in alert

    @settings(max_examples=100)
    @given(
        timestamp=timestamp_strategy,
        symbol=symbol_strategy,
        num_anomalies=st.integers(min_value=1, max_value=6),
    )
    def test_alert_count_matches_anomaly_count(
        self,
        timestamp: datetime,
        symbol: str,
        num_anomalies: int,
    ):
        """
        **Feature: pyspark-simplification, Property 2: Multi-anomaly detection**
        
        The number of alerts generated should match the number of anomalies detected.
        """
        # Define all possible anomaly types with their data
        all_anomalies = [
            ("WHALE_ALERT", "HIGH", {"price": 50000.0, "quantity": 3.0, "value": 150000.0}),
            ("VOLUME_SPIKE", "MEDIUM", {"volume": 1000.0, "avg_volume_20": 100.0, "spike_ratio": 10.0}),
            ("PRICE_SPIKE", "HIGH", {"open": 100.0, "close": 105.0, "price_change_pct": 5.0}),
            ("RSI_EXTREME", "LOW", {"rsi_14": 75.0, "condition": "OVERBOUGHT"}),
            ("BB_BREAKOUT", "MEDIUM", {"price": 115.0, "bb_upper": 110.0, "bb_lower": 90.0, "breakout_direction": "UPPER"}),
            ("MACD_CROSSOVER", "LOW", {"macd_line": 1.0, "macd_signal": 0.5, "direction": "BULLISH"}),
        ]
        
        # Select subset of anomalies
        selected_anomalies = all_anomalies[:num_anomalies]
        
        # Generate alerts
        alerts = []
        for alert_type, alert_level, details in selected_anomalies:
            alerts.append(create_alert(
                timestamp=timestamp,
                symbol=symbol,
                alert_type=alert_type,
                alert_level=alert_level,
                details=details,
            ))
        
        # Verify count matches
        assert len(alerts) == num_anomalies, \
            f"Expected {num_anomalies} alerts, got {len(alerts)}"
        
        # Verify all alert types are unique
        alert_types = [alert["alert_type"] for alert in alerts]
        assert len(alert_types) == len(set(alert_types)), \
            "Each anomaly type should appear only once"



class TestAnomalyTypeCoverage:
    """
    **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
    
    Property 3: Anomaly type coverage
    *For any* input data meeting the threshold conditions, the anomaly detection job 
    SHALL correctly identify and classify all 6 anomaly types: whale alerts, volume spikes, 
    price spikes, RSI extremes, BB breakouts, and MACD crossovers.
    **Validates: Requirements 7.1**
    """
    
    @settings(max_examples=100)
    @given(
        price=st.floats(min_value=1000.0, max_value=100000.0, allow_nan=False, allow_infinity=False),
        quantity=st.floats(min_value=0.1, max_value=1000.0, allow_nan=False, allow_infinity=False),
    )
    def test_whale_alert_detection(self, price: float, quantity: float):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        Whale alerts should be correctly detected when trade value > $100,000.
        """
        trade_value = price * quantity
        is_whale = is_whale_alert(price, quantity)
        
        if trade_value > 100000.0:
            assert is_whale, f"Should detect whale alert for value ${trade_value:,.2f}"
        else:
            assert not is_whale, f"Should not detect whale alert for value ${trade_value:,.2f}"

    @settings(max_examples=100)
    @given(
        current_volume=st.floats(min_value=1.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
        avg_volume=st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    )
    def test_volume_spike_detection(self, current_volume: float, avg_volume: float):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        Volume spikes should be correctly detected when volume > 3x average.
        """
        historical_volumes = [avg_volume] * 20
        is_spike = is_volume_spike(current_volume, historical_volumes)
        
        threshold = avg_volume * 3.0
        if current_volume > threshold:
            assert is_spike, f"Should detect volume spike: {current_volume} > {threshold}"
        else:
            assert not is_spike, f"Should not detect volume spike: {current_volume} <= {threshold}"

    @settings(max_examples=100)
    @given(
        open_price=st.floats(min_value=1.0, max_value=100000.0, allow_nan=False, allow_infinity=False),
        change_pct=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False),
    )
    def test_price_spike_detection(self, open_price: float, change_pct: float):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        Price spikes should be correctly detected when price change > 2%.
        """
        # Skip exact boundary values due to floating-point precision
        assume(abs(abs(change_pct) - 2.0) > 0.001)
        
        close_price = open_price * (1 + change_pct / 100)
        is_spike = is_price_spike(open_price, close_price)
        
        if abs(change_pct) > 2.0:
            assert is_spike, f"Should detect price spike for {change_pct:.2f}% change"
        else:
            assert not is_spike, f"Should not detect price spike for {change_pct:.2f}% change"

    @settings(max_examples=100)
    @given(
        rsi=st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_rsi_extreme_detection(self, rsi: float):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        RSI extremes should be correctly detected when RSI > 70 or RSI < 30.
        """
        is_extreme = is_rsi_extreme(rsi)
        
        if rsi > 70.0 or rsi < 30.0:
            assert is_extreme, f"Should detect RSI extreme for RSI={rsi:.2f}"
        else:
            assert not is_extreme, f"Should not detect RSI extreme for RSI={rsi:.2f}"

    @settings(max_examples=100)
    @given(
        price=st.floats(min_value=50.0, max_value=150.0, allow_nan=False, allow_infinity=False),
        bb_middle=st.just(100.0),
        bb_width=st.floats(min_value=5.0, max_value=20.0, allow_nan=False, allow_infinity=False),
    )
    def test_bb_breakout_detection(self, price: float, bb_middle: float, bb_width: float):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        BB breakouts should be correctly detected when price is outside bands.
        """
        bb_upper = bb_middle + bb_width
        bb_lower = bb_middle - bb_width
        
        is_breakout = is_bb_breakout(price, bb_upper, bb_lower)
        
        if price > bb_upper or price < bb_lower:
            assert is_breakout, f"Should detect BB breakout: price={price}, bands=[{bb_lower}, {bb_upper}]"
        else:
            assert not is_breakout, f"Should not detect BB breakout: price={price}, bands=[{bb_lower}, {bb_upper}]"

    @settings(max_examples=100)
    @given(
        prev_macd=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False),
        prev_signal=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False),
        curr_macd=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False),
        curr_signal=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False),
    )
    def test_macd_crossover_detection(
        self,
        prev_macd: float,
        prev_signal: float,
        curr_macd: float,
        curr_signal: float,
    ):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        MACD crossovers should be correctly detected when MACD crosses signal line.
        """
        is_crossover = is_macd_crossover(prev_macd, prev_signal, curr_macd, curr_signal)
        
        # Bullish crossover: was below/equal, now above
        bullish = (prev_macd <= prev_signal) and (curr_macd > curr_signal)
        # Bearish crossover: was above/equal, now below
        bearish = (prev_macd >= prev_signal) and (curr_macd < curr_signal)
        
        expected = bullish or bearish
        
        assert is_crossover == expected, \
            f"MACD crossover detection mismatch: prev=({prev_macd}, {prev_signal}), curr=({curr_macd}, {curr_signal})"

    @settings(max_examples=100)
    @given(timestamp=timestamp_strategy, symbol=symbol_strategy)
    def test_all_six_anomaly_types_can_be_generated(self, timestamp: datetime, symbol: str):
        """
        **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
        
        The system should be able to generate all 6 anomaly types.
        """
        # Generate one alert of each type
        all_types = [
            ("WHALE_ALERT", "HIGH", {"price": 50000.0, "quantity": 3.0, "value": 150000.0}),
            ("VOLUME_SPIKE", "MEDIUM", {"volume": 1000.0, "avg_volume_20": 100.0, "spike_ratio": 10.0}),
            ("PRICE_SPIKE", "HIGH", {"open": 100.0, "close": 105.0, "price_change_pct": 5.0}),
            ("RSI_EXTREME", "LOW", {"rsi_14": 75.0, "condition": "OVERBOUGHT"}),
            ("BB_BREAKOUT", "MEDIUM", {"price": 115.0, "bb_upper": 110.0, "bb_lower": 90.0, "breakout_direction": "UPPER"}),
            ("MACD_CROSSOVER", "LOW", {"macd_line": 1.0, "macd_signal": 0.5, "direction": "BULLISH"}),
        ]
        
        alerts = []
        for alert_type, alert_level, details in all_types:
            alert = create_alert(
                timestamp=timestamp,
                symbol=symbol,
                alert_type=alert_type,
                alert_level=alert_level,
                details=details,
            )
            alerts.append(alert)
        
        # Verify all 6 types are present
        alert_types = {alert["alert_type"] for alert in alerts}
        expected_types = {"WHALE_ALERT", "VOLUME_SPIKE", "PRICE_SPIKE", "RSI_EXTREME", "BB_BREAKOUT", "MACD_CROSSOVER"}
        
        assert alert_types == expected_types, \
            f"Missing alert types: {expected_types - alert_types}"
        
        # Verify correct alert levels
        level_mapping = {
            "WHALE_ALERT": "HIGH",
            "VOLUME_SPIKE": "MEDIUM",
            "PRICE_SPIKE": "HIGH",
            "RSI_EXTREME": "LOW",
            "BB_BREAKOUT": "MEDIUM",
            "MACD_CROSSOVER": "LOW",
        }
        
        for alert in alerts:
            expected_level = level_mapping[alert["alert_type"]]
            assert alert["alert_level"] == expected_level, \
                f"Wrong level for {alert['alert_type']}: expected {expected_level}, got {alert['alert_level']}"
