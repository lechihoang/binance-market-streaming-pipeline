"""
Property-based tests for Grafana dashboard configuration.

Feature: grafana-dashboard
Tests correctness properties for dashboard configuration.
"""

import pytest
import json
from pathlib import Path
from hypothesis import given, settings, strategies as st


# =============================================================================
# Constants
# =============================================================================

DASHBOARDS_PATH = Path("grafana/dashboards")

# Dashboard refresh intervals as specified in requirements
DASHBOARD_REFRESH_INTERVALS = {
    "market-overview.json": "5s",      # Requirement 2.4
    "symbol-deep-dive.json": "10s",    # Requirement 3.5
    "trading-analytics.json": "1m",    # Requirement 4.4
    "system-health.json": "30s",       # Requirement 5.4
}

# Required panels per dashboard as specified in requirements
DASHBOARD_REQUIRED_PANELS = {
    "market-overview.json": {
        # Requirement 2.1: price stat panels for BTC, ETH, BNB, SOL
        "price_panels": ["BTC Price", "ETH Price", "BNB Price", "SOL Price"],
        # Requirement 2.2: volume gauge and volume bar chart
        "volume_panels": ["Total Market Volume", "Volume by Symbol"],
        # Requirement 2.3: top gainers and top losers tables
        "mover_panels": ["Top Gainers", "Top Losers"],
    },
    "symbol-deep-dive.json": {
        # Requirement 3.2: candlestick/timeseries chart
        "price_chart": True,
        # Requirement 3.3: RSI and MACD graphs
        "indicator_panels": ["RSI", "MACD"],
        # Requirement 3.4: recent trades table
        "trades_table": True,
    },
    "trading-analytics.json": {
        # Requirement 4.1: volume heatmap
        "heatmap": True,
        # Requirement 4.2: volatility line chart
        "volatility_chart": True,
        # Requirement 4.3: whale alerts table
        "whale_alerts": True,
    },
    "system-health.json": {
        # Requirement 5.1: messages/sec and processing latency graphs
        "pipeline_panels": ["Messages/sec", "Processing Latency"],
        # Requirement 5.2: CPU, Memory, Disk I/O graphs
        "resource_panels": ["CPU Usage", "Memory Usage", "Disk I/O"],
        # Requirement 5.3: component status panels
        "status_panels": ["Kafka Status", "Redis Status", "API Status"],
    },
}

# Symbol Deep Dive variable configuration (Requirement 3.1)
SYMBOL_VARIABLE_OPTIONS = ["BTC", "ETH", "BNB", "SOL"]
INTERVAL_VARIABLE_OPTIONS = ["1m", "5m", "15m", "1h"]


# =============================================================================
# Helper Functions
# =============================================================================

def load_dashboard_json(filename: str) -> dict:
    """Load and parse a dashboard JSON file."""
    filepath = DASHBOARDS_PATH / filename
    if not filepath.exists():
        pytest.skip(f"Dashboard file not found: {filepath}")
    
    with open(filepath, "r") as f:
        return json.load(f)


def get_panel_titles(dashboard: dict) -> list:
    """Extract all panel titles from a dashboard."""
    panels = dashboard.get("panels", [])
    return [panel.get("title", "") for panel in panels]


def get_panel_types(dashboard: dict) -> dict:
    """Extract panel types mapped by title."""
    panels = dashboard.get("panels", [])
    return {panel.get("title", ""): panel.get("type", "") for panel in panels}


# =============================================================================
# Property 2: Dashboard Refresh Interval Correctness
# Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
# Validates: Requirements 2.4, 3.5, 4.4, 5.4
# =============================================================================

class TestDashboardRefreshIntervals:
    """Property tests for dashboard refresh interval correctness."""

    
    @given(dashboard_name=st.sampled_from(list(DASHBOARD_REFRESH_INTERVALS.keys())))
    @settings(max_examples=100)
    def test_dashboard_refresh_interval_correctness(self, dashboard_name):
        """
        Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
        Validates: Requirements 2.4, 3.5, 4.4, 5.4
        
        For any dashboard JSON file, the refresh field SHALL match the specified
        interval: Market Overview (5s), Symbol Deep Dive (10s), Trading Analytics (1m),
        System Health (30s).
        """
        dashboard = load_dashboard_json(dashboard_name)
        expected_refresh = DASHBOARD_REFRESH_INTERVALS[dashboard_name]
        actual_refresh = dashboard.get("refresh")
        
        assert actual_refresh == expected_refresh, \
            f"Dashboard '{dashboard_name}' has wrong refresh interval: " \
            f"expected '{expected_refresh}', got '{actual_refresh}'"
    
    def test_market_overview_refresh_5s(self):
        """
        Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
        Validates: Requirements 2.4
        
        WHEN dashboard refreshes THEN the system SHALL update data mỗi 5 giây.
        """
        dashboard = load_dashboard_json("market-overview.json")
        assert dashboard.get("refresh") == "5s"
    
    def test_symbol_deep_dive_refresh_10s(self):
        """
        Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
        Validates: Requirements 3.5
        
        WHEN dashboard refreshes THEN the system SHALL update data mỗi 10 giây.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        assert dashboard.get("refresh") == "10s"
    
    def test_trading_analytics_refresh_1m(self):
        """
        Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
        Validates: Requirements 4.4
        
        WHEN dashboard refreshes THEN the system SHALL update data mỗi 1 phút.
        """
        dashboard = load_dashboard_json("trading-analytics.json")
        assert dashboard.get("refresh") == "1m"
    
    def test_system_health_refresh_30s(self):
        """
        Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
        Validates: Requirements 5.4
        
        WHEN dashboard refreshes THEN the system SHALL update data mỗi 30 giây.
        """
        dashboard = load_dashboard_json("system-health.json")
        assert dashboard.get("refresh") == "30s"


# =============================================================================
# Property 3: Dashboard Panel Completeness
# Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
# Validates: Requirements 2.1, 2.2, 2.3, 3.2, 3.3, 3.4, 4.1, 4.2, 4.3, 5.1, 5.2, 5.3
# =============================================================================

class TestDashboardPanelCompleteness:
    """Property tests for dashboard panel completeness."""
    
    def test_market_overview_price_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 2.1
        
        WHEN user opens Market Overview dashboard THEN the system SHALL display
        price stat panels cho BTC, ETH, BNB, SOL với current price và 24h change.
        """
        dashboard = load_dashboard_json("market-overview.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        required_panels = DASHBOARD_REQUIRED_PANELS["market-overview.json"]["price_panels"]
        for panel_name in required_panels:
            assert panel_name in panel_titles, \
                f"Missing price panel: {panel_name}"
            assert panel_types.get(panel_name) == "stat", \
                f"Panel '{panel_name}' should be type 'stat', got '{panel_types.get(panel_name)}'"
    
    def test_market_overview_volume_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 2.2
        
        WHEN user views volume analysis THEN the system SHALL display total market
        volume gauge và volume by symbol bar chart.
        """
        dashboard = load_dashboard_json("market-overview.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Check volume gauge
        assert "Total Market Volume" in panel_titles, "Missing Total Market Volume panel"
        assert panel_types.get("Total Market Volume") == "gauge", \
            "Total Market Volume should be type 'gauge'"
        
        # Check volume bar chart
        assert "Volume by Symbol" in panel_titles, "Missing Volume by Symbol panel"
        assert panel_types.get("Volume by Symbol") == "barchart", \
            "Volume by Symbol should be type 'barchart'"
    
    def test_market_overview_mover_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 2.3
        
        WHEN user views top movers THEN the system SHALL display top gainers và
        top losers tables với columns: Symbol, Price, Change%, Volume.
        """
        dashboard = load_dashboard_json("market-overview.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        required_panels = DASHBOARD_REQUIRED_PANELS["market-overview.json"]["mover_panels"]
        for panel_name in required_panels:
            assert panel_name in panel_titles, f"Missing mover panel: {panel_name}"
            assert panel_types.get(panel_name) == "table", \
                f"Panel '{panel_name}' should be type 'table'"

    
    def test_symbol_deep_dive_price_chart(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 3.2
        
        WHEN user views price chart THEN the system SHALL display candlestick chart
        với OHLC data từ API.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Find price chart panel (contains "Price Chart" in title)
        price_chart_panels = [t for t in panel_titles if "Price Chart" in t]
        assert len(price_chart_panels) > 0, "Missing Price Chart panel"
        
        # Verify it's a timeseries type (Grafana's candlestick visualization)
        for panel_title in price_chart_panels:
            assert panel_types.get(panel_title) == "timeseries", \
                f"Price Chart panel should be type 'timeseries'"
    
    def test_symbol_deep_dive_indicator_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 3.3
        
        WHEN user views technical indicators THEN the system SHALL display RSI graph
        (0-100 scale) và MACD graph (line, signal, histogram).
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Check RSI panel
        rsi_panels = [t for t in panel_titles if "RSI" in t]
        assert len(rsi_panels) > 0, "Missing RSI panel"
        
        # Check MACD panel
        macd_panels = [t for t in panel_titles if "MACD" in t]
        assert len(macd_panels) > 0, "Missing MACD panel"
        
        # Both should be timeseries type
        for panel_title in rsi_panels + macd_panels:
            assert panel_types.get(panel_title) == "timeseries", \
                f"Indicator panel '{panel_title}' should be type 'timeseries'"
    
    def test_symbol_deep_dive_trades_table(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 3.4
        
        WHEN user views order flow THEN the system SHALL display recent trades table
        với columns: Time, Price, Quantity, Side.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Find trades table panel
        trades_panels = [t for t in panel_titles if "Trades" in t or "trades" in t]
        assert len(trades_panels) > 0, "Missing Recent Trades panel"
        
        for panel_title in trades_panels:
            assert panel_types.get(panel_title) == "table", \
                f"Trades panel '{panel_title}' should be type 'table'"
    
    def test_trading_analytics_heatmap(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 4.1
        
        WHEN user views volume heatmap THEN the system SHALL display heatmap theo
        hours và symbols từ DuckDB data.
        """
        dashboard = load_dashboard_json("trading-analytics.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Find heatmap panel
        heatmap_panels = [t for t in panel_titles if "Heatmap" in t or "heatmap" in t]
        assert len(heatmap_panels) > 0, "Missing Volume Heatmap panel"
        
        for panel_title in heatmap_panels:
            assert panel_types.get(panel_title) == "heatmap", \
                f"Heatmap panel '{panel_title}' should be type 'heatmap'"
    
    def test_trading_analytics_volatility_chart(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 4.2
        
        WHEN user views volatility comparison THEN the system SHALL display line chart
        so sánh volatility của multiple symbols.
        """
        dashboard = load_dashboard_json("trading-analytics.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Find volatility panel
        volatility_panels = [t for t in panel_titles if "Volatility" in t or "volatility" in t]
        assert len(volatility_panels) > 0, "Missing Volatility Comparison panel"
        
        for panel_title in volatility_panels:
            assert panel_types.get(panel_title) == "timeseries", \
                f"Volatility panel '{panel_title}' should be type 'timeseries'"
    
    def test_trading_analytics_whale_alerts(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 4.3
        
        WHEN user views whale alerts THEN the system SHALL display table các large
        transactions với columns: Time, Symbol, Side, Amount.
        """
        dashboard = load_dashboard_json("trading-analytics.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Find whale alerts panel
        whale_panels = [t for t in panel_titles if "Whale" in t or "whale" in t]
        assert len(whale_panels) > 0, "Missing Whale Alerts panel"
        
        for panel_title in whale_panels:
            assert panel_types.get(panel_title) == "table", \
                f"Whale Alerts panel '{panel_title}' should be type 'table'"

    
    def test_system_health_pipeline_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 5.1
        
        WHEN user views data pipeline metrics THEN the system SHALL display
        messages/sec graph và processing latency graph từ Prometheus.
        """
        dashboard = load_dashboard_json("system-health.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        required_panels = DASHBOARD_REQUIRED_PANELS["system-health.json"]["pipeline_panels"]
        for panel_name in required_panels:
            assert panel_name in panel_titles, f"Missing pipeline panel: {panel_name}"
            assert panel_types.get(panel_name) == "timeseries", \
                f"Pipeline panel '{panel_name}' should be type 'timeseries'"
    
    def test_system_health_resource_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 5.2
        
        WHEN user views system resources THEN the system SHALL display CPU, Memory,
        Disk I/O graphs.
        """
        dashboard = load_dashboard_json("system-health.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        required_panels = DASHBOARD_REQUIRED_PANELS["system-health.json"]["resource_panels"]
        for panel_name in required_panels:
            assert panel_name in panel_titles, f"Missing resource panel: {panel_name}"
            assert panel_types.get(panel_name) == "timeseries", \
                f"Resource panel '{panel_name}' should be type 'timeseries'"
    
    def test_system_health_status_panels(self):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 5.3
        
        WHEN user views component status THEN the system SHALL display status stat
        panels cho Kafka, Redis, API.
        """
        dashboard = load_dashboard_json("system-health.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        required_panels = DASHBOARD_REQUIRED_PANELS["system-health.json"]["status_panels"]
        for panel_name in required_panels:
            assert panel_name in panel_titles, f"Missing status panel: {panel_name}"
            assert panel_types.get(panel_name) == "stat", \
                f"Status panel '{panel_name}' should be type 'stat'"
    
    @given(dashboard_name=st.sampled_from(list(DASHBOARD_REQUIRED_PANELS.keys())))
    @settings(max_examples=100)
    def test_dashboard_has_panels(self, dashboard_name):
        """
        Feature: grafana-dashboard, Property 3: Dashboard Panel Completeness
        Validates: Requirements 2.1, 2.2, 2.3, 3.2, 3.3, 3.4, 4.1, 4.2, 4.3, 5.1, 5.2, 5.3
        
        For any dashboard JSON file, the panels array SHALL contain at least one panel.
        """
        dashboard = load_dashboard_json(dashboard_name)
        panels = dashboard.get("panels", [])
        
        assert len(panels) > 0, f"Dashboard '{dashboard_name}' has no panels"


# =============================================================================
# Property 4: Variable Configuration Correctness
# Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
# Validates: Requirements 3.1
# =============================================================================

class TestVariableConfiguration:
    """Property tests for dashboard variable configuration correctness."""
    
    def test_symbol_deep_dive_has_symbol_variable(self):
        """
        Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
        Validates: Requirements 3.1
        
        WHEN user selects symbol variable THEN the system SHALL filter all panels
        theo symbol được chọn (BTC, ETH, BNB, SOL).
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        # Find symbol variable
        symbol_var = None
        for var in variables:
            if var.get("name") == "symbol":
                symbol_var = var
                break
        
        assert symbol_var is not None, "Missing 'symbol' variable in Symbol Deep Dive dashboard"
        assert symbol_var.get("type") == "custom", "Symbol variable should be type 'custom'"
    
    def test_symbol_deep_dive_has_interval_variable(self):
        """
        Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
        Validates: Requirements 3.1
        
        Symbol Deep Dive dashboard SHALL have interval variable with options
        (1m, 5m, 15m, 1h).
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        # Find interval variable
        interval_var = None
        for var in variables:
            if var.get("name") == "interval":
                interval_var = var
                break
        
        assert interval_var is not None, "Missing 'interval' variable in Symbol Deep Dive dashboard"
        assert interval_var.get("type") == "custom", "Interval variable should be type 'custom'"
    
    @given(symbol=st.sampled_from(SYMBOL_VARIABLE_OPTIONS))
    @settings(max_examples=100)
    def test_symbol_variable_contains_required_options(self, symbol):
        """
        Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
        Validates: Requirements 3.1
        
        For any required symbol (BTC, ETH, BNB, SOL), the symbol variable SHALL
        contain that option.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        # Find symbol variable
        symbol_var = None
        for var in variables:
            if var.get("name") == "symbol":
                symbol_var = var
                break
        
        assert symbol_var is not None, "Missing 'symbol' variable"
        
        # Check options
        options = symbol_var.get("options", [])
        option_values = [opt.get("value") for opt in options]
        
        assert symbol in option_values, \
            f"Symbol variable missing option: {symbol}. Available: {option_values}"
    
    @given(interval=st.sampled_from(INTERVAL_VARIABLE_OPTIONS))
    @settings(max_examples=100)
    def test_interval_variable_contains_required_options(self, interval):
        """
        Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
        Validates: Requirements 3.1
        
        For any required interval (1m, 5m, 15m, 1h), the interval variable SHALL
        contain that option.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        # Find interval variable
        interval_var = None
        for var in variables:
            if var.get("name") == "interval":
                interval_var = var
                break
        
        assert interval_var is not None, "Missing 'interval' variable"
        
        # Check options
        options = interval_var.get("options", [])
        option_values = [opt.get("value") for opt in options]
        
        assert interval in option_values, \
            f"Interval variable missing option: {interval}. Available: {option_values}"
    
    def test_symbol_variable_all_options_present(self):
        """
        Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
        Validates: Requirements 3.1
        
        Symbol variable SHALL contain all required options: BTC, ETH, BNB, SOL.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        symbol_var = next((v for v in variables if v.get("name") == "symbol"), None)
        assert symbol_var is not None, "Missing 'symbol' variable"
        
        options = symbol_var.get("options", [])
        option_values = set(opt.get("value") for opt in options)
        required_options = set(SYMBOL_VARIABLE_OPTIONS)
        
        assert required_options.issubset(option_values), \
            f"Missing symbol options: {required_options - option_values}"
    
    def test_interval_variable_all_options_present(self):
        """
        Feature: grafana-dashboard, Property 4: Variable Configuration Correctness
        Validates: Requirements 3.1
        
        Interval variable SHALL contain all required options: 1m, 5m, 15m, 1h.
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        interval_var = next((v for v in variables if v.get("name") == "interval"), None)
        assert interval_var is not None, "Missing 'interval' variable"
        
        options = interval_var.get("options", [])
        option_values = set(opt.get("value") for opt in options)
        required_options = set(INTERVAL_VARIABLE_OPTIONS)
        
        assert required_options.issubset(option_values), \
            f"Missing interval options: {required_options - option_values}"
