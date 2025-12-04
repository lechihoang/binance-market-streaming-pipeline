#!/bin/bash
# Quick kill all streaming jobs (without stopping Docker)

echo "Killing all streaming jobs..."

# Kill by process name
pkill -9 -f "technical_indicators_job" && echo "✓ Killed Technical Indicators Job" || true
pkill -9 -f "trade_aggregation_job" && echo "✓ Killed Trade Aggregation Job" || true  
pkill -9 -f "whale_alert_job" && echo "✓ Killed Whale Alert Job" || true
pkill -9 -f "price_spike_job" && echo "✓ Killed Price Spike Job" || true
pkill -9 -f "volume_spike_job" && echo "✓ Killed Volume Spike Job" || true
pkill -9 -f "rsi_extreme_job" && echo "✓ Killed RSI Extreme Job" || true
pkill -9 -f "macd_crossover_job" && echo "✓ Killed MACD Crossover Job" || true
pkill -9 -f "bb_breakout_job" && echo "✓ Killed BB Breakout Job" || true
pkill -9 -f "binance_kafka_connector" && echo "✓ Killed Binance Connector" || true

# Clean up PySpark processes
pkill -9 -f "pyspark" && echo "✓ Cleaned PySpark processes" || true

# Remove PID files
rm -f /tmp/*.pid 2>/dev/null
echo "✓ Cleaned PID files"

echo ""
echo "All jobs killed!"
