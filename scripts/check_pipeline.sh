#!/bin/bash
# Check pipeline health and dependencies

echo "=========================================="
echo "PIPELINE HEALTH CHECK"
echo "=========================================="
echo ""

# Check Docker
echo "1. Docker Services:"
docker-compose ps
echo ""

# Check Kafka topics
echo "2. Kafka Topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "❌ Kafka not available"
echo ""

# Check topic message counts
echo "3. Topic Message Counts:"
for topic in raw_trades processed_aggregations processed_indicators alerts; do
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic $topic 2>/dev/null | awk -F':' '{sum += $3} END {print sum}')
    if [ -z "$count" ]; then
        echo "  $topic: Topic not found"
    else
        echo "  $topic: $count messages"
    fi
done
echo ""

# Check Redis keys
echo "4. Redis Keys:"
echo "  Candles: $(docker exec redis redis-cli --scan --pattern 'candle:*' 2>/dev/null | wc -l | tr -d ' ')"
echo "  Indicators: $(docker exec redis redis-cli --scan --pattern 'indicator:*' 2>/dev/null | wc -l | tr -d ' ')"
echo "  Alerts: $(docker exec redis redis-cli LLEN alerts:recent 2>/dev/null || echo 0)"
echo ""

# Check running processes
echo "5. Running Jobs:"
for pidfile in /tmp/connector.pid /tmp/trade_aggregation.pid /tmp/technical_indicators.pid; do
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        name=$(basename "$pidfile" .pid)
        if ps -p $pid > /dev/null 2>&1; then
            echo "  ✓ $name (PID: $pid)"
        else
            echo "  ❌ $name (stale PID file)"
        fi
    fi
done
echo ""

echo "=========================================="
