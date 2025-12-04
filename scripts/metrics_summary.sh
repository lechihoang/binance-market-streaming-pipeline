#!/bin/bash
# Generate metrics summary for PySpark streaming jobs

set -e

echo "=== PySpark Streaming Metrics Summary ==="
echo "Generated at: $(date)"
echo ""

# Kafka consumer lag
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Kafka Consumer Lag"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if command -v kafka-consumer-groups.sh &> /dev/null; then
    kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null | grep -E "GROUP|LAG" || echo "No consumer groups found"
else
    echo "kafka-consumer-groups.sh not found, skipping"
fi

echo ""

# Redis metrics
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Redis Metrics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if command -v redis-cli &> /dev/null; then
    if redis-cli ping > /dev/null 2>&1; then
        echo "Memory Usage:"
        redis-cli INFO memory | grep -E "used_memory_human|used_memory_peak_human|maxmemory_human"
        echo ""
        echo "Key Statistics:"
        redis-cli INFO keyspace
        echo ""
        echo "Sample Keys:"
        redis-cli KEYS "candle:*" | head -5
        redis-cli KEYS "indicator:*" | head -5
    else
        echo "Redis not accessible"
    fi
else
    echo "redis-cli not found, skipping"
fi

echo ""

# DuckDB metrics
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DuckDB Metrics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if command -v duckdb &> /dev/null; then
    if [ -f "./data/streaming_data.duckdb" ]; then
        echo "Table Row Counts:"
        duckdb data/streaming_data.duckdb "
            SELECT 'candles' as table_name, COUNT(*) as row_count FROM candles 
            UNION ALL 
            SELECT 'indicators', COUNT(*) FROM indicators 
            UNION ALL 
            SELECT 'alerts', COUNT(*) FROM alerts;
        " 2>/dev/null || echo "Error querying DuckDB"
        
        echo ""
        echo "Recent Candles (last 5):"
        duckdb data/streaming_data.duckdb "
            SELECT window_start, symbol, open, high, low, close, volume 
            FROM candles 
            ORDER BY window_start DESC 
            LIMIT 5;
        " 2>/dev/null || echo "No candles found"
        
        echo ""
        echo "Alert Summary (last 24 hours):"
        duckdb data/streaming_data.duckdb "
            SELECT alert_type, alert_level, COUNT(*) as count 
            FROM alerts 
            WHERE timestamp > NOW() - INTERVAL 24 HOURS
            GROUP BY alert_type, alert_level 
            ORDER BY count DESC;
        " 2>/dev/null || echo "No recent alerts found"
    else
        echo "DuckDB database not found at ./data/streaming_data.duckdb"
    fi
else
    echo "duckdb not found, skipping"
fi

echo ""

# Processing metrics from logs
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Processing Metrics (from logs)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -d "./logs" ]; then
    echo "Recent Batch Processing (last 10):"
    grep -h "batch_duration\|Batch completed" logs/*.log 2>/dev/null | tail -10 || echo "No batch metrics found"
    
    echo ""
    echo "Error Summary (last hour):"
    error_count=$(grep -h ERROR logs/*.log 2>/dev/null | wc -l)
    echo "Total errors: $error_count"
    if [ "$error_count" -gt 0 ]; then
        echo "Error types:"
        grep -h ERROR logs/*.log 2>/dev/null | awk '{print $5}' | sort | uniq -c | sort -rn | head -5
    fi
else
    echo "Logs directory not found"
fi

echo ""

# Disk usage
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Disk Usage"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Checkpoints:"
du -sh /tmp/spark-checkpoints/* 2>/dev/null || echo "No checkpoints found"

echo ""
echo "Data:"
du -sh ./data/* 2>/dev/null || echo "No data directories found"

echo ""
echo "Logs:"
du -sh ./logs 2>/dev/null || echo "No logs directory found"

echo ""

# System resources
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "System Resources"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Memory Usage:"
if command -v free &> /dev/null; then
    free -h
elif command -v vm_stat &> /dev/null; then
    # macOS
    vm_stat | head -10
else
    echo "Memory stats not available"
fi

echo ""
echo "CPU Usage:"
if command -v top &> /dev/null; then
    top -bn1 | grep "Cpu(s)" || top -l 1 | grep "CPU usage"
else
    echo "CPU stats not available"
fi

echo ""
echo "Disk Space:"
df -h . 2>/dev/null || echo "Disk stats not available"

echo ""
echo "=== Metrics Summary Complete ==="
