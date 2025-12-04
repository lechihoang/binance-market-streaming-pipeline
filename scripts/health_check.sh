#!/bin/bash
# Health check for all streaming jobs

set -e

echo "=== PySpark Streaming Jobs Health Check ==="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Spark UI is accessible
echo "Checking Spark Jobs..."
SPARK_JOBS=(
    "4040:Trade Aggregation"
    "4041:Technical Indicators"
    "4042:Whale Alert"
    "4043:Volume Spike"
    "4044:Price Spike"
    "4045:RSI Extreme"
    "4046:BB Breakout"
    "4047:MACD Crossover"
)

for job in "${SPARK_JOBS[@]}"; do
    port="${job%%:*}"
    name="${job##*:}"
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:$port | grep -q "200"; then
        echo -e "${GREEN}✓${NC} $name (port $port) is running"
    else
        echo -e "${RED}✗${NC} $name (port $port) is not accessible"
    fi
done

echo ""

# Check Kafka connectivity
echo "Checking Kafka..."
if command -v kafka-topics.sh &> /dev/null; then
    if kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Kafka is accessible"
        
        # List topics
        echo "  Topics:"
        kafka-topics.sh --bootstrap-server localhost:9092 --list | sed 's/^/    /'
    else
        echo -e "${RED}✗${NC} Kafka is not accessible"
    fi
else
    echo -e "${YELLOW}⚠${NC} kafka-topics.sh not found, skipping Kafka check"
fi

echo ""

# Check Redis connectivity
echo "Checking Redis..."
if command -v redis-cli &> /dev/null; then
    if redis-cli ping > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Redis is accessible"
        
        # Get Redis info
        memory=$(redis-cli INFO memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
        keys=$(redis-cli DBSIZE | cut -d: -f2 | tr -d ' \r')
        echo "  Memory: $memory"
        echo "  Keys: $keys"
    else
        echo -e "${RED}✗${NC} Redis is not accessible"
    fi
else
    echo -e "${YELLOW}⚠${NC} redis-cli not found, skipping Redis check"
fi

echo ""

# Check checkpoint directories
echo "Checking Checkpoint Directories..."
CHECKPOINT_BASE="/tmp/spark-checkpoints"
if [ -d "$CHECKPOINT_BASE" ]; then
    for dir in "$CHECKPOINT_BASE"/*; do
        if [ -d "$dir" ]; then
            size=$(du -sh "$dir" 2>/dev/null | cut -f1)
            name=$(basename "$dir")
            echo -e "${GREEN}✓${NC} Checkpoint: $name - Size: $size"
        fi
    done
else
    echo -e "${YELLOW}⚠${NC} Checkpoint directory not found: $CHECKPOINT_BASE"
fi

echo ""

# Check data directories
echo "Checking Data Directories..."
if [ -d "./data" ]; then
    if [ -f "./data/streaming_data.duckdb" ]; then
        size=$(du -sh "./data/streaming_data.duckdb" | cut -f1)
        echo -e "${GREEN}✓${NC} DuckDB database exists - Size: $size"
    else
        echo -e "${YELLOW}⚠${NC} DuckDB database not found"
    fi
    
    if [ -d "./data/parquet_output" ]; then
        size=$(du -sh "./data/parquet_output" | cut -f1)
        count=$(find "./data/parquet_output" -name "*.parquet" | wc -l)
        echo -e "${GREEN}✓${NC} Parquet output directory exists - Size: $size, Files: $count"
    else
        echo -e "${YELLOW}⚠${NC} Parquet output directory not found"
    fi
else
    echo -e "${YELLOW}⚠${NC} Data directory not found"
fi

echo ""

# Check recent logs for errors
echo "Checking Recent Errors..."
if [ -d "./logs" ]; then
    error_count=$(grep -h ERROR logs/*.log 2>/dev/null | wc -l)
    if [ "$error_count" -gt 0 ]; then
        echo -e "${YELLOW}⚠${NC} Found $error_count errors in logs"
        echo "  Recent errors:"
        grep -h ERROR logs/*.log 2>/dev/null | tail -5 | sed 's/^/    /'
    else
        echo -e "${GREEN}✓${NC} No errors found in logs"
    fi
else
    echo -e "${YELLOW}⚠${NC} Logs directory not found"
fi

echo ""
echo "=== Health Check Complete ==="
