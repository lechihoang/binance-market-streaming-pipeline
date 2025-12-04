#!/bin/bash

# Status Check Script
# Shows status of all services

echo "========================================="
echo "Crypto Streaming Pipeline Status"
echo "========================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to check process status
check_process() {
    local pid_file=$1
    local name=$2
    
    if [ -f "$pid_file" ]; then
        PID=$(cat "$pid_file")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}✓ $name is running (PID: $PID)${NC}"
            return 0
        else
            echo -e "${RED}✗ $name is not running (stale PID file)${NC}"
            return 1
        fi
    else
        echo -e "${RED}✗ $name is not running${NC}"
        return 1
    fi
}

# Check Docker services
echo ""
echo "Docker Infrastructure:"
echo "----------------------"
docker-compose ps

# Check Python processes
echo ""
echo "Streaming Jobs:"
echo "---------------"
check_process "/tmp/connector.pid" "Binance Connector"
check_process "/tmp/trade_aggregation.pid" "Trade Aggregation Job"
check_process "/tmp/technical_indicators.pid" "Technical Indicators Job"

# Check Kafka topics
echo ""
echo "Kafka Topics:"
echo "-------------"
if docker ps | grep -q kafka; then
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo -e "${YELLOW}Could not list Kafka topics${NC}"
else
    echo -e "${RED}Kafka is not running${NC}"
fi

# Check Redis
echo ""
echo "Redis Status:"
echo "-------------"
if docker ps | grep -q redis; then
    REDIS_INFO=$(docker exec redis redis-cli info stats 2>/dev/null | grep total_commands_processed || echo "")
    if [ -n "$REDIS_INFO" ]; then
        echo -e "${GREEN}✓ Redis is running${NC}"
        echo "$REDIS_INFO"
    else
        echo -e "${YELLOW}Redis is running but could not get stats${NC}"
    fi
else
    echo -e "${RED}✗ Redis is not running${NC}"
fi

# Memory usage
echo ""
echo "Memory Usage:"
echo "-------------"
echo "Docker containers:"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}" 2>/dev/null || echo -e "${YELLOW}Could not get Docker stats${NC}"

echo ""
echo "========================================="
