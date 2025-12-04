#!/bin/bash

# Stop All Services Script
# Gracefully stops all streaming jobs and Docker infrastructure

set -e

echo "========================================="
echo "Stopping Crypto Streaming Pipeline"
echo "========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to stop a process gracefully
stop_process() {
    local pid_file=$1
    local name=$2
    
    if [ -f "$pid_file" ]; then
        PID=$(cat "$pid_file")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${YELLOW}Stopping $name (PID: $PID)...${NC}"
            kill -SIGTERM $PID
            
            # Wait for graceful shutdown (max 30 seconds)
            for i in {1..30}; do
                if ! ps -p $PID > /dev/null 2>&1; then
                    echo -e "${GREEN}✓ $name stopped gracefully${NC}"
                    rm -f "$pid_file"
                    return 0
                fi
                sleep 1
            done
            
            # Force kill if still running
            if ps -p $PID > /dev/null 2>&1; then
                echo -e "${RED}Force killing $name...${NC}"
                kill -9 $PID
                rm -f "$pid_file"
            fi
        else
            echo -e "${YELLOW}$name is not running${NC}"
            rm -f "$pid_file"
        fi
    else
        echo -e "${YELLOW}No PID file found for $name${NC}"
    fi
}

# Step 1: Stop all streaming jobs by pattern (more reliable)
echo ""
echo -e "${GREEN}Step 1: Stopping all streaming jobs${NC}"

# Kill by process name pattern
pkill -f "technical_indicators_job" && echo -e "${GREEN}✓ Killed Technical Indicators Job${NC}" || echo -e "${YELLOW}No Technical Indicators Job running${NC}"
pkill -f "trade_aggregation_job" && echo -e "${GREEN}✓ Killed Trade Aggregation Job${NC}" || echo -e "${YELLOW}No Trade Aggregation Job running${NC}"
pkill -f "whale_alert_job" && echo -e "${GREEN}✓ Killed Whale Alert Job${NC}" || echo -e "${YELLOW}No Whale Alert Job running${NC}"
pkill -f "binance_kafka_connector" && echo -e "${GREEN}✓ Killed Binance Connector${NC}" || echo -e "${YELLOW}No Binance Connector running${NC}"

# Also try PID files as backup
stop_process "/tmp/technical_indicators.pid" "Technical Indicators Job (PID file)"
stop_process "/tmp/trade_aggregation.pid" "Trade Aggregation Job (PID file)"
stop_process "/tmp/connector.pid" "Binance Connector (PID file)"
stop_process "/tmp/whale_alert.pid" "Whale Alert Job (PID file)"

# Clean up any remaining PySpark processes
pkill -f "pyspark" && echo -e "${GREEN}✓ Cleaned up remaining PySpark processes${NC}" || true

# Remove all PID files
rm -f /tmp/connector.pid /tmp/trade_aggregation.pid /tmp/technical_indicators.pid /tmp/whale_alert.pid
echo -e "${GREEN}✓ Cleaned PID files${NC}"

# Step 4: Stop Docker infrastructure
echo ""
echo -e "${GREEN}Step 4: Stopping Docker infrastructure${NC}"
docker-compose down

echo ""
echo "========================================="
echo -e "${GREEN}All services stopped successfully!${NC}"
echo "========================================="
