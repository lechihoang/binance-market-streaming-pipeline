#!/bin/bash

# Start All Services Script
# Starts infrastructure (Docker) and all streaming jobs

set -e

echo "========================================="
echo "Starting Crypto Streaming Pipeline"
echo "========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Load environment variables
if [ -f .env ]; then
    echo -e "${GREEN}Loading environment variables from .env${NC}"
    export $(cat .env | grep -v '^#' | xargs)
else
    echo -e "${YELLOW}No .env file found, using defaults${NC}"
fi

# Clean old checkpoints to avoid offset issues
echo ""
echo -e "${YELLOW}Cleaning old Spark checkpoints...${NC}"
rm -rf /tmp/spark-checkpoints/* 2>/dev/null || true
echo -e "${GREEN}✓ Checkpoints cleaned${NC}"

# Step 1: Start Docker infrastructure
echo ""
echo -e "${GREEN}Step 1: Starting Docker infrastructure (Kafka, Zookeeper, Redis)${NC}"
docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to be ready..."
sleep 10

# Check if Kafka is ready
echo "Checking Kafka..."
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo "Waiting for Kafka to be ready..."
    sleep 2
done
echo -e "${GREEN}✓ Kafka is ready${NC}"

# Check if Redis is ready
echo "Checking Redis..."
until docker exec redis redis-cli ping > /dev/null 2>&1; do
    echo "Waiting for Redis to be ready..."
    sleep 2
done
echo -e "${GREEN}✓ Redis is ready${NC}"

# Activate venv if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
    echo -e "${GREEN}✓ Virtual environment activated${NC}"
fi

# Step 2: Start Binance Connector (Phase 1)
echo ""
echo -e "${GREEN}Step 2: Starting Binance-Kafka Connector${NC}"
nohup python -m src.binance_kafka_connector > logs/connector.log 2>&1 &
CONNECTOR_PID=$!
echo $CONNECTOR_PID > /tmp/connector.pid
echo -e "${GREEN}✓ Connector started (PID: $CONNECTOR_PID)${NC}"

# Wait a bit for connector to start producing data
sleep 5

# Step 3: Start Spark Job 1 - Trade Aggregation
echo ""
echo -e "${GREEN}Step 3: Starting Spark Job 1 - Trade Aggregation${NC}"
nohup python -m src.pyspark_streaming_processor.jobs.trade_aggregation_job \
    > logs/trade_aggregation.log 2>&1 &
JOB1_PID=$!
echo $JOB1_PID > /tmp/trade_aggregation.pid
echo -e "${GREEN}✓ Trade Aggregation Job started (PID: $JOB1_PID)${NC}"

# Wait a bit for job 1 to start producing aggregations
sleep 5

# Step 4: Start Spark Job 2 - Technical Indicators
echo ""
echo -e "${GREEN}Step 4: Starting Spark Job 2 - Technical Indicators${NC}"
nohup python -m src.pyspark_streaming_processor.jobs.technical_indicators_job \
    > logs/technical_indicators.log 2>&1 &
JOB2_PID=$!
echo $JOB2_PID > /tmp/technical_indicators.pid
echo -e "${GREEN}✓ Technical Indicators Job started (PID: $JOB2_PID)${NC}"

# Summary
echo ""
echo "========================================="
echo -e "${GREEN}All services started successfully!${NC}"
echo "========================================="
echo ""
echo "Running processes:"
echo "  - Docker infrastructure: docker-compose ps"
echo "  - Binance Connector: PID $CONNECTOR_PID"
echo "  - Trade Aggregation: PID $JOB1_PID"
echo "  - Technical Indicators: PID $JOB2_PID"
echo ""
echo "Logs:"
echo "  - Connector: tail -f logs/connector.log"
echo "  - Trade Aggregation: tail -f logs/trade_aggregation.log"
echo "  - Technical Indicators: tail -f logs/technical_indicators.log"
echo ""
echo "To stop all services: ./scripts/stop_all.sh"
echo "========================================="
