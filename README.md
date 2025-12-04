# Binance-Kafka Connector

Real-time data ingestion pipeline connecting Binance WebSocket API to Kafka.

## Overview

This service connects to Binance WebSocket streams to collect cryptocurrency market data (trades, klines, tickers) and publishes them to Kafka topics with proper validation, enrichment, and batching.

## Installation

```bash
pip install -e .
```

For development:

```bash
pip install -e ".[dev]"
```

## Configuration

Configure the connector using environment variables. All settings have sensible defaults.

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| **WebSocket Configuration** |
| `BINANCE_WS_URL` | Binance WebSocket endpoint | `wss://stream.binance.com:9443/stream` | No |
| `BINANCE_STREAMS` | Comma-separated list of streams to subscribe | `btcusdt@trade,ethusdt@trade,bnbusdt@trade,btcusdt@kline_1m,ethusdt@kline_1m,btcusdt@ticker` | No |
| `PING_INTERVAL_SECONDS` | Keepalive ping interval | `180` | No |
| `RECONNECT_MAX_DELAY_SECONDS` | Maximum reconnection delay | `60` | No |
| `WS_CONNECTION_TIMEOUT` | Connection timeout in seconds | `10` | No |
| **Kafka Configuration** |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` | No |
| `KAFKA_COMPRESSION` | Compression type (snappy, gzip, lz4) | `snappy` | No |
| `KAFKA_ACKS` | Acknowledgment level (0, 1, all) | `1` | No |
| **Kafka Topics** |
| `TOPIC_RAW_TRADES` | Topic for trade events | `raw_trades` | No |
| `TOPIC_RAW_KLINES` | Topic for kline/candlestick data | `raw_klines` | No |
| `TOPIC_RAW_TICKERS` | Topic for ticker statistics | `raw_tickers` | No |
| **Batching Configuration** |
| `BATCH_SIZE` | Maximum messages per batch | `100` | No |
| `BATCH_TIMEOUT_MS` | Maximum batch age in milliseconds | `100` | No |
| `BATCH_CHECK_INTERVAL_MS` | Batch check interval in milliseconds | `10` | No |
| **Logging** |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` | No |

### Example Configuration

Create a `.env` file for local development:

```bash
# WebSocket Configuration
BINANCE_WS_URL=wss://stream.binance.com:9443/stream
BINANCE_STREAMS=btcusdt@trade,ethusdt@trade,bnbusdt@trade,btcusdt@kline_1m,ethusdt@kline_1m,btcusdt@ticker
PING_INTERVAL_SECONDS=180
RECONNECT_MAX_DELAY_SECONDS=60
WS_CONNECTION_TIMEOUT=10

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_COMPRESSION=snappy
KAFKA_ACKS=1

# Kafka Topics
TOPIC_RAW_TRADES=raw_trades
TOPIC_RAW_KLINES=raw_klines
TOPIC_RAW_TICKERS=raw_tickers

# Batching Configuration
BATCH_SIZE=100
BATCH_TIMEOUT_MS=100
BATCH_CHECK_INTERVAL_MS=10

# Logging
LOG_LEVEL=INFO
```

## Usage

### Running with Docker (Recommended)

The easiest way to run the connector is using Docker Compose, which includes Kafka and Zookeeper:

```bash
# Start all services (Zookeeper, Kafka, and Connector)
docker-compose up -d

# View logs
docker-compose logs -f binance-connector

# Stop all services
docker-compose down
```

The docker-compose setup includes:
- **Zookeeper**: Kafka coordination service (port 2181)
- **Kafka**: Message broker (ports 9092, 9093)
- **Binance Connector**: The data ingestion service

For detailed Docker deployment instructions, monitoring, troubleshooting, and production configuration, see [DOCKER.md](DOCKER.md).

#### Customizing Docker Configuration

Edit the `docker-compose.yml` file to customize environment variables:

```yaml
binance-connector:
  environment:
    BINANCE_STREAMS: "btcusdt@trade,ethusdt@trade"  # Customize streams
    BATCH_SIZE: "200"                                # Adjust batch size
    LOG_LEVEL: "DEBUG"                               # Change log level
```

#### Building the Docker Image

To build the Docker image manually:

```bash
docker build -t binance-kafka-connector:latest .
```

### Running Locally

Run the connector as a module (requires local Kafka):

```bash
python -m binance_kafka_connector
```

Or programmatically:

```python
import asyncio
from binance_kafka_connector import BinanceKafkaConnector

async def main():
    connector = BinanceKafkaConnector()
    await connector.run()

if __name__ == "__main__":
    asyncio.run(main())
```

The connector will:
1. Connect to Binance WebSocket API
2. Subscribe to configured streams
3. Process and validate incoming messages
4. Batch messages for efficient Kafka production
5. Send batches to appropriate Kafka topics
6. Handle reconnections and errors gracefully

### Graceful Shutdown

The connector handles SIGINT (Ctrl+C) and SIGTERM signals gracefully:
- Stops accepting new messages
- Flushes all pending batches to Kafka
- Closes connections cleanly

## Running Tests

```bash
pytest
```

Run with coverage:

```bash
pytest --cov=binance_kafka_connector
```

## Project Structure

```
src/binance_kafka_connector/
├── __init__.py
├── config.py              # Configuration management
├── models.py              # Pydantic data models
├── websocket_client.py    # WebSocket connection manager
├── message_processor.py   # Message parsing and enrichment
├── message_batcher.py     # Message batching logic
├── kafka_producer.py      # Kafka producer client
└── connector.py           # Main application orchestrator

tests/
├── test_models.py
├── test_websocket_client.py
├── test_message_processor.py
├── test_message_batcher.py
├── test_kafka_producer.py
└── test_connector.py
```

## License

MIT
