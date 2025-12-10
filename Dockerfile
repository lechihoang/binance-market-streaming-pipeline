# Dockerfile for Crypto Data Pipeline
# Multi-stage build for optimized image size

# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    libpq-dev \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir . && \
    pip install --no-cache-dir uvicorn[standard] fastapi redis psycopg2-binary minio

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies (curl for healthcheck, libsnappy for Kafka compression)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn
COPY --from=builder /app/src /app/src

# Create data directory
RUN mkdir -p /app/data

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Set Python path
ENV PYTHONPATH=/app/src:/app

# Default command (can be overridden in docker-compose)
CMD ["python", "-m", "binance_kafka_connector"]
