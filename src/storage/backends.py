"""
Backend storage modules for warm and cold paths.

Contains PostgreSQL (warm) and MinIO (cold) storage implementations.
Provides sub-second latency access to historical data.

Table of Contents:
- PostgresStorage (line ~50)
- MinioStorage (line ~400)
- Health Check Functions (line ~750)

Refactored to use shared utils for:
- Retry logic with exponential backoff (src.utils.retry)
- Metrics tracking (src.utils.metrics)
"""

import io
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor

from src.utils.retry import RetryConfig, retry_operation
from src.utils.metrics import (
    track_latency,
    record_error,
    record_retry,
)

logger = logging.getLogger(__name__)


# ============================================================================
# POSTGRESQL STORAGE - Warm Path
# ============================================================================

class PostgresStorage:
    """PostgreSQL storage for interactive analytics (Warm Path).
    
    Stores 90 days of historical data with sub-second query latency.
    Supports concurrent writes from multiple Spark jobs.
    Tables: trades_1m, indicators, alerts, daily_stats
    """
    
    # Retention periods in days
    RETENTION_TRADES_1M = 90
    RETENTION_INDICATORS = 90
    RETENTION_ALERTS = 180
    RETENTION_DAILY_STATS = 730  # 2 years

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "crypto",
        password: str = "crypto",
        database: str = "crypto_data",
        min_connections: int = 1,
        max_connections: int = 10,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """Initialize PostgreSQL connection pool.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: Database user
            password: Database password
            database: Database name
            min_connections: Minimum connections in pool
            max_connections: Maximum connections in pool
            max_retries: Maximum number of connection retry attempts
            retry_delay: Base delay in seconds between retries (exponential backoff)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_connections = min_connections
        self.max_connections = max_connections
        
        # Configure retry using utils
        self._retry_config = RetryConfig(
            max_retries=max_retries,
            initial_delay_ms=int(retry_delay * 1000),
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.1,
            retryable_exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError),
        )
        
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._connect_with_retry()
        self._init_tables()
        logger.info(
            f"PostgresStorage initialized at {host}:{port}/{database}"
        )

    def _connect_with_retry(self) -> None:
        """Create connection pool with retry logic and exponential backoff.
        
        Uses utils.retry.retry_operation for centralized retry logic.
        
        Raises:
            psycopg2.OperationalError: If connection fails after all retries
        """
        def create_pool():
            self._pool = pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            return self._pool
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", "connect", "failed")
        
        try:
            retry_operation(
                create_pool,
                config=self._retry_config,
                operation_name="PostgreSQL connection",
                on_retry=on_retry,
            )
            record_retry("postgres_storage", "connect", "success")
        except Exception:
            record_error("postgres_storage", "connection_error", "critical")
            raise

    @contextmanager
    def _get_connection(self):
        """Get a connection from the pool with automatic return.
        
        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    def _execute_with_retry(
        self, 
        query: str, 
        params: tuple = None,
        fetch: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute a query with retry logic.
        
        Uses utils.retry.retry_operation for centralized retry logic.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: If True, fetch and return results
            
        Returns:
            List of result dicts if fetch=True, else None
        """
        def execute_query():
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    if fetch:
                        return [dict(row) for row in cur.fetchall()]
                    return None
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", "query", "failed")
        
        try:
            with track_latency("postgres_storage", "query"):
                result = retry_operation(
                    execute_query,
                    config=self._retry_config,
                    operation_name="PostgreSQL query",
                    on_retry=on_retry,
                )
            record_retry("postgres_storage", "query", "success")
            return result
        except Exception as e:
            record_error("postgres_storage", "query_error", "error")
            raise

    def _init_tables(self) -> None:
        """Create tables and indexes if they don't exist.
        
        Creates:
        - trades_1m: 1-minute candle data
        - indicators: Technical indicators
        - alerts: Alert records
        - daily_stats: Daily statistics
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # trades_1m table for 1-minute candles
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades_1m (
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        quote_volume DOUBLE PRECISION,
                        trades_count INTEGER,
                        PRIMARY KEY (symbol, timestamp)
                    )
                """)
                
                # Create index on timestamp for trades_1m
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trades_1m_ts 
                    ON trades_1m(timestamp)
                """)
                
                # indicators table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS indicators (
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        rsi DOUBLE PRECISION,
                        macd DOUBLE PRECISION,
                        macd_signal DOUBLE PRECISION,
                        sma_20 DOUBLE PRECISION,
                        ema_12 DOUBLE PRECISION,
                        ema_26 DOUBLE PRECISION,
                        bb_upper DOUBLE PRECISION,
                        bb_lower DOUBLE PRECISION,
                        atr DOUBLE PRECISION,
                        PRIMARY KEY (symbol, timestamp)
                    )
                """)
                
                # Create index on timestamp for indicators
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_indicators_ts 
                    ON indicators(timestamp)
                """)
                
                # alerts table with SERIAL id
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        message TEXT,
                        metadata JSONB
                    )
                """)
                
                # Create index on timestamp and symbol for alerts
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_ts 
                    ON alerts(timestamp DESC, symbol)
                """)
                
                # daily_stats table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        date DATE NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        volatility DOUBLE PRECISION,
                        PRIMARY KEY (symbol, date)
                    )
                """)
                
        logger.debug("PostgreSQL tables initialized")

    def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    # ==================== Upsert Operations ====================

    def upsert_candle(self, candle: Dict[str, Any]) -> None:
        """Upsert a 1-minute candle record using ON CONFLICT.
        
        Args:
            candle: Dict with keys: timestamp, symbol, open, high, low, close,
                   volume, quote_volume, trades_count
        """
        query = """
            INSERT INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, trades_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trades_count = EXCLUDED.trades_count
        """
        params = (
            candle.get('timestamp'),
            candle.get('symbol'),
            candle.get('open'),
            candle.get('high'),
            candle.get('low'),
            candle.get('close'),
            candle.get('volume'),
            candle.get('quote_volume'),
            candle.get('trades_count')
        )
        self._execute_with_retry(query, params)

    def upsert_indicators(self, indicators: Dict[str, Any]) -> None:
        """Upsert technical indicators record using ON CONFLICT.
        
        Args:
            indicators: Dict with keys: timestamp, symbol, rsi, macd, macd_signal,
                       sma_20, ema_12, ema_26, bb_upper, bb_lower, atr
        """
        query = """
            INSERT INTO indicators
            (timestamp, symbol, rsi, macd, macd_signal, sma_20, ema_12, ema_26, 
             bb_upper, bb_lower, atr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                rsi = EXCLUDED.rsi,
                macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                sma_20 = EXCLUDED.sma_20,
                ema_12 = EXCLUDED.ema_12,
                ema_26 = EXCLUDED.ema_26,
                bb_upper = EXCLUDED.bb_upper,
                bb_lower = EXCLUDED.bb_lower,
                atr = EXCLUDED.atr
        """
        params = (
            indicators.get('timestamp'),
            indicators.get('symbol'),
            indicators.get('rsi'),
            indicators.get('macd'),
            indicators.get('macd_signal'),
            indicators.get('sma_20'),
            indicators.get('ema_12'),
            indicators.get('ema_26'),
            indicators.get('bb_upper'),
            indicators.get('bb_lower'),
            indicators.get('atr')
        )
        self._execute_with_retry(query, params)

    def insert_alert(self, alert: Dict[str, Any]) -> None:
        """Insert an alert record with SERIAL id.
        
        Args:
            alert: Dict with keys: timestamp, symbol, alert_type, severity,
                  message, metadata
        """
        metadata = alert.get('metadata')
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)
        
        query = """
            INSERT INTO alerts
            (timestamp, symbol, alert_type, severity, message, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            alert.get('timestamp'),
            alert.get('symbol'),
            alert.get('alert_type'),
            alert.get('severity'),
            alert.get('message'),
            metadata
        )
        self._execute_with_retry(query, params)

    def upsert_daily_stats(self, stats: Dict[str, Any]) -> None:
        """Upsert daily statistics record using ON CONFLICT.
        
        Args:
            stats: Dict with keys: date, symbol, open, high, low, close,
                  volume, volatility
        """
        query = """
            INSERT INTO daily_stats
            (date, symbol, open, high, low, close, volume, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                volatility = EXCLUDED.volatility
        """
        params = (
            stats.get('date'),
            stats.get('symbol'),
            stats.get('open'),
            stats.get('high'),
            stats.get('low'),
            stats.get('close'),
            stats.get('volume'),
            stats.get('volatility')
        )
        self._execute_with_retry(query, params)

    # ==================== Query Methods ====================

    def query_candles(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query candles for a symbol within time range, aggregated by interval.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start: Start datetime
            end: End datetime
            interval: Candle interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
            
        Returns:
            List of candle dictionaries
        """
        # Map interval to PostgreSQL date_trunc/time bucket
        interval_minutes = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440
        }
        
        minutes = interval_minutes.get(interval, 1)
        
        if minutes == 1:
            # No aggregation needed for 1m
            query = """
                SELECT timestamp, symbol, open, high, low, close, 
                       volume, quote_volume, trades_count
                FROM trades_1m
                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
            """
            result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        else:
            # Aggregate 1m candles into larger intervals
            query = """
                WITH bucketed AS (
                    SELECT 
                        date_trunc('hour', timestamp) + 
                        (EXTRACT(minute FROM timestamp)::int / %s) * INTERVAL '%s minutes' AS bucket,
                        symbol, open, high, low, close, volume, quote_volume, trades_count,
                        timestamp,
                        ROW_NUMBER() OVER (PARTITION BY 
                            date_trunc('hour', timestamp) + 
                            (EXTRACT(minute FROM timestamp)::int / %s) * INTERVAL '%s minutes'
                            ORDER BY timestamp ASC) as rn_first,
                        ROW_NUMBER() OVER (PARTITION BY 
                            date_trunc('hour', timestamp) + 
                            (EXTRACT(minute FROM timestamp)::int / %s) * INTERVAL '%s minutes'
                            ORDER BY timestamp DESC) as rn_last
                    FROM trades_1m
                    WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                )
                SELECT 
                    bucket as timestamp,
                    %s as symbol,
                    (SELECT open FROM bucketed b2 WHERE b2.bucket = b.bucket AND b2.rn_first = 1 LIMIT 1) as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (SELECT close FROM bucketed b3 WHERE b3.bucket = b.bucket AND b3.rn_last = 1 LIMIT 1) as close,
                    SUM(volume) as volume,
                    SUM(quote_volume) as quote_volume,
                    SUM(trades_count) as trades_count
                FROM bucketed b
                GROUP BY bucket
                ORDER BY bucket ASC
            """
            result = self._execute_with_retry(
                query, 
                (minutes, minutes, minutes, minutes, minutes, minutes, symbol, start, end, symbol), 
                fetch=True
            )
        
        return result or []

    def query_indicators(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query technical indicators for a symbol within time range.
        
        Args:
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of indicator dictionaries
        """
        query = """
            SELECT timestamp, symbol, rsi, macd, macd_signal, sma_20,
                   ema_12, ema_26, bb_upper, bb_lower, atr
            FROM indicators
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        return result or []

    def query_alerts(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query alerts for a symbol within time range.
        
        Args:
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of alert dictionaries
        """
        query = """
            SELECT timestamp, symbol, alert_type, severity, message, metadata
            FROM alerts
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp DESC
        """
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        if not result:
            return []
        
        # Parse metadata JSON if present (PostgreSQL returns dict for JSONB)
        alerts = []
        for row in result:
            alert = dict(row)
            # JSONB is already parsed by psycopg2, but handle string case
            if alert.get('metadata') and isinstance(alert['metadata'], str):
                try:
                    alert['metadata'] = json.loads(alert['metadata'])
                except (json.JSONDecodeError, TypeError):
                    pass
            alerts.append(alert)
        return alerts

    def query_daily_stats(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query daily statistics for a symbol within date range.
        
        Args:
            symbol: Trading pair symbol
            start: Start datetime (date portion used)
            end: End datetime (date portion used)
            
        Returns:
            List of daily stats dictionaries
        """
        start_date = start.date() if isinstance(start, datetime) else start
        end_date = end.date() if isinstance(end, datetime) else end
        
        query = """
            SELECT date, symbol, open, high, low, close, volume, volatility
            FROM daily_stats
            WHERE symbol = %s AND date >= %s AND date <= %s
            ORDER BY date ASC
        """
        result = self._execute_with_retry(query, (symbol, start_date, end_date), fetch=True)
        return result or []

    # ==================== Retention Cleanup ====================

    def cleanup_old_data(self) -> Dict[str, int]:
        """Delete records older than retention periods.
        
        Retention periods:
        - trades_1m: 90 days
        - indicators: 90 days
        - alerts: 180 days
        - daily_stats: 2 years (730 days)
        
        Returns:
            Dict with count of deleted records per table
        """
        now = datetime.now()
        deleted_counts = {}
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Cleanup trades_1m (90 days)
                cutoff_trades = now - timedelta(days=self.RETENTION_TRADES_1M)
                cur.execute(
                    "DELETE FROM trades_1m WHERE timestamp < %s",
                    (cutoff_trades,)
                )
                deleted_counts['trades_1m'] = cur.rowcount
                
                # Cleanup indicators (90 days)
                cutoff_indicators = now - timedelta(days=self.RETENTION_INDICATORS)
                cur.execute(
                    "DELETE FROM indicators WHERE timestamp < %s",
                    (cutoff_indicators,)
                )
                deleted_counts['indicators'] = cur.rowcount
                
                # Cleanup alerts (180 days)
                cutoff_alerts = now - timedelta(days=self.RETENTION_ALERTS)
                cur.execute(
                    "DELETE FROM alerts WHERE timestamp < %s",
                    (cutoff_alerts,)
                )
                deleted_counts['alerts'] = cur.rowcount
                
                # Cleanup daily_stats (2 years)
                cutoff_daily = now - timedelta(days=self.RETENTION_DAILY_STATS)
                cur.execute(
                    "DELETE FROM daily_stats WHERE date < %s",
                    (cutoff_daily.date(),)
                )
                deleted_counts['daily_stats'] = cur.rowcount
        
        logger.info(f"Cleanup completed: {deleted_counts}")
        return deleted_counts


# ============================================================================
# MINIO STORAGE - Cold Path
# ============================================================================

class MinioStorage:
    """MinIO/S3 storage for historical data (Cold Path).
    
    Stores data in S3-compatible bucket with date-partitioned paths:
    {bucket}/{data_type}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
    
    Uses snappy compression for efficient storage.
    """
    
    # Parquet configuration
    COMPRESSION = "snappy"
    
    # Schema definitions (matching ParquetStorage)
    KLINES_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('open', pa.float64()),
        ('high', pa.float64()),
        ('low', pa.float64()),
        ('close', pa.float64()),
        ('volume', pa.float64()),
        ('quote_volume', pa.float64()),
        ('trades_count', pa.int64()),
    ])
    
    INDICATORS_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('rsi', pa.float64()),
        ('macd', pa.float64()),
        ('macd_signal', pa.float64()),
        ('sma_20', pa.float64()),
        ('ema_12', pa.float64()),
        ('ema_26', pa.float64()),
        ('bb_upper', pa.float64()),
        ('bb_lower', pa.float64()),
        ('atr', pa.float64()),
    ])

    ALERTS_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('alert_type', pa.string()),
        ('severity', pa.string()),
        ('message', pa.string()),
        ('metadata', pa.string()),  # JSON string
    ])
    
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket: str = "crypto-data",
        secure: bool = False,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint (host:port)
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket: Bucket name for storing data
            secure: Use HTTPS if True
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay in seconds between retries (exponential backoff)
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.secure = secure
        
        # Configure retry using utils
        self._retry_config = RetryConfig(
            max_retries=max_retries,
            initial_delay_ms=int(retry_delay * 1000),
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.1,
            retryable_exceptions=(S3Error, Exception),
        )
        
        self._client: Optional[Minio] = None
        self._connect_with_retry()
        self._ensure_bucket()
        logger.info(
            f"MinioStorage initialized at {endpoint}, bucket={bucket}"
        )

    def _connect_with_retry(self) -> None:
        """Create MinIO client with retry logic and exponential backoff.
        
        Uses utils.retry.retry_operation for centralized retry logic.
        
        Raises:
            S3Error: If connection fails after all retries
        """
        def create_client():
            self._client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            # Test connection by listing buckets
            self._client.list_buckets()
            return self._client
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("minio_storage", "connect", "failed")
        
        try:
            retry_operation(
                create_client,
                config=self._retry_config,
                operation_name="MinIO connection",
                on_retry=on_retry,
            )
            record_retry("minio_storage", "connect", "success")
        except Exception:
            record_error("minio_storage", "connection_error", "critical")
            raise

    def _ensure_bucket(self) -> None:
        """Create bucket if it doesn't exist."""
        try:
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
            else:
                logger.debug(f"Bucket already exists: {self.bucket}")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise

    def _get_object_path(
        self, 
        data_type: str, 
        symbol: str, 
        date: datetime
    ) -> str:
        """Generate object path for a given data type, symbol, and date.
        
        Path format: {data_type}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
        
        Args:
            data_type: Type of data (klines, indicators, alerts)
            symbol: Trading symbol
            date: Date for partitioning
            
        Returns:
            Object path within bucket
        """
        date_str = date.strftime("%Y-%m-%d")
        return f"{data_type}/symbol={symbol}/date={date_str}/data.parquet"

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute an operation with retry logic.
        
        Uses utils.retry.retry_operation for centralized retry logic.
        
        Args:
            operation: Callable to execute
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result of the operation
            
        Raises:
            Exception: If operation fails after all retries
        """
        def execute_op():
            return operation(*args, **kwargs)
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("minio_storage", "operation", "failed")
        
        try:
            with track_latency("minio_storage", "operation"):
                result = retry_operation(
                    execute_op,
                    config=self._retry_config,
                    operation_name="MinIO operation",
                    on_retry=on_retry,
                )
            record_retry("minio_storage", "operation", "success")
            return result
        except Exception as e:
            record_error("minio_storage", "operation_error", "error")
            raise

    def _write_parquet_to_minio(
        self, 
        table: pa.Table, 
        object_path: str
    ) -> bool:
        """Write a PyArrow table to MinIO as Parquet.
        
        Args:
            table: PyArrow table to write
            object_path: Object path within bucket
            
        Returns:
            True if successful
        """
        # Write table to in-memory buffer
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=self.COMPRESSION
        )
        buffer.seek(0)
        
        # Upload to MinIO
        data_length = buffer.getbuffer().nbytes
        self._execute_with_retry(
            self._client.put_object,
            self.bucket,
            object_path,
            buffer,
            data_length,
            content_type="application/octet-stream"
        )
        
        logger.debug(f"Wrote {data_length} bytes to {object_path}")
        return True

    def _read_parquet_from_minio(self, object_path: str) -> Optional[pa.Table]:
        """Read a Parquet file from MinIO.
        
        Args:
            object_path: Object path within bucket
            
        Returns:
            PyArrow table or None if not found
        """
        try:
            response = self._execute_with_retry(
                self._client.get_object,
                self.bucket,
                object_path
            )
            
            # Read into buffer
            buffer = io.BytesIO(response.read())
            response.close()
            response.release_conn()
            
            # Parse Parquet
            return pq.read_table(buffer)
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.debug(f"Object not found: {object_path}")
                return None
            raise

    # ==================== Write Methods ====================

    def write_klines(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        date: datetime
    ) -> bool:
        """Write klines data as Parquet to MinIO.
        
        Args:
            symbol: Trading symbol
            data: List of kline dictionaries
            date: Date for partitioning
            
        Returns:
            True if successful
        """
        if not data:
            return True
        
        # Convert to PyArrow table
        arrays = {
            'timestamp': pa.array([
                d["timestamp"] if isinstance(d["timestamp"], datetime)
                else datetime.fromtimestamp(
                    d["timestamp"] / 1000 if d["timestamp"] > 1e12 else d["timestamp"]
                )
                for d in data
            ], type=pa.timestamp('ms')),
            'symbol': pa.array([d.get("symbol", symbol) for d in data]),
            'open': pa.array([float(d["open"]) for d in data]),
            'high': pa.array([float(d["high"]) for d in data]),
            'low': pa.array([float(d["low"]) for d in data]),
            'close': pa.array([float(d["close"]) for d in data]),
            'volume': pa.array([float(d["volume"]) for d in data]),
            'quote_volume': pa.array([float(d.get("quote_volume", 0)) for d in data]),
            'trades_count': pa.array([int(d.get("trades_count", 0)) for d in data]),
        }
        table = pa.table(arrays, schema=self.KLINES_SCHEMA)
        
        object_path = self._get_object_path("klines", symbol, date)
        return self._write_parquet_to_minio(table, object_path)

    def write_indicators(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        date: datetime
    ) -> bool:
        """Write indicators data as Parquet to MinIO.
        
        Args:
            symbol: Trading symbol
            data: List of indicator dictionaries
            date: Date for partitioning
            
        Returns:
            True if successful
        """
        if not data:
            return True
        
        # Helper to safely convert to float, handling None values
        def safe_float(value, default=0.0):
            if value is None:
                return default
            return float(value)
        
        # Convert to PyArrow table
        arrays = {
            'timestamp': pa.array([
                d["timestamp"] if isinstance(d["timestamp"], datetime)
                else datetime.fromtimestamp(
                    d["timestamp"] / 1000 if d["timestamp"] > 1e12 else d["timestamp"]
                )
                for d in data
            ], type=pa.timestamp('ms')),
            'symbol': pa.array([d.get("symbol", symbol) for d in data]),
            'rsi': pa.array([safe_float(d.get("rsi")) for d in data]),
            'macd': pa.array([safe_float(d.get("macd")) for d in data]),
            'macd_signal': pa.array([safe_float(d.get("macd_signal")) for d in data]),
            'sma_20': pa.array([safe_float(d.get("sma_20")) for d in data]),
            'ema_12': pa.array([safe_float(d.get("ema_12")) for d in data]),
            'ema_26': pa.array([safe_float(d.get("ema_26")) for d in data]),
            'bb_upper': pa.array([safe_float(d.get("bb_upper")) for d in data]),
            'bb_lower': pa.array([safe_float(d.get("bb_lower")) for d in data]),
            'atr': pa.array([safe_float(d.get("atr")) for d in data]),
        }
        table = pa.table(arrays, schema=self.INDICATORS_SCHEMA)
        
        object_path = self._get_object_path("indicators", symbol, date)
        return self._write_parquet_to_minio(table, object_path)

    def write_alerts(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        date: datetime
    ) -> bool:
        """Write alerts data as Parquet to MinIO.
        
        Args:
            symbol: Trading symbol
            data: List of alert dictionaries
            date: Date for partitioning
            
        Returns:
            True if successful
        """
        if not data:
            return True
        
        def parse_timestamp(ts):
            """Parse timestamp from various formats."""
            if isinstance(ts, datetime):
                return ts
            if isinstance(ts, str):
                try:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except ValueError:
                    return datetime.now()
            if isinstance(ts, (int, float)):
                if ts > 1e12:
                    return datetime.fromtimestamp(ts / 1000)
                return datetime.fromtimestamp(ts)
            return datetime.now()
        
        # Convert to PyArrow table
        arrays = {
            'timestamp': pa.array([
                parse_timestamp(d["timestamp"])
                for d in data
            ], type=pa.timestamp('ms')),
            'symbol': pa.array([d.get("symbol", symbol) for d in data]),
            'alert_type': pa.array([d["alert_type"] for d in data]),
            'severity': pa.array([d["severity"] for d in data]),
            'message': pa.array([d.get("message", "") for d in data]),
            'metadata': pa.array([
                json.dumps(d.get("metadata")) if d.get("metadata") else ""
                for d in data
            ]),
        }
        table = pa.table(arrays, schema=self.ALERTS_SCHEMA)
        
        object_path = self._get_object_path("alerts", symbol, date)
        return self._write_parquet_to_minio(table, object_path)

    # ==================== Read Methods ====================

    def read_klines(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Read klines from date-partitioned Parquet files.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of kline dictionaries (empty list if not found)
        """
        all_records = []
        
        # Iterate through each date in range
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("klines", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                # Convert timestamp to timezone-naive for comparison
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                # Filter by time range
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                all_records.extend(df.to_dict('records'))
            
            current += timedelta(days=1)
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def read_indicators(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Read indicators from date-partitioned Parquet files.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of indicator dictionaries (empty list if not found)
        """
        all_records = []
        
        # Iterate through each date in range
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("indicators", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                # Convert timestamp to timezone-naive for comparison
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                # Filter by time range
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                all_records.extend(df.to_dict('records'))
            
            current += timedelta(days=1)
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def read_alerts(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Read alerts from date-partitioned Parquet files.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of alert dictionaries (empty list if not found)
        """
        all_records = []
        
        # Iterate through each date in range
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("alerts", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                # Convert timestamp to timezone-naive for comparison
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                # Filter by time range
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                records = df.to_dict('records')
                
                # Parse metadata JSON
                for record in records:
                    if record.get('metadata'):
                        try:
                            record['metadata'] = json.loads(record['metadata'])
                        except (json.JSONDecodeError, TypeError):
                            record['metadata'] = None
                    else:
                        record['metadata'] = None
                
                all_records.extend(records)
            
            current += timedelta(days=1)
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def close(self) -> None:
        """Close the MinIO client (no-op, client is stateless)."""
        logger.info("MinioStorage closed")


# ============================================================================
# HEALTH CHECKS
# ============================================================================

def check_postgres_health(
    host: str = "localhost",
    port: int = 5432,
    user: str = "crypto",
    password: str = "crypto",
    database: str = "crypto_data",
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """
    Check PostgreSQL connection health (Warm Path).
    
    Uses utils.retry.retry_operation for centralized retry logic.
    
    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        user: Database user
        password: Database password
        database: Database name
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        context: Optional Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    retry_config = RetryConfig(
        max_retries=max_retries,
        initial_delay_ms=int(retry_delay * 1000),
        max_delay_ms=60000,
        multiplier=2.0,
        jitter_factor=0.1,
    )
    
    attempt_count = [0]  # Use list to allow mutation in closure
    
    def do_health_check():
        attempt_count[0] += 1
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10
        )
        # Test connection with simple query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        
        return {
            'service': 'postgresql',
            'tier': 'warm',
            'status': 'healthy',
            'host': host,
            'port': port,
            'database': database,
            'attempt': attempt_count[0],
            'timestamp': datetime.now().isoformat()
        }
    
    def on_retry(attempt: int, delay_ms: int, error: Exception):
        record_retry("postgres_health", "check", "failed")
    
    try:
        with track_latency("postgres_health", "check"):
            result = retry_operation(
                do_health_check,
                config=retry_config,
                operation_name="PostgreSQL health check",
                on_retry=on_retry,
            )
        logger.info(f"PostgreSQL health check passed: {host}:{port}/{database}")
        record_retry("postgres_health", "check", "success")
        return result
    except Exception as e:
        record_error("postgres_health", "health_check_error", "critical")
        raise Exception(f"PostgreSQL health check failed after {max_retries} attempts: {e}")


def check_minio_health(
    endpoint: str = "localhost:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    bucket: str = "crypto-data",
    secure: bool = False,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """
    Check MinIO connection health (Cold Path).
    
    Uses utils.retry.retry_operation for centralized retry logic.
    
    Args:
        endpoint: MinIO endpoint (host:port)
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket: Bucket name to check/create
        secure: Use HTTPS if True
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        context: Optional Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    retry_config = RetryConfig(
        max_retries=max_retries,
        initial_delay_ms=int(retry_delay * 1000),
        max_delay_ms=60000,
        multiplier=2.0,
        jitter_factor=0.1,
    )
    
    attempt_count = [0]  # Use list to allow mutation in closure
    
    def do_health_check():
        attempt_count[0] += 1
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        # Test connection by listing buckets
        client.list_buckets()
        
        # Ensure bucket exists
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info(f"Created MinIO bucket: {bucket}")
        
        return {
            'service': 'minio',
            'tier': 'cold',
            'status': 'healthy',
            'endpoint': endpoint,
            'bucket': bucket,
            'attempt': attempt_count[0],
            'timestamp': datetime.now().isoformat()
        }
    
    def on_retry(attempt: int, delay_ms: int, error: Exception):
        record_retry("minio_health", "check", "failed")
    
    try:
        with track_latency("minio_health", "check"):
            result = retry_operation(
                do_health_check,
                config=retry_config,
                operation_name="MinIO health check",
                on_retry=on_retry,
            )
        logger.info(f"MinIO health check passed: {endpoint}, bucket={bucket}")
        record_retry("minio_health", "check", "success")
        return result
    except Exception as e:
        record_error("minio_health", "health_check_error", "critical")
        raise Exception(f"MinIO health check failed after {max_retries} attempts: {e}")


def check_all_storage_health(
    redis_config: Optional[Dict[str, Any]] = None,
    postgres_config: Optional[Dict[str, Any]] = None,
    minio_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Check health of all 3 storage tiers.
    
    Args:
        redis_config: Redis connection config
        postgres_config: PostgreSQL connection config
        minio_config: MinIO connection config
        
    Returns:
        Dict with health status for each tier
        
    Raises:
        Exception: If any health check fails
    """
    from .redis import check_redis_health
    
    results = {}
    
    # Check Redis (Hot Path)
    redis_cfg = redis_config or {}
    results['redis'] = check_redis_health(**redis_cfg)
    
    # Check PostgreSQL (Warm Path)
    postgres_cfg = postgres_config or {}
    results['postgresql'] = check_postgres_health(**postgres_cfg)
    
    # Check MinIO (Cold Path)
    minio_cfg = minio_config or {}
    results['minio'] = check_minio_health(**minio_cfg)
    
    return results
