"""
DuckDBStorage - Warm Path storage for interactive analytics.

Provides sub-second latency access to 90-day historical data
using DuckDB OLAP database.
"""

import duckdb
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json
import os

logger = logging.getLogger(__name__)


class DuckDBStorage:
    """DuckDB storage for interactive analytics (Warm Path).
    
    Stores 90 days of historical data with sub-second query latency.
    Tables: trades_1m, indicators, alerts, daily_stats
    """
    
    # Retention periods in days
    RETENTION_TRADES_1M = 90
    RETENTION_INDICATORS = 90
    RETENTION_ALERTS = 180
    RETENTION_DAILY_STATS = 730  # 2 years
    
    def __init__(self, db_path: str = "/data/duckdb/crypto.duckdb", 
                 read_only: bool = False,
                 max_retries: int = 5,
                 retry_delay: float = 2.0):
        """Initialize DuckDB connection and create tables.
        
        Args:
            db_path: Path to DuckDB database file
            read_only: If True, open in read-only mode (allows concurrent reads)
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay in seconds between retries
        """
        self.db_path = db_path
        self.read_only = read_only
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        self.conn = self._connect_with_retry()
        if not read_only:
            self._init_tables()
        logger.info(f"DuckDBStorage initialized at {db_path} (read_only={read_only})")
    
    def _connect_with_retry(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB with retry logic for handling lock conflicts.
        
        Returns:
            DuckDB connection object
            
        Raises:
            duckdb.IOException: If connection fails after all retries
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                conn = duckdb.connect(self.db_path, read_only=self.read_only)
                if attempt > 0:
                    logger.info(f"DuckDB connection successful on attempt {attempt + 1}")
                return conn
            except duckdb.IOException as e:
                last_error = e
                if "lock" in str(e).lower() and attempt < self.max_retries - 1:
                    logger.warning(
                        f"DuckDB lock conflict (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {self.retry_delay}s: {e}"
                    )
                    time.sleep(self.retry_delay)
                else:
                    raise
        
        raise last_error
    
    def _init_tables(self) -> None:
        """Create tables if they don't exist."""
        # trades_1m table for 1-minute candles
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades_1m (
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                quote_volume DOUBLE,
                trades_count INTEGER,
                PRIMARY KEY (symbol, timestamp)
            )
        """)

        
        # Create index on timestamp for trades_1m
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_1m_ts 
            ON trades_1m(timestamp)
        """)
        
        # indicators table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR NOT NULL,
                rsi DOUBLE,
                macd DOUBLE,
                macd_signal DOUBLE,
                sma_20 DOUBLE,
                ema_12 DOUBLE,
                ema_26 DOUBLE,
                bb_upper DOUBLE,
                bb_lower DOUBLE,
                atr DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        
        # Create index on timestamp for indicators
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_indicators_ts 
            ON indicators(timestamp)
        """)
        
        # alerts table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR NOT NULL,
                alert_type VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                message VARCHAR,
                metadata VARCHAR
            )
        """)
        
        # Create index on timestamp and symbol for alerts
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_ts 
            ON alerts(timestamp DESC, symbol)
        """)
        
        # daily_stats table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                volatility DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)
        
        logger.debug("DuckDB tables initialized")
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")
    
    # ==================== Upsert Operations ====================
    
    def upsert_candle(self, candle: Dict[str, Any]) -> None:
        """Upsert a 1-minute candle record.
        
        Args:
            candle: Dict with keys: timestamp, symbol, open, high, low, close,
                   volume, quote_volume, trades_count
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, trades_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            candle.get('timestamp'),
            candle.get('symbol'),
            candle.get('open'),
            candle.get('high'),
            candle.get('low'),
            candle.get('close'),
            candle.get('volume'),
            candle.get('quote_volume'),
            candle.get('trades_count')
        ])
    
    def upsert_indicators(self, indicators: Dict[str, Any]) -> None:
        """Upsert technical indicators record.
        
        Args:
            indicators: Dict with keys: timestamp, symbol, rsi, macd, macd_signal,
                       sma_20, ema_12, ema_26, bb_upper, bb_lower, atr
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO indicators
            (timestamp, symbol, rsi, macd, macd_signal, sma_20, ema_12, ema_26, 
             bb_upper, bb_lower, atr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
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
        ])
    
    def insert_alert(self, alert: Dict[str, Any]) -> None:
        """Insert an alert record.
        
        Args:
            alert: Dict with keys: timestamp, symbol, alert_type, severity,
                  message, metadata
        """
        metadata = alert.get('metadata')
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)
        
        self.conn.execute("""
            INSERT INTO alerts
            (timestamp, symbol, alert_type, severity, message, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            alert.get('timestamp'),
            alert.get('symbol'),
            alert.get('alert_type'),
            alert.get('severity'),
            alert.get('message'),
            metadata
        ])
    
    def upsert_daily_stats(self, stats: Dict[str, Any]) -> None:
        """Upsert daily statistics record.
        
        Args:
            stats: Dict with keys: date, symbol, open, high, low, close,
                  volume, volatility
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO daily_stats
            (date, symbol, open, high, low, close, volume, volatility)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            stats.get('date'),
            stats.get('symbol'),
            stats.get('open'),
            stats.get('high'),
            stats.get('low'),
            stats.get('close'),
            stats.get('volume'),
            stats.get('volatility')
        ])

    
    # ==================== Query Methods ====================
    
    def query_candles(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query 1-minute candles for a symbol within time range.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start: Start datetime
            end: End datetime
            
        Returns:
            List of candle dictionaries
        """
        result = self.conn.execute("""
            SELECT timestamp, symbol, open, high, low, close, 
                   volume, quote_volume, trades_count
            FROM trades_1m
            WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
        """, [symbol, start, end]).fetchall()
        
        columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close',
                   'volume', 'quote_volume', 'trades_count']
        return [dict(zip(columns, row)) for row in result]
    
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
        result = self.conn.execute("""
            SELECT timestamp, symbol, rsi, macd, macd_signal, sma_20,
                   ema_12, ema_26, bb_upper, bb_lower, atr
            FROM indicators
            WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
        """, [symbol, start, end]).fetchall()
        
        columns = ['timestamp', 'symbol', 'rsi', 'macd', 'macd_signal', 'sma_20',
                   'ema_12', 'ema_26', 'bb_upper', 'bb_lower', 'atr']
        return [dict(zip(columns, row)) for row in result]
    
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
        result = self.conn.execute("""
            SELECT timestamp, symbol, alert_type, severity, message, metadata
            FROM alerts
            WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp DESC
        """, [symbol, start, end]).fetchall()
        
        columns = ['timestamp', 'symbol', 'alert_type', 'severity', 'message', 'metadata']
        alerts = []
        for row in result:
            alert = dict(zip(columns, row))
            # Parse metadata JSON if present
            if alert['metadata']:
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
        result = self.conn.execute("""
            SELECT date, symbol, open, high, low, close, volume, volatility
            FROM daily_stats
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, [symbol, start.date() if isinstance(start, datetime) else start,
              end.date() if isinstance(end, datetime) else end]).fetchall()
        
        columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'volatility']
        return [dict(zip(columns, row)) for row in result]
    
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
        
        # Cleanup trades_1m (90 days)
        cutoff_trades = now - timedelta(days=self.RETENTION_TRADES_1M)
        result = self.conn.execute("""
            DELETE FROM trades_1m WHERE timestamp < ?
        """, [cutoff_trades])
        deleted_counts['trades_1m'] = result.fetchone()[0] if result else 0
        
        # Cleanup indicators (90 days)
        cutoff_indicators = now - timedelta(days=self.RETENTION_INDICATORS)
        result = self.conn.execute("""
            DELETE FROM indicators WHERE timestamp < ?
        """, [cutoff_indicators])
        deleted_counts['indicators'] = result.fetchone()[0] if result else 0
        
        # Cleanup alerts (180 days)
        cutoff_alerts = now - timedelta(days=self.RETENTION_ALERTS)
        result = self.conn.execute("""
            DELETE FROM alerts WHERE timestamp < ?
        """, [cutoff_alerts])
        deleted_counts['alerts'] = result.fetchone()[0] if result else 0
        
        # Cleanup daily_stats (2 years)
        cutoff_daily = now - timedelta(days=self.RETENTION_DAILY_STATS)
        result = self.conn.execute("""
            DELETE FROM daily_stats WHERE date < ?
        """, [cutoff_daily.date()])
        deleted_counts['daily_stats'] = result.fetchone()[0] if result else 0
        
        logger.info(f"Cleanup completed: {deleted_counts}")
        return deleted_counts
