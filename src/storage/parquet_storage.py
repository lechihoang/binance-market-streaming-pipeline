"""
ParquetStorage - Cold Path storage for historical archive.

Provides unlimited retention of historical data using Parquet files
with date/symbol partitioning.
"""

import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetStorage:
    """
    Parquet storage for historical archive (Cold Path).
    
    Stores data in partitioned Parquet files with the structure:
    /data/parquet/{data_type}/year={year}/month={month}/day={day}/symbol={symbol}/
    
    Uses snappy compression for efficient storage.
    """
    
    # Parquet configuration per requirements 3.5
    COMPRESSION = "snappy"
    ROW_GROUP_SIZE = 128 * 1024 * 1024  # 128MB
    DATA_PAGE_SIZE = 1 * 1024 * 1024    # 1MB
    
    # Schema definitions
    TRADES_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('price', pa.float64()),
        ('quantity', pa.float64()),
        ('is_buyer_maker', pa.bool_()),
    ])
    
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
    
    def __init__(self, base_path: str = "/data/parquet"):
        """
        Initialize ParquetStorage.
        
        Args:
            base_path: Base directory for Parquet files
        """
        self.base_path = base_path
    
    def _get_partition_path(
        self, 
        data_type: str, 
        timestamp: datetime, 
        symbol: str
    ) -> str:
        """
        Generate partition path for a given data type, timestamp, and symbol.
        
        Path format: {base_path}/{data_type}/year={year}/month={month}/day={day}/symbol={symbol}/
        
        Args:
            data_type: Type of data (trades, klines_1m, indicators, alerts)
            timestamp: Timestamp for partitioning
            symbol: Trading symbol
            
        Returns:
            Full partition path
        """
        year = timestamp.year
        month = str(timestamp.month).zfill(2)
        day = str(timestamp.day).zfill(2)
        
        return os.path.join(
            self.base_path,
            data_type,
            f"year={year}",
            f"month={month}",
            f"day={day}",
            f"symbol={symbol}"
        )

    def _ensure_directory(self, path: str) -> None:
        """Create directory if it doesn't exist."""
        os.makedirs(path, exist_ok=True)
    
    def _get_file_path(self, partition_path: str, batch_id: Optional[str] = None) -> str:
        """
        Generate file path within a partition.
        
        Args:
            partition_path: Partition directory path
            batch_id: Optional batch identifier for the file
            
        Returns:
            Full file path
        """
        if batch_id:
            filename = f"data_{batch_id}.parquet"
        else:
            filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        return os.path.join(partition_path, filename)
    
    def _write_table(
        self, 
        table: pa.Table, 
        partition_path: str,
        batch_id: Optional[str] = None
    ) -> str:
        """
        Write a PyArrow table to a Parquet file.
        
        Args:
            table: PyArrow table to write
            partition_path: Partition directory path
            batch_id: Optional batch identifier
            
        Returns:
            Path to the written file
        """
        self._ensure_directory(partition_path)
        file_path = self._get_file_path(partition_path, batch_id)
        
        pq.write_table(
            table,
            file_path,
            compression=self.COMPRESSION,
            row_group_size=self.ROW_GROUP_SIZE,
            data_page_size=self.DATA_PAGE_SIZE,
        )
        
        return file_path
    
    def write_trades(
        self, 
        trades: List[Dict[str, Any]], 
        batch_id: Optional[str] = None
    ) -> List[str]:
        """
        Write trades to Parquet files partitioned by date and symbol.
        
        Path: /data/parquet/trades/year={year}/month={month}/day={day}/symbol={symbol}/
        
        Args:
            trades: List of trade dictionaries with timestamp, symbol, price, quantity, is_buyer_maker
            batch_id: Optional batch identifier for the file
            
        Returns:
            List of written file paths
        """
        if not trades:
            return []
        
        # Group trades by partition (date + symbol)
        partitions: Dict[str, List[Dict]] = {}
        for trade in trades:
            ts = trade["timestamp"]
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
            symbol = trade["symbol"]
            partition_path = self._get_partition_path("trades", ts, symbol)
            
            if partition_path not in partitions:
                partitions[partition_path] = []
            partitions[partition_path].append(trade)
        
        # Write each partition
        written_files = []
        for partition_path, partition_trades in partitions.items():
            # Convert to PyArrow table
            arrays = {
                'timestamp': pa.array([
                    t["timestamp"] if isinstance(t["timestamp"], datetime) 
                    else datetime.fromtimestamp(t["timestamp"] / 1000 if t["timestamp"] > 1e12 else t["timestamp"])
                    for t in partition_trades
                ], type=pa.timestamp('ms')),
                'symbol': pa.array([t["symbol"] for t in partition_trades]),
                'price': pa.array([float(t["price"]) for t in partition_trades]),
                'quantity': pa.array([float(t["quantity"]) for t in partition_trades]),
                'is_buyer_maker': pa.array([bool(t.get("is_buyer_maker", False)) for t in partition_trades]),
            }
            table = pa.table(arrays, schema=self.TRADES_SCHEMA)
            file_path = self._write_table(table, partition_path, batch_id)
            written_files.append(file_path)
        
        return written_files
    
    def write_klines(
        self, 
        klines: List[Dict[str, Any]], 
        batch_id: Optional[str] = None
    ) -> List[str]:
        """
        Write klines (candlesticks) to Parquet files partitioned by date and symbol.
        
        Path: /data/parquet/klines_1m/year={year}/month={month}/day={day}/symbol={symbol}/
        
        Args:
            klines: List of kline dictionaries with OHLCV data
            batch_id: Optional batch identifier for the file
            
        Returns:
            List of written file paths
        """
        if not klines:
            return []
        
        # Group klines by partition
        partitions: Dict[str, List[Dict]] = {}
        for kline in klines:
            ts = kline["timestamp"]
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
            symbol = kline["symbol"]
            partition_path = self._get_partition_path("klines_1m", ts, symbol)
            
            if partition_path not in partitions:
                partitions[partition_path] = []
            partitions[partition_path].append(kline)
        
        # Write each partition
        written_files = []
        for partition_path, partition_klines in partitions.items():
            arrays = {
                'timestamp': pa.array([
                    k["timestamp"] if isinstance(k["timestamp"], datetime)
                    else datetime.fromtimestamp(k["timestamp"] / 1000 if k["timestamp"] > 1e12 else k["timestamp"])
                    for k in partition_klines
                ], type=pa.timestamp('ms')),
                'symbol': pa.array([k["symbol"] for k in partition_klines]),
                'open': pa.array([float(k["open"]) for k in partition_klines]),
                'high': pa.array([float(k["high"]) for k in partition_klines]),
                'low': pa.array([float(k["low"]) for k in partition_klines]),
                'close': pa.array([float(k["close"]) for k in partition_klines]),
                'volume': pa.array([float(k["volume"]) for k in partition_klines]),
                'quote_volume': pa.array([float(k.get("quote_volume", 0)) for k in partition_klines]),
                'trades_count': pa.array([int(k.get("trades_count", 0)) for k in partition_klines]),
            }
            table = pa.table(arrays, schema=self.KLINES_SCHEMA)
            file_path = self._write_table(table, partition_path, batch_id)
            written_files.append(file_path)
        
        return written_files
    
    def write_indicators(
        self, 
        indicators: List[Dict[str, Any]], 
        batch_id: Optional[str] = None
    ) -> List[str]:
        """
        Write technical indicators to Parquet files partitioned by date and symbol.
        
        Path: /data/parquet/indicators/year={year}/month={month}/day={day}/symbol={symbol}/
        
        Args:
            indicators: List of indicator dictionaries
            batch_id: Optional batch identifier for the file
            
        Returns:
            List of written file paths
        """
        if not indicators:
            return []
        
        # Group indicators by partition
        partitions: Dict[str, List[Dict]] = {}
        for ind in indicators:
            ts = ind["timestamp"]
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
            symbol = ind["symbol"]
            partition_path = self._get_partition_path("indicators", ts, symbol)
            
            if partition_path not in partitions:
                partitions[partition_path] = []
            partitions[partition_path].append(ind)
        
        # Write each partition
        written_files = []
        for partition_path, partition_indicators in partitions.items():
            arrays = {
                'timestamp': pa.array([
                    i["timestamp"] if isinstance(i["timestamp"], datetime)
                    else datetime.fromtimestamp(i["timestamp"] / 1000 if i["timestamp"] > 1e12 else i["timestamp"])
                    for i in partition_indicators
                ], type=pa.timestamp('ms')),
                'symbol': pa.array([i["symbol"] for i in partition_indicators]),
                'rsi': pa.array([float(i.get("rsi", 0)) for i in partition_indicators]),
                'macd': pa.array([float(i.get("macd", 0)) for i in partition_indicators]),
                'macd_signal': pa.array([float(i.get("macd_signal", 0)) for i in partition_indicators]),
                'sma_20': pa.array([float(i.get("sma_20", 0)) for i in partition_indicators]),
                'ema_12': pa.array([float(i.get("ema_12", 0)) for i in partition_indicators]),
                'ema_26': pa.array([float(i.get("ema_26", 0)) for i in partition_indicators]),
                'bb_upper': pa.array([float(i.get("bb_upper", 0)) for i in partition_indicators]),
                'bb_lower': pa.array([float(i.get("bb_lower", 0)) for i in partition_indicators]),
                'atr': pa.array([float(i.get("atr", 0)) for i in partition_indicators]),
            }
            table = pa.table(arrays, schema=self.INDICATORS_SCHEMA)
            file_path = self._write_table(table, partition_path, batch_id)
            written_files.append(file_path)
        
        return written_files
    
    def write_alerts(
        self, 
        alerts: List[Dict[str, Any]], 
        batch_id: Optional[str] = None
    ) -> List[str]:
        """
        Write alerts to Parquet files partitioned by date and symbol.
        
        Path: /data/parquet/alerts/year={year}/month={month}/day={day}/symbol={symbol}/
        
        Args:
            alerts: List of alert dictionaries
            batch_id: Optional batch identifier for the file
            
        Returns:
            List of written file paths
        """
        if not alerts:
            return []
        
        import json
        
        # Group alerts by partition
        partitions: Dict[str, List[Dict]] = {}
        for alert in alerts:
            ts = alert["timestamp"]
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
            symbol = alert["symbol"]
            partition_path = self._get_partition_path("alerts", ts, symbol)
            
            if partition_path not in partitions:
                partitions[partition_path] = []
            partitions[partition_path].append(alert)
        
        # Write each partition
        written_files = []
        for partition_path, partition_alerts in partitions.items():
            arrays = {
                'timestamp': pa.array([
                    a["timestamp"] if isinstance(a["timestamp"], datetime)
                    else datetime.fromtimestamp(a["timestamp"] / 1000 if a["timestamp"] > 1e12 else a["timestamp"])
                    for a in partition_alerts
                ], type=pa.timestamp('ms')),
                'symbol': pa.array([a["symbol"] for a in partition_alerts]),
                'alert_type': pa.array([a["alert_type"] for a in partition_alerts]),
                'severity': pa.array([a["severity"] for a in partition_alerts]),
                'message': pa.array([a.get("message", "") for a in partition_alerts]),
                'metadata': pa.array([
                    json.dumps(a.get("metadata")) if a.get("metadata") else ""
                    for a in partition_alerts
                ]),
            }
            table = pa.table(arrays, schema=self.ALERTS_SCHEMA)
            file_path = self._write_table(table, partition_path, batch_id)
            written_files.append(file_path)
        
        return written_files

    def _get_partition_paths_for_range(
        self, 
        data_type: str, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[str]:
        """
        Get all partition paths that could contain data for a time range.
        
        Uses partition pruning to only return relevant paths.
        
        Args:
            data_type: Type of data (trades, klines_1m, indicators, alerts)
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of partition paths to scan
        """
        paths = []
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        from datetime import timedelta
        while current <= end_date:
            path = self._get_partition_path(data_type, current, symbol)
            if os.path.exists(path):
                paths.append(path)
            current += timedelta(days=1)
        
        return paths
    
    def _read_parquet_files(self, partition_paths: List[str]) -> Optional[pa.Table]:
        """
        Read and concatenate Parquet files from multiple partition paths.
        
        Args:
            partition_paths: List of partition directory paths
            
        Returns:
            Combined PyArrow table or None if no data
        """
        tables = []
        for path in partition_paths:
            if os.path.exists(path):
                for filename in os.listdir(path):
                    if filename.endswith('.parquet'):
                        file_path = os.path.join(path, filename)
                        try:
                            # Use ParquetFile to avoid schema inference issues
                            pf = pq.ParquetFile(file_path)
                            table = pf.read()
                            tables.append(table)
                        except Exception:
                            # Skip corrupted files
                            continue
        
        if not tables:
            return None
        
        return pa.concat_tables(tables)
    
    def read_trades(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """
        Read trades from Parquet files for a symbol and time range.
        
        Uses partition pruning for efficient reads.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of trade dictionaries
        """
        partition_paths = self._get_partition_paths_for_range("trades", symbol, start, end)
        table = self._read_parquet_files(partition_paths)
        
        if table is None:
            return []
        
        # Filter by time range
        df = table.to_pandas()
        # Convert timestamp to timezone-naive for comparison
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
        df = df.sort_values('timestamp')
        
        return df.to_dict('records')
    
    def read_klines(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """
        Read klines from Parquet files for a symbol and time range.
        
        Uses partition pruning for efficient reads.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of kline dictionaries
        """
        partition_paths = self._get_partition_paths_for_range("klines_1m", symbol, start, end)
        table = self._read_parquet_files(partition_paths)
        
        if table is None:
            return []
        
        # Filter by time range
        df = table.to_pandas()
        # Convert timestamp to timezone-naive for comparison
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
        df = df.sort_values('timestamp')
        
        return df.to_dict('records')
    
    def read_indicators(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """
        Read indicators from Parquet files for a symbol and time range.
        
        Uses partition pruning for efficient reads.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of indicator dictionaries
        """
        partition_paths = self._get_partition_paths_for_range("indicators", symbol, start, end)
        table = self._read_parquet_files(partition_paths)
        
        if table is None:
            return []
        
        # Filter by time range
        df = table.to_pandas()
        # Convert timestamp to timezone-naive for comparison
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
        df = df.sort_values('timestamp')
        
        return df.to_dict('records')
    
    def read_alerts(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """
        Read alerts from Parquet files for a symbol and time range.
        
        Uses partition pruning for efficient reads.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of alert dictionaries
        """
        import json
        
        partition_paths = self._get_partition_paths_for_range("alerts", symbol, start, end)
        table = self._read_parquet_files(partition_paths)
        
        if table is None:
            return []
        
        # Filter by time range
        df = table.to_pandas()
        # Convert timestamp to timezone-naive for comparison
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
        df = df.sort_values('timestamp')
        
        # Parse metadata JSON
        records = df.to_dict('records')
        for record in records:
            if record.get('metadata'):
                try:
                    record['metadata'] = json.loads(record['metadata'])
                except (json.JSONDecodeError, TypeError):
                    record['metadata'] = None
            else:
                record['metadata'] = None
        
        return records
