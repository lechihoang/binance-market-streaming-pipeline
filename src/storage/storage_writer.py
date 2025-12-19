"""StorageWriter - Multi-tier write coordinator."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from .redis import RedisStorage
from .postgres import PostgresStorage
from .minio import MinioStorage
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BatchResult:
    total_records: int
    success_count: int
    failure_count: int
    tier_results: Dict[str, bool]
    failed_records: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0


class StorageWriter:
    def __init__(
        self,
        redis: RedisStorage,
        postgres: Optional[PostgresStorage] = None,
        minio: Optional[MinioStorage] = None,
    ):
        """Initialize StorageWriter with storage tier instances."""
        self.redis = redis
        self._warm_storage: Optional[PostgresStorage] = postgres
        self._cold_storage: Optional[MinioStorage] = minio
        
        if self._warm_storage is None:
            logger.warning("No warm path storage configured (postgres)")
        if self._cold_storage is None:
            logger.warning("No cold path storage configured (minio)")

    def _write_to_tier(
        self,
        tier: str,
        write_fn: Callable[[], bool],
        data_type: str,
        symbol: str
    ) -> bool:
        """Execute a write operation for a single tier with error handling.
        
        Wraps a write callable with try/except and logging. This method
        provides consistent error handling across all tier writes.
        """
        try:
            result = write_fn()
            return result
        except Exception as e:
            logger.error(f"{tier} write_{data_type} failed for {symbol}: {e}")
            return False

    def _execute_parallel_writes(
        self,
        tier_write_fns: Dict[str, Callable[[], Tuple[str, bool, List[Dict[str, Any]]]]],
        timeout: int = 30
    ) -> Tuple[Dict[str, bool], List[Dict[str, Any]]]:
        """Execute tier writes in parallel using ThreadPoolExecutor.
        
        Extracts common ThreadPoolExecutor logic from batch methods.
        Each tier write function should return a tuple of:
        (tier_name, success, failed_records)
        """
        tier_results = {tier: False for tier in tier_write_fns.keys()}
        all_failed_records: List[Dict[str, Any]] = []
        
        with ThreadPoolExecutor(max_workers=len(tier_write_fns)) as executor:
            futures = {
                executor.submit(write_fn): tier_name
                for tier_name, write_fn in tier_write_fns.items()
            }
            
            for future in as_completed(futures, timeout=timeout):
                tier_name = futures[future]
                try:
                    tier, success, failed = future.result(timeout=timeout)
                    tier_results[tier] = success
                    if not success and failed:
                        all_failed_records.extend(failed)
                except Exception as e:
                    logger.error(f"{tier_name} tier write timed out or failed: {e}")
                    tier_results[tier_name] = False
        
        return tier_results, all_failed_records

    def _transform_aggregation_for_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform aggregation record for Redis storage.
        
        Converts datetime to ISO string and extracts OHLCV fields.
        """
        timestamp_dt: Optional[datetime] = data.get('timestamp')
        timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
        
        return {
            'symbol': data.get('symbol', ''),
            'interval': data.get('interval', '1m'),
            'open': data.get('open'),
            'high': data.get('high'),
            'low': data.get('low'),
            'close': data.get('close'),
            'volume': data.get('volume'),
            'timestamp': timestamp_iso,
        }

    def _transform_aggregation_for_postgres(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform aggregation record for PostgreSQL storage.
        
        Keeps datetime as-is and maps fields to candle schema.
        """
        return {
            'timestamp': data.get('timestamp'),
            'symbol': data.get('symbol', ''),
            'open': data.get('open'),
            'high': data.get('high'),
            'low': data.get('low'),
            'close': data.get('close'),
            'volume': data.get('volume'),
            'quote_volume': data.get('quote_volume'),
            'trades_count': data.get('trades_count'),
            'buy_count': data.get('buy_count', 0),
            'sell_count': data.get('sell_count', 0),
        }

    def _transform_aggregation_for_minio(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform aggregation record for MinIO storage.
        
        Keeps datetime as-is and adds default values for optional fields.
        """
        return {
            'timestamp': data.get('timestamp'),
            'symbol': data.get('symbol', ''),
            'open': data.get('open'),
            'high': data.get('high'),
            'low': data.get('low'),
            'close': data.get('close'),
            'volume': data.get('volume'),
            'quote_volume': data.get('quote_volume', 0),
            'trades_count': data.get('trades_count', 0),
            'buy_count': data.get('buy_count', 0),
            'sell_count': data.get('sell_count', 0),
        }

    def _transform_alert_for_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform alert record for Redis storage.
        
        Converts datetime fields to ISO strings.
        """
        timestamp_dt: Optional[datetime] = data.get('timestamp')
        created_at_dt: Optional[datetime] = data.get('created_at')
        timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
        created_at_iso = created_at_dt.isoformat() if created_at_dt else None
        
        return {
            **data,
            'timestamp': timestamp_iso,
            'created_at': created_at_iso,
        }

    def _transform_alert_for_postgres(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform alert record for PostgreSQL storage.
        
        Maps alert_level to severity and generates message from alert_type and symbol.
        """
        symbol = data.get('symbol', '')
        alert_type = data.get('alert_type')
        
        return {
            'timestamp': data.get('timestamp'),
            'symbol': symbol,
            'alert_type': alert_type,
            'severity': data.get('alert_level'),  # Map alert_level to severity
            'message': f"{alert_type}: {symbol}",
            'metadata': data.get('details'),
        }

    def _transform_alert_for_minio(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform alert record for MinIO storage.
        
        Same mapping as postgres - maps alert_level to severity and generates message.
        """
        symbol = data.get('symbol', '')
        alert_type = data.get('alert_type')
        
        return {
            'timestamp': data.get('timestamp'),
            'symbol': symbol,
            'alert_type': alert_type,
            'severity': data.get('alert_level'),  # Map alert_level to severity
            'message': f"{alert_type}: {symbol}",
            'metadata': data.get('details'),
        }

    def write_aggregation(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write aggregation data to all 3 tiers.
        
        Redis: overwrite aggregations:{symbol}:{interval} hash
        PostgreSQL: upsert into trades_1m table
        MinIO: append to klines partition
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        interval = data.get('interval', '1m')
        timestamp_dt: Optional[datetime] = data.get('timestamp')
        
        # Transform records for each tier
        redis_data = self._transform_aggregation_for_redis(data)
        postgres_data = self._transform_aggregation_for_postgres(data)
        minio_data = self._transform_aggregation_for_minio(data)
        
        # Write to Redis using generic helper
        def write_redis() -> bool:
            self.redis.write_aggregation(symbol, interval, redis_data)
            return True
        
        results['redis'] = self._write_to_tier('redis', write_redis, 'aggregation', symbol)
        
        # Write to warm path (PostgreSQL) using generic helper
        def write_postgres() -> bool:
            if self._warm_storage is None:
                return True  # No warm storage configured, consider success
            self._warm_storage.upsert_candle(postgres_data)
            return True
        
        results['warm'] = self._write_to_tier('warm', write_postgres, 'aggregation', symbol)
        
        # Write to cold path (MinIO) using generic helper
        def write_minio() -> bool:
            if self._cold_storage is None:
                return True  # No cold storage configured, consider success
            write_date = timestamp_dt or datetime.now()
            self._cold_storage.write_klines(symbol, [minio_data], write_date)
            return True
        
        results['cold'] = self._write_to_tier('cold', write_minio, 'aggregation', symbol)
        
        self._log_write_result('aggregation', symbol, results)
        return results

    def write_alert(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write alert to all 3 tiers.
        
        Redis: push to alerts:recent list
        PostgreSQL: insert into alerts table
        MinIO: append to alerts partition
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        timestamp_dt: Optional[datetime] = data.get('timestamp')
        
        # Transform records for each tier
        redis_data = self._transform_alert_for_redis(data)
        postgres_data = self._transform_alert_for_postgres(data)
        minio_data = self._transform_alert_for_minio(data)
        
        # Write to Redis using generic helper
        def write_redis() -> bool:
            self.redis.write_alert(redis_data)
            return True
        
        results['redis'] = self._write_to_tier('redis', write_redis, 'alert', symbol)
        
        # Write to warm path (PostgreSQL) using generic helper
        def write_postgres() -> bool:
            if self._warm_storage is None:
                return True  # No warm storage configured, consider success
            self._warm_storage.insert_alert(postgres_data)
            return True
        
        results['warm'] = self._write_to_tier('warm', write_postgres, 'alert', symbol)
        
        # Write to cold path (MinIO) using generic helper
        def write_minio() -> bool:
            if self._cold_storage is None:
                return True  # No cold storage configured, consider success
            write_date = timestamp_dt or datetime.now()
            self._cold_storage.write_alerts(symbol, [minio_data], write_date)
            return True
        
        results['cold'] = self._write_to_tier('cold', write_minio, 'alert', symbol)
        
        self._log_write_result('alert', symbol, results)
        return results
    
    def _log_write_result(
        self, 
        data_type: str, 
        symbol: str, 
        results: Dict[str, bool]
    ) -> None:
        """Log write results with appropriate level."""
        success_count = sum(results.values())
        total_count = len(results)
        
        if success_count == total_count:
            logger.debug(f"Write {data_type} for {symbol}: all tiers succeeded")
        elif success_count == 0:
            logger.error(f"Write {data_type} for {symbol}: all tiers failed")
        else:
            failed_tiers = [k for k, v in results.items() if not v]
            logger.warning(
                f"Write {data_type} for {symbol}: partial failure - "
                f"{failed_tiers} failed"
            )

    def write_aggregations_batch(
        self, records: List[Dict[str, Any]]
    ) -> BatchResult:
        """Write aggregation data to all 3 tiers in parallel.
        
        Uses _execute_parallel_writes() to call batch methods on all storage tiers
        concurrently. Handles partial failures gracefully - if one tier fails,
        other tiers continue their writes.
        
        Redis: write_aggregations_batch (pipeline HSET)
        PostgreSQL: upsert_candles_batch (executemany with ON CONFLICT)
        MinIO: write_klines_batch (single Parquet file per symbol)
        """
        if not records:
            return BatchResult(
                total_records=0,
                success_count=0,
                failure_count=0,
                tier_results={'redis': True, 'warm': True, 'cold': True},
                failed_records=[],
                duration_ms=0.0
            )
        
        start_time = time.time()
        
        # Use transformer functions for record preparation
        redis_records = [self._transform_aggregation_for_redis(r) for r in records]
        postgres_records = [self._transform_aggregation_for_postgres(r) for r in records]
        minio_records = [self._transform_aggregation_for_minio(r) for r in records]
        
        # Define tier write functions that return (tier_name, success, failed_records)
        def write_redis() -> Tuple[str, bool, List[Dict[str, Any]]]:
            try:
                # write_aggregations_batch returns int (success count), not tuple
                success_count = self.redis.write_aggregations_batch(redis_records)
                return ('redis', success_count == len(redis_records), [])
            except Exception as e:
                logger.error(f"Redis batch write failed: {e}")
                return ('redis', False, redis_records)
        
        def write_postgres() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self._warm_storage is None:
                return ('warm', True, [])
            try:
                row_count = self._warm_storage.upsert_candles_batch(postgres_records)
                return ('warm', row_count > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL batch write failed: {e}")
                return ('warm', False, postgres_records)
        
        def write_minio() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self._cold_storage is None:
                return ('cold', True, [])
            try:
                write_date = records[0].get('timestamp') or datetime.now()
                success_count, failed_symbols = self._cold_storage.write_klines_batch(
                    minio_records, write_date
                )
                return ('cold', len(failed_symbols) == 0, [])
            except Exception as e:
                logger.error(f"MinIO batch write failed: {e}")
                return ('cold', False, minio_records)
        
        # Delegate parallel execution to generic method
        tier_write_fns = {
            'redis': write_redis,
            'warm': write_postgres,
            'cold': write_minio,
        }
        tier_results, all_failed_records = self._execute_parallel_writes(tier_write_fns)
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Calculate success/failure counts
        # Success if at least one tier succeeded
        success_count = len(records) if any(tier_results.values()) else 0
        failure_count = len(records) - success_count
        
        result = BatchResult(
            total_records=len(records),
            success_count=success_count,
            failure_count=failure_count,
            tier_results=tier_results,
            failed_records=all_failed_records,
            duration_ms=duration_ms
        )
        
        self._log_batch_result('aggregations', result)
        return result

    def write_alerts_batch(
        self, alerts: List[Dict[str, Any]]
    ) -> BatchResult:
        """Write alerts to all 3 tiers in parallel.
        
        Uses _execute_parallel_writes() to call batch methods on all storage tiers
        concurrently. Handles partial failures gracefully - if one tier fails,
        other tiers continue their writes.
        
        Redis: write_alerts_batch (pipeline LPUSH)
        PostgreSQL: insert_alerts_batch (executemany)
        MinIO: write_alerts_batch (single Parquet file)
        """
        if not alerts:
            return BatchResult(
                total_records=0,
                success_count=0,
                failure_count=0,
                tier_results={'redis': True, 'warm': True, 'cold': True},
                failed_records=[],
                duration_ms=0.0
            )
        
        start_time = time.time()
        
        # Use transformer functions for record preparation
        redis_alerts = [self._transform_alert_for_redis(a) for a in alerts]
        postgres_alerts = [self._transform_alert_for_postgres(a) for a in alerts]
        minio_alerts = [self._transform_alert_for_minio(a) for a in alerts]
        
        # Define tier write functions that return (tier_name, success, failed_records)
        def write_redis() -> Tuple[str, bool, List[Dict[str, Any]]]:
            try:
                # write_alerts_batch returns int (success count), not tuple
                success_count = self.redis.write_alerts_batch(redis_alerts)
                return ('redis', success_count == len(redis_alerts), [])
            except Exception as e:
                logger.error(f"Redis alerts batch write failed: {e}")
                return ('redis', False, redis_alerts)
        
        def write_postgres() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self._warm_storage is None:
                return ('warm', True, [])
            try:
                row_count = self._warm_storage.insert_alerts_batch(postgres_alerts)
                return ('warm', row_count > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL alerts batch write failed: {e}")
                return ('warm', False, postgres_alerts)
        
        def write_minio() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self._cold_storage is None:
                return ('cold', True, [])
            try:
                write_date = alerts[0].get('timestamp') or datetime.now()
                success_count, errors = self._cold_storage.write_alerts_batch(
                    minio_alerts, write_date
                )
                return ('cold', len(errors) == 0, [])
            except Exception as e:
                logger.error(f"MinIO alerts batch write failed: {e}")
                return ('cold', False, minio_alerts)
        
        # Delegate parallel execution to generic method
        tier_write_fns = {
            'redis': write_redis,
            'warm': write_postgres,
            'cold': write_minio,
        }
        tier_results, all_failed_records = self._execute_parallel_writes(tier_write_fns)
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Calculate success/failure counts
        success_count = len(alerts) if any(tier_results.values()) else 0
        failure_count = len(alerts) - success_count
        
        result = BatchResult(
            total_records=len(alerts),
            success_count=success_count,
            failure_count=failure_count,
            tier_results=tier_results,
            failed_records=all_failed_records,
            duration_ms=duration_ms
        )
        
        self._log_batch_result('alerts', result)
        return result

    def _log_batch_result(self, data_type: str, result: BatchResult) -> None:
        """Log batch write results with appropriate level."""
        tier_status = ", ".join(
            f"{tier}={'OK' if success else 'FAIL'}"
            for tier, success in result.tier_results.items()
        )
        
        if all(result.tier_results.values()):
            logger.debug(
                f"Batch write {data_type}: {result.total_records} records, "
                f"all tiers succeeded ({tier_status}), {result.duration_ms:.1f}ms"
            )
        elif not any(result.tier_results.values()):
            logger.error(
                f"Batch write {data_type}: {result.total_records} records, "
                f"all tiers failed ({tier_status}), {result.duration_ms:.1f}ms"
            )
        else:
            logger.warning(
                f"Batch write {data_type}: {result.total_records} records, "
                f"partial failure ({tier_status}), {result.duration_ms:.1f}ms"
            )
