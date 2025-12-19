"""Storage lifecycle management - cleanup, compaction, and retention policies."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class LifecycleConfig:
    postgres_retention_days: int = 90
    minio_retention_days: int = 365
    compaction_min_files: int = 2
    cleanup_batch_size: int = 1000
    enabled_tiers: List[str] = field(default_factory=lambda: ["postgres", "minio"])


@dataclass
class CleanupResult:
    tier: str
    records_deleted: int
    bytes_reclaimed: int
    duration_ms: float
    success: bool
    error: Optional[str] = None


@dataclass
class CompactionResult:
    partitions_processed: int
    files_merged: int
    files_deleted: int
    bytes_before: int
    bytes_after: int
    duration_ms: float
    success: bool
    errors: List[str] = field(default_factory=list)


@dataclass
class LifecycleResult:
    postgres_cleanup: Optional[CleanupResult]
    minio_compaction: Optional[CompactionResult]
    minio_retention: Optional[CleanupResult]
    total_duration_ms: float
    success: bool


@dataclass
class TierStatus:
    tier: str
    last_run: Optional[str]
    success: bool
    records_affected: int = 0
    bytes_reclaimed: int = 0
    error: Optional[str] = None


@dataclass
class LifecycleStatus:
    last_run: Optional[str]
    overall_success: bool
    tiers: List[TierStatus] = field(default_factory=list)


import threading
import time

from src.utils.logging import get_logger
from src.utils.metrics import (
    track_latency,
    record_error,
    MESSAGES_PROCESSED,
)
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage

logger = get_logger(__name__)


class LifecycleManager:
    """Main coordinator for all lifecycle operations."""
    
    def __init__(
        self,
        postgres: PostgresStorage,
        minio: MinioStorage,
        config: Optional[LifecycleConfig] = None
    ):
        """Initialize LifecycleManager with storage instances and configuration."""
        self.postgres = postgres
        self.minio = minio
        self.config = config or LifecycleConfig()
        
        self._lock = threading.Lock()
        self._running = False
        
        logger.info(
            f"LifecycleManager initialized with config: "
            f"postgres_retention={self.config.postgres_retention_days}d, "
            f"minio_retention={self.config.minio_retention_days}d, "
            f"compaction_min_files={self.config.compaction_min_files}"
        )

    def run_postgres_cleanup(self) -> CleanupResult:
        """Run PostgreSQL retention cleanup."""
        start_time = time.perf_counter()
        
        try:
            with track_latency("lifecycle_manager", "postgres_cleanup"):
                results = self.postgres.cleanup_all_tables(
                    retention_days=self.config.postgres_retention_days,
                    batch_size=self.config.cleanup_batch_size
                )
            
            total_deleted = sum(results.values())
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            MESSAGES_PROCESSED.labels(
                service="lifecycle_manager",
                topic="postgres_cleanup",
                status="success"
            ).inc(total_deleted)
            
            logger.info(
                f"PostgreSQL cleanup completed: {total_deleted} records deleted "
                f"in {duration_ms:.2f}ms"
            )
            
            return CleanupResult(
                tier="postgres",
                records_deleted=total_deleted,
                bytes_reclaimed=0,  # PostgreSQL doesn't report bytes directly
                duration_ms=duration_ms,
                success=True,
                error=None
            )
            
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            record_error("lifecycle_manager", "postgres_cleanup_error", "error")
            logger.error(f"PostgreSQL cleanup failed: {e}")
            
            return CleanupResult(
                tier="postgres",
                records_deleted=0,
                bytes_reclaimed=0,
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )

    def run_minio_compaction(self) -> CompactionResult:
        """Run MinIO file compaction."""
        start_time = time.perf_counter()
        
        total_partitions = 0
        total_files_merged = 0
        total_files_deleted = 0
        total_bytes_before = 0
        total_bytes_after = 0
        errors: List[str] = []
        
        data_types = ["klines", "indicators", "alerts"]
        
        try:
            with track_latency("lifecycle_manager", "minio_compaction"):
                for data_type in data_types:
                    # Find partitions needing compaction
                    partitions = self.minio.list_partitions_to_compact(
                        data_type=data_type,
                        min_files=self.config.compaction_min_files
                    )
                    
                    for dt, symbol, date in partitions:
                        result = self.minio.compact_partition(dt, symbol, date)
                        
                        if result["success"]:
                            total_partitions += 1
                            total_files_merged += result["files_merged"]
                            total_files_deleted += result["files_deleted"]
                            total_bytes_before += result["bytes_before"]
                            total_bytes_after += result["bytes_after"]
                        else:
                            error_msg = (
                                f"Compaction failed for {dt}/{symbol}/{date}: "
                                f"{result.get('error', 'Unknown error')}"
                            )
                            errors.append(error_msg)
                            logger.warning(error_msg)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            MESSAGES_PROCESSED.labels(
                service="lifecycle_manager",
                topic="minio_compaction",
                status="success"
            ).inc(total_files_merged)
            
            success = len(errors) == 0
            
            logger.info(
                f"MinIO compaction completed: {total_partitions} partitions, "
                f"{total_files_merged} files merged, {total_files_deleted} deleted, "
                f"bytes {total_bytes_before} -> {total_bytes_after} "
                f"in {duration_ms:.2f}ms"
            )
            
            return CompactionResult(
                partitions_processed=total_partitions,
                files_merged=total_files_merged,
                files_deleted=total_files_deleted,
                bytes_before=total_bytes_before,
                bytes_after=total_bytes_after,
                duration_ms=duration_ms,
                success=success,
                errors=errors
            )
            
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            record_error("lifecycle_manager", "minio_compaction_error", "error")
            logger.error(f"MinIO compaction failed: {e}")
            
            return CompactionResult(
                partitions_processed=total_partitions,
                files_merged=total_files_merged,
                files_deleted=total_files_deleted,
                bytes_before=total_bytes_before,
                bytes_after=total_bytes_after,
                duration_ms=duration_ms,
                success=False,
                errors=errors + [str(e)]
            )

    def run_minio_retention(self) -> CleanupResult:
        """Run MinIO retention cleanup."""
        start_time = time.perf_counter()
        
        total_files_deleted = 0
        total_bytes_deleted = 0
        errors: List[str] = []
        
        data_types = ["klines", "indicators", "alerts"]
        
        try:
            with track_latency("lifecycle_manager", "minio_retention"):
                for data_type in data_types:
                    result = self.minio.delete_old_files(
                        data_type=data_type,
                        retention_days=self.config.minio_retention_days
                    )
                    
                    if result["success"]:
                        total_files_deleted += result["files_deleted"]
                        total_bytes_deleted += result["bytes_deleted"]
                    else:
                        error_msg = (
                            f"Retention cleanup failed for {data_type}: "
                            f"{result.get('error', 'Unknown error')}"
                        )
                        errors.append(error_msg)
                        logger.warning(error_msg)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            MESSAGES_PROCESSED.labels(
                service="lifecycle_manager",
                topic="minio_retention",
                status="success"
            ).inc(total_files_deleted)
            
            success = len(errors) == 0
            
            logger.info(
                f"MinIO retention cleanup completed: {total_files_deleted} files deleted, "
                f"{total_bytes_deleted} bytes reclaimed in {duration_ms:.2f}ms"
            )
            
            return CleanupResult(
                tier="minio",
                records_deleted=total_files_deleted,
                bytes_reclaimed=total_bytes_deleted,
                duration_ms=duration_ms,
                success=success,
                error="; ".join(errors) if errors else None
            )
            
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            record_error("lifecycle_manager", "minio_retention_error", "error")
            logger.error(f"MinIO retention cleanup failed: {e}")
            
            return CleanupResult(
                tier="minio",
                records_deleted=total_files_deleted,
                bytes_reclaimed=total_bytes_deleted,
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )

    def run_all(self) -> LifecycleResult:
        """Execute all lifecycle operations in sequence."""
        if not self._lock.acquire(blocking=False):
            logger.warning(
                "Lifecycle job already running, skipping this execution"
            )
            return LifecycleResult(
                postgres_cleanup=None,
                minio_compaction=None,
                minio_retention=None,
                total_duration_ms=0,
                success=False
            )
        
        self._running = True
        start_time = time.perf_counter()
        
        postgres_result: Optional[CleanupResult] = None
        compaction_result: Optional[CompactionResult] = None
        minio_retention_result: Optional[CleanupResult] = None
        
        try:
            logger.info("Starting lifecycle operations...")
            
            # Run PostgreSQL cleanup if enabled
            if "postgres" in self.config.enabled_tiers:
                logger.info("Running PostgreSQL cleanup...")
                postgres_result = self.run_postgres_cleanup()
            else:
                logger.info("PostgreSQL cleanup disabled, skipping")
            
            # Run MinIO compaction if enabled
            if "minio" in self.config.enabled_tiers:
                logger.info("Running MinIO compaction...")
                compaction_result = self.run_minio_compaction()
                
                logger.info("Running MinIO retention cleanup...")
                minio_retention_result = self.run_minio_retention()
            else:
                logger.info("MinIO operations disabled, skipping")
            
            total_duration_ms = (time.perf_counter() - start_time) * 1000
            
            # Determine overall success
            all_success = True
            if postgres_result and not postgres_result.success:
                all_success = False
            if compaction_result and not compaction_result.success:
                all_success = False
            if minio_retention_result and not minio_retention_result.success:
                all_success = False
            
            if all_success:
                logger.info(
                    f"All lifecycle operations completed successfully "
                    f"in {total_duration_ms:.2f}ms"
                )
            else:
                record_error("lifecycle_manager", "partial_failure", "warning")
                logger.warning(
                    f"Lifecycle operations completed with some failures "
                    f"in {total_duration_ms:.2f}ms"
                )
            
            return LifecycleResult(
                postgres_cleanup=postgres_result,
                minio_compaction=compaction_result,
                minio_retention=minio_retention_result,
                total_duration_ms=total_duration_ms,
                success=all_success
            )
            
        except Exception as e:
            total_duration_ms = (time.perf_counter() - start_time) * 1000
            record_error("lifecycle_manager", "run_all_error", "critical")
            logger.error(f"Lifecycle operations failed with unexpected error: {e}")
            
            return LifecycleResult(
                postgres_cleanup=postgres_result,
                minio_compaction=compaction_result,
                minio_retention=minio_retention_result,
                total_duration_ms=total_duration_ms,
                success=False
            )
            
        finally:
            self._running = False
            self._lock.release()


# Redis key for storing lifecycle status
LIFECYCLE_STATUS_KEY = "lifecycle:status"
LIFECYCLE_STATUS_TTL = 86400 * 7  # 7 days


def store_lifecycle_status(
    redis_client,
    result: LifecycleResult,
) -> bool:
    """Store lifecycle status in Redis for health check endpoint."""
    import json
    
    try:
        now = datetime.utcnow().isoformat() + "Z"
        
        status = {
            "last_run": now,
            "overall_success": result.success,
            "total_duration_ms": result.total_duration_ms,
            "tiers": []
        }
        
        # Add PostgreSQL cleanup status
        if result.postgres_cleanup:
            status["tiers"].append({
                "tier": "postgres",
                "last_run": now,
                "success": result.postgres_cleanup.success,
                "records_affected": result.postgres_cleanup.records_deleted,
                "bytes_reclaimed": result.postgres_cleanup.bytes_reclaimed,
                "error": result.postgres_cleanup.error,
            })
        
        # Add MinIO compaction status
        if result.minio_compaction:
            status["tiers"].append({
                "tier": "minio_compaction",
                "last_run": now,
                "success": result.minio_compaction.success,
                "records_affected": result.minio_compaction.files_merged,
                "bytes_reclaimed": result.minio_compaction.bytes_before - result.minio_compaction.bytes_after,
                "error": "; ".join(result.minio_compaction.errors) if result.minio_compaction.errors else None,
            })
        
        # Add MinIO retention status
        if result.minio_retention:
            status["tiers"].append({
                "tier": "minio_retention",
                "last_run": now,
                "success": result.minio_retention.success,
                "records_affected": result.minio_retention.records_deleted,
                "bytes_reclaimed": result.minio_retention.bytes_reclaimed,
                "error": result.minio_retention.error,
            })
        
        # Store in Redis
        redis_client.setex(
            LIFECYCLE_STATUS_KEY,
            LIFECYCLE_STATUS_TTL,
            json.dumps(status)
        )
        
        logger.info(f"Stored lifecycle status in Redis: success={result.success}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store lifecycle status: {e}")
        return False


def get_lifecycle_status(redis_client) -> LifecycleStatus:
    """Retrieve lifecycle status from Redis for health check endpoint."""
    import json
    
    try:
        data = redis_client.get(LIFECYCLE_STATUS_KEY)
        
        if not data:
            # No status stored yet
            return LifecycleStatus(
                last_run=None,
                overall_success=False,
                tiers=[]
            )
        
        status = json.loads(data)
        
        tiers = []
        for tier_data in status.get("tiers", []):
            tiers.append(TierStatus(
                tier=tier_data.get("tier", "unknown"),
                last_run=tier_data.get("last_run"),
                success=tier_data.get("success", False),
                records_affected=tier_data.get("records_affected", 0),
                bytes_reclaimed=tier_data.get("bytes_reclaimed", 0),
                error=tier_data.get("error"),
            ))
        
        return LifecycleStatus(
            last_run=status.get("last_run"),
            overall_success=status.get("overall_success", False),
            tiers=tiers,
        )
        
    except Exception as e:
        logger.error(f"Failed to retrieve lifecycle status: {e}")
        return LifecycleStatus(
            last_run=None,
            overall_success=False,
            tiers=[]
        )
