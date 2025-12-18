"""
Base Spark Job Module.

Abstract base class for all Spark streaming jobs, providing common functionality:
- Logging setup with StructuredFormatter
- Signal handling for graceful shutdown
- SparkSession creation with consistent configuration
- Memory monitoring and batch metrics
- StorageWriter initialization for 3-tier storage
- Cleanup logic

Requirements Coverage:
---------------------
    - Requirement 1.1: BaseSparkJob class to inherit from
    - Requirement 1.2: Logging setup with StructuredFormatter
    - Requirement 1.3: Signal handlers for SIGINT and SIGTERM
    - Requirement 1.4: SparkSession with consistent configuration
    - Requirement 2.1: JVM memory metrics via _log_memory_metrics()
    - Requirement 2.2: Batch metrics via _log_batch_metrics()
    - Requirement 2.3: High memory usage warning
    - Requirement 3.1: Graceful shutdown with shutdown_requested flag
    - Requirement 3.2: should_stop() checks shutdown, timeout, empty batches
    - Requirement 3.3: _cleanup() stops query and SparkSession
    - Requirement 4.1: StorageWriter initialization via _init_storage_writer()
    - Requirement 4.2: Configuration from Config object
    - Requirement 6.1: Abstract run() method for subclasses
"""

import logging
import signal
import sys
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from src.utils.config import Config
from src.utils.logging import StructuredFormatter
from src.utils.shutdown import GracefulShutdown
from src.storage.redis import RedisStorage
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.storage_writer import StorageWriter


class BaseSparkJob(ABC):
    """
    Abstract base class for all Spark streaming jobs.
    
    Provides common functionality for:
    - Logging setup with StructuredFormatter
    - Signal handling for graceful shutdown
    - SparkSession creation with consistent configuration
    - Memory monitoring and batch metrics
    - StorageWriter initialization for 3-tier storage
    - Cleanup logic
    
    Subclasses must implement the run() method with job-specific logic.
    """
    
    # Default thresholds (can be overridden by subclasses)
    DEFAULT_EMPTY_BATCH_THRESHOLD = 3
    DEFAULT_MAX_RUNTIME_SECONDS = 180
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT = 60
    
    def __init__(self, config: Config, job_name: str):
        """
        Initialize base job with common setup.
        
        Args:
            config: Configuration object with Kafka, Spark, Redis, PostgreSQL, MinIO settings
            job_name: Name of the job for logging and identification
        """
        self.config = config
        self.job_name = job_name
        self.logger = self._setup_logging()
        self.spark: Optional[SparkSession] = None
        self.query: Optional[Any] = None
        self.shutdown_requested = False
        self.storage_writer: Optional[StorageWriter] = None
        
        # Micro-batch auto-stop attributes
        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = self.DEFAULT_EMPTY_BATCH_THRESHOLD
        self.max_runtime_seconds: int = self.DEFAULT_MAX_RUNTIME_SECONDS
        self.start_time: Optional[float] = None
        
        # Initialize GracefulShutdown for controlled shutdown behavior
        self.graceful_shutdown = GracefulShutdown(
            graceful_shutdown_timeout=self.DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
            shutdown_progress_interval=10,
            logger=self.logger
        )
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logging(self) -> logging.Logger:
        """
        Set up structured logging for this job.
        
        Creates a logger with StructuredFormatter using the job_name.
        Sets log level from config.
        
        Returns:
            Configured logger instance
            
        Requirements: 1.2
        """
        numeric_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        # Create handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(numeric_level)
        
        # Set formatter with job name
        formatter = StructuredFormatter(self.job_name)
        handler.setFormatter(formatter)
        
        # Get logger for this module with job-specific name
        logger = logging.getLogger(f"{__name__}.{self.job_name}")
        logger.setLevel(numeric_level)
        logger.handlers.clear()
        logger.addHandler(handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals with graceful shutdown support.
        
        IMPORTANT: We do NOT call query.stop() here immediately.
        Instead, we set a flag and let the current batch complete naturally.
        The query will be stopped after the batch finishes and checkpoint is committed.
        This prevents offset loss due to incomplete checkpoint writes.
        
        Args:
            signum: Signal number received
            frame: Current stack frame
            
        Requirements: 3.1
        """
        # Delegate to GracefulShutdown for proper shutdown handling
        self.graceful_shutdown.request_shutdown(signum)
        self.shutdown_requested = True
        
        # NOTE: Do NOT call query.stop() here!
        # Let the current batch complete and checkpoint commit.
        # The should_stop() method will stop the query after the batch completes.
        self.logger.info(
            "Shutdown signal received - will stop after current batch completes "
            "and checkpoint commits"
        )
    
    def should_stop(self, is_empty_batch: bool) -> bool:
        """
        Check if job should stop based on shutdown signal, empty batch count or timeout.
        
        IMPORTANT: This is called INSIDE foreachBatch, so returning True here
        and calling query.stop() will allow Spark to commit the checkpoint
        for the current batch before stopping.
        
        Args:
            is_empty_batch: Whether the current batch is empty
            
        Returns:
            True if job should stop, False otherwise
            
        Requirements: 3.2
        """
        # Check if shutdown was requested via signal (SIGTERM/SIGINT)
        if self.shutdown_requested:
            self.logger.info("Shutdown signal detected, stopping after this batch completes")
            return True
        
        # Check timeout
        if self.start_time and (time.time() - self.start_time) > self.max_runtime_seconds:
            self.logger.info(f"Max runtime {self.max_runtime_seconds}s exceeded, stopping")
            return True
        
        # Check empty batches
        if is_empty_batch:
            self.empty_batch_count += 1
            self.logger.info(
                f"Empty batch detected, count: {self.empty_batch_count}/"
                f"{self.empty_batch_threshold}"
            )
            if self.empty_batch_count >= self.empty_batch_threshold:
                self.logger.info(
                    f"{self.empty_batch_threshold} consecutive empty batches, stopping"
                )
                return True
        else:
            # Reset counter when data is received
            if self.empty_batch_count > 0:
                self.logger.info(
                    f"Non-empty batch received, resetting empty batch counter "
                    f"from {self.empty_batch_count} to 0"
                )
            self.empty_batch_count = 0
        
        return False

    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure SparkSession with resource limits.
        
        Memory config: executor=512m, driver=512m (Spark minimum ~450MB)
        
        Returns:
            Configured SparkSession
            
        Requirements: 1.4
        """
        # Spark requires minimum ~450MB for driver memory
        executor_memory = "512m"
        driver_memory = "512m"
        
        self.logger.info("Creating SparkSession with configuration:")
        self.logger.info(f"  Executor memory: {executor_memory}")
        self.logger.info(f"  Driver memory: {driver_memory}")
        self.logger.info(f"  Executor cores: {self.config.spark.executor_cores}")
        self.logger.info(f"  Shuffle partitions: {self.config.spark.shuffle_partitions}")
        
        spark = (SparkSession.builder
                 .appName(self.config.spark.app_name)
                 .config("spark.jars.packages", 
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                 .config("spark.sql.caseSensitive", "true")
                 .config("spark.executor.memory", executor_memory)
                 .config("spark.driver.memory", driver_memory)
                 .config("spark.executor.cores", str(self.config.spark.executor_cores))
                 .config("spark.sql.shuffle.partitions", 
                        str(self.config.spark.shuffle_partitions))
                 .config("spark.streaming.kafka.maxRatePerPartition",
                        str(self.config.kafka.max_rate_per_partition))
                 .config("spark.streaming.backpressure.enabled",
                        str(self.config.spark.backpressure_enabled).lower())
                 .config("spark.sql.streaming.checkpointLocation",
                        self.config.spark.checkpoint_location)
                 # State store reliability settings
                 .config("spark.sql.streaming.minBatchesToRetain",
                        str(self.config.spark.state_store_min_batches_to_retain))
                 .config("spark.sql.streaming.stateStore.maintenanceInterval",
                        self.config.spark.state_store_maintenance_interval)
                 .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "5")
                 .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
                 .getOrCreate())
        
        # Set log level
        spark.sparkContext.setLogLevel(self.config.log_level)
        
        self.logger.info(f"SparkSession created: {spark.sparkContext.applicationId}")
        self.logger.info(f"Checkpoint location: {self.config.spark.checkpoint_location}")
        
        return spark
    
    def _get_jvm_memory_metrics(self) -> Dict[str, Any]:
        """
        Get JVM memory metrics from Spark.
        
        Returns:
            Dictionary with memory metrics in MB
            
        Requirements: 2.1
        """
        try:
            from pyspark import SparkContext
            
            sc = SparkContext.getOrCreate()
            jvm = sc._jvm
            runtime = jvm.java.lang.Runtime.getRuntime()
            
            max_memory = runtime.maxMemory() / (1024 * 1024)
            total_memory = runtime.totalMemory() / (1024 * 1024)
            free_memory = runtime.freeMemory() / (1024 * 1024)
            used_memory = total_memory - free_memory
            
            usage_pct = (used_memory / max_memory) * 100 if max_memory > 0 else 0
            
            return {
                "heap_used_mb": round(used_memory, 2),
                "heap_max_mb": round(max_memory, 2),
                "heap_usage_pct": round(usage_pct, 2),
                "heap_free_mb": round(free_memory, 2),
                "heap_total_mb": round(total_memory, 2),
            }
        except Exception as e:
            return {
                "heap_used_mb": 0,
                "heap_max_mb": 0,
                "heap_usage_pct": 0,
                "heap_free_mb": 0,
                "heap_total_mb": 0,
                "error": str(e)
            }
    
    def _log_memory_metrics(
        self, 
        batch_id: Optional[int] = None, 
        alert_threshold_pct: float = 80.0
    ) -> None:
        """
        Log JVM memory metrics and alert if usage is high.
        
        Args:
            batch_id: Optional batch identifier
            alert_threshold_pct: Percentage threshold for high memory alert
            
        Requirements: 2.1, 2.3
        """
        metrics = self._get_jvm_memory_metrics()
        
        if "error" in metrics:
            self.logger.warning(f"Failed to get memory metrics: {metrics['error']}")
            return
        
        batch_str = f"Batch {batch_id}: " if batch_id is not None else ""
        
        self.logger.info(
            f"{batch_str}Memory usage: "
            f"{metrics['heap_used_mb']:.0f}MB / {metrics['heap_max_mb']:.0f}MB "
            f"({metrics['heap_usage_pct']:.1f}%), "
            f"free={metrics['heap_free_mb']:.0f}MB"
        )
        
        if metrics['heap_usage_pct'] >= alert_threshold_pct:
            self.logger.warning(
                f"{batch_str}HIGH MEMORY USAGE ALERT: "
                f"{metrics['heap_usage_pct']:.1f}% "
                f"(threshold: {alert_threshold_pct}%) - "
                f"Used: {metrics['heap_used_mb']:.0f}MB / {metrics['heap_max_mb']:.0f}MB"
            )
    
    def _log_batch_metrics(
        self,
        batch_id: int,
        duration_seconds: float,
        record_count: int,
        watermark: Optional[str] = None
    ) -> None:
        """
        Log batch processing metrics.
        
        Args:
            batch_id: Batch identifier
            duration_seconds: Processing duration in seconds
            record_count: Number of records processed
            watermark: Current watermark timestamp
            
        Requirements: 2.2
        """
        watermark_str = f", watermark={watermark}" if watermark else ""
        self.logger.info(
            f"Batch {batch_id} completed: "
            f"duration={duration_seconds:.2f}s, "
            f"records={record_count}{watermark_str}"
        )

    def _init_storage_writer(self) -> StorageWriter:
        """
        Initialize StorageWriter with all 3 storage tiers.
        
        Uses PostgresStorage for warm path (concurrent write support)
        and MinioStorage for cold path (S3-compatible object storage).
        
        Returns:
            Configured StorageWriter instance
            
        Requirements: 4.1, 4.2
        """
        self.logger.info(
            "Initializing StorageWriter with 3-tier storage (Redis, PostgreSQL, MinIO)"
        )
        
        # Initialize Redis storage (hot path)
        redis_storage = RedisStorage(
            host=self.config.redis.host,
            port=self.config.redis.port,
            db=self.config.redis.db
        )
        
        # Initialize PostgreSQL storage (warm path - supports concurrent writes)
        postgres_storage = PostgresStorage(
            host=self.config.postgres.host,
            port=self.config.postgres.port,
            user=self.config.postgres.user,
            password=self.config.postgres.password,
            database=self.config.postgres.database,
            max_retries=self.config.postgres.max_retries,
            retry_delay=self.config.postgres.retry_delay
        )
        
        # Initialize MinIO storage (cold path - S3-compatible object storage)
        minio_storage = MinioStorage(
            endpoint=self.config.minio.endpoint,
            access_key=self.config.minio.access_key,
            secret_key=self.config.minio.secret_key,
            bucket=self.config.minio.bucket,
            secure=self.config.minio.secure,
            max_retries=self.config.minio.max_retries
        )
        
        storage_writer = StorageWriter(
            redis=redis_storage,
            postgres=postgres_storage,
            minio=minio_storage
        )
        
        self.logger.info("StorageWriter initialized successfully with PostgreSQL and MinIO")
        return storage_writer
    
    def _cleanup(self) -> None:
        """
        Clean up resources on shutdown.
        
        Stops the streaming query and SparkSession.
        
        Requirements: 3.3
        """
        self.logger.info("Starting cleanup...")
        
        if self.query and self.query.isActive:
            self.logger.info("Stopping streaming query...")
            self.query.stop()
        
        if self.spark:
            self.logger.info("Stopping SparkSession...")
            self.spark.stop()
        
        self.logger.info("Cleanup completed. Shutdown successful.")
    
    @abstractmethod
    def run(self) -> None:
        """
        Run the streaming job.
        
        Must be implemented by subclasses with job-specific logic.
        
        Requirements: 6.1
        """
        pass
