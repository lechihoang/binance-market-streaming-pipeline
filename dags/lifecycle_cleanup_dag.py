"""
Lifecycle Cleanup DAG

Orchestrates automated cleanup, compaction, and retention operations for the
multi-tier storage system (PostgreSQL, MinIO).

Schedule: Daily at 3AM UTC
Operations:
1. PostgreSQL cleanup - Delete records older than 90 days
2. MinIO compaction - Merge fragmented batch Parquet files
3. MinIO retention - Delete files older than 365 days

Requirements: 4.2
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to path so 'from src.xxx' imports work
sys.path.insert(0, '/opt/airflow')

from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.lifecycle import LifecycleManager, LifecycleConfig, store_lifecycle_status
from src.utils.logging import get_logger

import redis

logger = get_logger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Storage config from environment
postgres_host = os.getenv('POSTGRES_HOST', 'postgres-data')
postgres_port = int(os.getenv('POSTGRES_PORT', '5432'))
postgres_user = os.getenv('POSTGRES_USER', 'crypto')
postgres_password = os.getenv('POSTGRES_PASSWORD', 'crypto')
postgres_db = os.getenv('POSTGRES_DB', 'crypto_data')

minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
minio_bucket = os.getenv('MINIO_BUCKET', 'crypto-data')
minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

# Lifecycle config from environment
postgres_retention_days = int(os.getenv('POSTGRES_RETENTION_DAYS', '90'))
minio_retention_days = int(os.getenv('MINIO_RETENTION_DAYS', '365'))
compaction_min_files = int(os.getenv('COMPACTION_MIN_FILES', '2'))
cleanup_batch_size = int(os.getenv('CLEANUP_BATCH_SIZE', '1000'))

# Redis config for storing lifecycle status
redis_host = os.getenv('REDIS_HOST', 'redis')
redis_port = int(os.getenv('REDIS_PORT', '6379'))


def run_lifecycle_cleanup(**context):
    """Execute all lifecycle cleanup operations.
    
    Initializes storage connections and runs the LifecycleManager.run_all()
    method to perform PostgreSQL cleanup, MinIO compaction, and MinIO retention.
    
    Requirements: 4.2
    """
    logger.info("Starting lifecycle cleanup operations...")
    
    # Initialize storage connections
    postgres = PostgresStorage(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        database=postgres_db,
    )
    
    minio = MinioStorage(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        bucket=minio_bucket,
        secure=minio_secure,
    )
    
    # Initialize lifecycle config
    config = LifecycleConfig(
        postgres_retention_days=postgres_retention_days,
        minio_retention_days=minio_retention_days,
        compaction_min_files=compaction_min_files,
        cleanup_batch_size=cleanup_batch_size,
    )
    
    # Initialize and run lifecycle manager
    manager = LifecycleManager(
        postgres=postgres,
        minio=minio,
        config=config,
    )
    
    result = manager.run_all()
    
    # Log results
    logger.info(f"Lifecycle cleanup completed in {result.total_duration_ms:.2f}ms")
    logger.info(f"Overall success: {result.success}")
    
    if result.postgres_cleanup:
        logger.info(
            f"PostgreSQL cleanup: {result.postgres_cleanup.records_deleted} records deleted, "
            f"success={result.postgres_cleanup.success}"
        )
        if result.postgres_cleanup.error:
            logger.error(f"PostgreSQL cleanup error: {result.postgres_cleanup.error}")
    
    if result.minio_compaction:
        logger.info(
            f"MinIO compaction: {result.minio_compaction.partitions_processed} partitions, "
            f"{result.minio_compaction.files_merged} files merged, "
            f"success={result.minio_compaction.success}"
        )
        if result.minio_compaction.errors:
            for error in result.minio_compaction.errors:
                logger.error(f"MinIO compaction error: {error}")
    
    if result.minio_retention:
        logger.info(
            f"MinIO retention: {result.minio_retention.records_deleted} files deleted, "
            f"{result.minio_retention.bytes_reclaimed} bytes reclaimed, "
            f"success={result.minio_retention.success}"
        )
        if result.minio_retention.error:
            logger.error(f"MinIO retention error: {result.minio_retention.error}")
    
    # Push result to XCom for downstream tasks or monitoring
    context['ti'].xcom_push(key='lifecycle_result', value={
        'success': result.success,
        'total_duration_ms': result.total_duration_ms,
        'postgres_records_deleted': result.postgres_cleanup.records_deleted if result.postgres_cleanup else 0,
        'minio_files_merged': result.minio_compaction.files_merged if result.minio_compaction else 0,
        'minio_files_deleted': result.minio_retention.records_deleted if result.minio_retention else 0,
        'minio_bytes_reclaimed': result.minio_retention.bytes_reclaimed if result.minio_retention else 0,
    })
    
    # Store lifecycle status in Redis for health check endpoint (Requirement 5.3)
    try:
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        store_lifecycle_status(redis_client, result)
    except Exception as e:
        logger.warning(f"Failed to store lifecycle status in Redis: {e}")
    
    if not result.success:
        raise Exception("Lifecycle cleanup completed with failures")
    
    return result.success


with DAG(
    dag_id='lifecycle_cleanup_dag',
    default_args=default_args,
    description='Automated cleanup, compaction, and retention for storage tiers',
    schedule_interval='0 3 * * *',  # Run daily at 3AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs (Requirement 4.3)
    tags=['lifecycle', 'cleanup', 'maintenance'],
) as dag:
    
    run_cleanup = PythonOperator(
        task_id='run_lifecycle_cleanup',
        python_callable=run_lifecycle_cleanup,
        provide_context=True,
    )
