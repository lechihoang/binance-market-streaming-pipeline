"""
Streaming Processing DAG

Orchestrates the Spark streaming jobs that process trade data from Kafka.
This DAG runs independently from the Binance Connector DAG.

Task Flow:
1. Health checks (parallel): test_redis_health, test_duckdb_health, test_parquet_path
2. run_trade_aggregation_job: Aggregate trades into OHLCV candles (writes to processed_aggregations topic)
3. run_technical_indicators_job: Calculate RSI, MACD, BB indicators (writes to processed_indicators topic)
4. run_anomaly_detection_job: Detect whale, volume, price anomalies (reads from processed_indicators topic)
5. cleanup_streaming: Cleanup resources when jobs finish or fail
6. wait_before_retrigger: Check if all jobs succeeded before retriggering
7. retrigger_dag: Retrigger the DAG for continuous processing

Note: anomaly_detection_job runs AFTER technical_indicators_job because it depends on
the processed_indicators Kafka topic which is created by technical_indicators_job.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import logging
import time

from data_quality import (
    health_check_service,
    on_failure_callback,
)
from cleanup_utils import cleanup_streaming_resources
from retrigger_utils import check_and_wait_before_retrigger

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': on_failure_callback,
}

# Redis config
redis_host = os.getenv('REDIS_HOST', 'redis')
redis_port = int(os.getenv('REDIS_PORT', '6379'))


def check_duckdb_path(**context):
    """Check DuckDB database directory is accessible and writable."""
    db_path = os.getenv('DUCKDB_PATH', '/opt/airflow/data/streaming.duckdb')
    db_dir = os.path.dirname(db_path)
    
    # Create directory if not exists
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    # Check directory is writable (don't open DuckDB to avoid lock conflicts)
    try:
        test_file = os.path.join(db_dir, '.duckdb_write_test')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        return {'status': 'healthy', 'path': db_path, 'dir': db_dir}
    except Exception as e:
        raise Exception(f"DuckDB path check failed: {e}")


def check_parquet_path(**context):
    """Check Parquet output directory is writable."""
    parquet_path = os.getenv('PARQUET_PATH', '/opt/airflow/data/parquet')
    
    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path, exist_ok=True)
    
    # Check writable
    test_file = os.path.join(parquet_path, '.write_test')
    try:
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        return {'status': 'healthy', 'path': parquet_path}
    except Exception as e:
        raise Exception(f"Parquet path check failed: {e}")


# wait_before_retrigger moved to retrigger_utils.py as check_and_wait_before_retrigger


with DAG(
    dag_id='streaming_processing_dag',
    default_args=default_args,
    description='Spark streaming jobs for processing trade data from Kafka',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs (Requirements: 2.3)
    tags=['streaming', 'spark', 'processing'],
) as dag:
    
    # Task 1: Test Redis health (parallel with other health checks)
    test_redis_health = PythonOperator(
        task_id='test_redis_health',
        python_callable=health_check_service,
        op_kwargs={
            'service_name': 'redis',
            'host': redis_host,
            'port': redis_port,
            'max_retries': 3,
        },
    )
    
    # Task 2: Test DuckDB health (parallel with other health checks)
    test_duckdb_health = PythonOperator(
        task_id='test_duckdb_health',
        python_callable=check_duckdb_path,
    )
    
    # Task 3: Test Parquet path (parallel with other health checks)
    test_parquet_path = PythonOperator(
        task_id='test_parquet_path',
        python_callable=check_parquet_path,
    )
    
    # Task 4: Run trade aggregation job (depends on all health checks)
    run_trade_aggregation_job = BashOperator(
        task_id='run_trade_aggregation_job',
        bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m pyspark_streaming_processor.trade_aggregation_job',
        cwd='/opt/airflow',
        env={
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'REDIS_HOST': redis_host,
            'REDIS_PORT': str(redis_port),
            'DUCKDB_PATH': os.getenv('DUCKDB_PATH', '/opt/airflow/data/streaming.duckdb'),
            'PARQUET_PATH': os.getenv('PARQUET_PATH', '/opt/airflow/data/parquet'),
            # PostgreSQL Data Storage (Warm Path)
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres-data'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'crypto'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'crypto'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'crypto_data'),
            # MinIO Object Storage (Cold Path)
            'MINIO_ENDPOINT': os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            'MINIO_ACCESS_KEY': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'MINIO_BUCKET': os.getenv('MINIO_BUCKET', 'crypto-data'),
            'MINIO_SECURE': os.getenv('MINIO_SECURE', 'false'),
        },
    )
    
    # Task 5: Run technical indicators job (must complete before anomaly detection)
    # This job writes to processed_indicators topic which anomaly detection reads from
    run_technical_indicators_job = BashOperator(
        task_id='run_technical_indicators_job',
        bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m pyspark_streaming_processor.technical_indicators_job',
        cwd='/opt/airflow',
        env={
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'REDIS_HOST': redis_host,
            'REDIS_PORT': str(redis_port),
            'DUCKDB_PATH': os.getenv('DUCKDB_PATH', '/opt/airflow/data/streaming.duckdb'),
            'PARQUET_PATH': os.getenv('PARQUET_PATH', '/opt/airflow/data/parquet'),
            # PostgreSQL Data Storage (Warm Path)
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres-data'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'crypto'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'crypto'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'crypto_data'),
            # MinIO Object Storage (Cold Path)
            'MINIO_ENDPOINT': os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            'MINIO_ACCESS_KEY': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'MINIO_BUCKET': os.getenv('MINIO_BUCKET', 'crypto-data'),
            'MINIO_SECURE': os.getenv('MINIO_SECURE', 'false'),
        },
    )
    
    # Task 6: Run anomaly detection job (depends on technical indicators job)
    # Reads from processed_indicators topic created by technical_indicators_job
    run_anomaly_detection_job = BashOperator(
        task_id='run_anomaly_detection_job',
        bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m pyspark_streaming_processor.anomaly_detection_job',
        cwd='/opt/airflow',
        env={
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'REDIS_HOST': redis_host,
            'REDIS_PORT': str(redis_port),
            'DUCKDB_PATH': os.getenv('DUCKDB_PATH', '/opt/airflow/data/streaming.duckdb'),
            'PARQUET_PATH': os.getenv('PARQUET_PATH', '/opt/airflow/data/parquet'),
            # PostgreSQL Data Storage (Warm Path)
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres-data'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'crypto'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'crypto'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'crypto_data'),
            # MinIO Object Storage (Cold Path)
            'MINIO_ENDPOINT': os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            'MINIO_ACCESS_KEY': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            'MINIO_BUCKET': os.getenv('MINIO_BUCKET', 'crypto-data'),
            'MINIO_SECURE': os.getenv('MINIO_SECURE', 'false'),
        },
    )
    
    # Task 7: Cleanup streaming resources
    cleanup_streaming_task = PythonOperator(
        task_id='cleanup_streaming',
        python_callable=cleanup_streaming_resources,
        op_kwargs={
            'redis_host': redis_host,
            'redis_port': redis_port,
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream status
    )
    
    # Task 8: Check critical tasks and wait before retrigger (Requirements: 2.1)
    # This task checks if all critical processing jobs succeeded.
    # If any failed, it raises exception and DAG stops (no retrigger).
    # If all succeeded, it waits for configured interval then allows retrigger.
    wait_task = PythonOperator(
        task_id='wait_before_retrigger',
        python_callable=check_and_wait_before_retrigger,
        trigger_rule=TriggerRule.ALL_DONE,  # Always run to check state
    )
    
    # Task 9: Retrigger DAG (Requirements: 2.2, 2.4)
    retrigger_task = TriggerDagRunOperator(
        task_id='retrigger_dag',
        trigger_dag_id='streaming_processing_dag',
        conf={'triggered_at': '{{ ts }}'},
        wait_for_completion=False,
    )
    
    # Set dependencies:
    # All health checks run in parallel, then trade aggregation
    # Then technical indicators runs, followed by anomaly detection
    # (anomaly detection depends on processed_indicators topic created by technical_indicators_job)
    # Cleanup runs after all jobs complete (regardless of status)
    # Wait and retrigger ONLY run if all processing jobs succeeded
    # If any task failed, DAG stops after cleanup (no retrigger)
    [test_redis_health, test_duckdb_health, test_parquet_path] >> run_trade_aggregation_job
    run_trade_aggregation_job >> run_technical_indicators_job >> run_anomaly_detection_job
    run_anomaly_detection_job >> cleanup_streaming_task
    cleanup_streaming_task >> wait_task >> retrigger_task
