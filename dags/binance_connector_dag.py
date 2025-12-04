"""
Binance Connector DAG

Orchestrates the Binance WebSocket connector that ingests trade data into Kafka.
This DAG runs independently from the streaming processing DAG.

Tasks:
- test_kafka_health: Check Kafka connectivity before starting connector
- run_binance_connector: Run Binance WebSocket connector
- cleanup_connector: Cleanup resources when connector finishes or fails
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import logging

from data_quality import (
    health_check_service,
    on_failure_callback,
)
from cleanup_utils import cleanup_connector_resources

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': on_failure_callback,
}


with DAG(
    dag_id='binance_connector_dag',
    default_args=default_args,
    description='Binance WebSocket connector for ingesting trade data into Kafka',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'binance', 'connector'],
) as dag:
    
    # Task 1: Test Kafka health before starting connector
    test_kafka_health = PythonOperator(
        task_id='test_kafka_health',
        python_callable=health_check_service,
        op_kwargs={
            'service_name': 'kafka',
            'host': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(':')[0],
            'port': int(os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(':')[1]),
            'max_retries': 3,
        },
    )
    
    # Task 2: Run Binance connector
    run_binance_connector = BashOperator(
        task_id='run_binance_connector',
        bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m binance_kafka_connector',
        cwd='/opt/airflow',
        env={
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'BINANCE_STREAMS': os.getenv('BINANCE_STREAMS', 'btcusdt@trade,ethusdt@trade,bnbusdt@trade,solusdt@trade'),
        },
    )
    
    # Task 3: Cleanup connector resources
    cleanup_connector_task = PythonOperator(
        task_id='cleanup_connector',
        python_callable=cleanup_connector_resources,
        trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream status
    )
    
    # Set dependencies
    test_kafka_health >> run_binance_connector >> cleanup_connector_task
