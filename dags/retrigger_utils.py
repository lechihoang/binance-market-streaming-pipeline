"""
Retrigger Utilities

Utilities for checking task states and deciding whether to retrigger DAG.
"""

import logging
import time
import os
from airflow.utils.state import State


# Critical tasks that must succeed for retrigger
CRITICAL_TASKS = [
    'run_trade_aggregation_job',
    'run_technical_indicators_job',
    'run_anomaly_detection_job'
]


def check_and_wait_before_retrigger(**context):
    """
    Check if all critical tasks succeeded, then wait before retrigger.
    
    This function:
    1. Checks state of all critical processing tasks
    2. If any failed/upstream_failed, raises exception to stop retrigger
    3. If all succeeded, waits for configured interval
    
    Args:
        context: Airflow context with dag_run info
        
    Raises:
        Exception: If any critical task did not succeed
    """
    dag_run = context['dag_run']
    
    # Check all critical tasks
    failed_tasks = []
    for task_id in CRITICAL_TASKS:
        ti = dag_run.get_task_instance(task_id)
        if ti is None:
            logging.warning(f"Task {task_id} not found in DAG run")
            failed_tasks.append((task_id, 'not_found'))
        elif ti.state != State.SUCCESS:
            logging.warning(f"Task {task_id} is in state: {ti.state}")
            failed_tasks.append((task_id, ti.state))
    
    # If any critical task failed, stop retrigger
    if failed_tasks:
        failed_info = ', '.join([f"{t}={s}" for t, s in failed_tasks])
        logging.error(
            f"Stopping retrigger: Critical tasks did not succeed: {failed_info}. "
            f"DAG will stop here and NOT retrigger."
        )
        raise Exception(f"Retrigger blocked: {failed_info}")
    
    # All critical tasks succeeded, proceed with wait
    logging.info("All critical tasks succeeded, proceeding to wait before retrigger")
    
    interval = int(os.getenv('RETRIGGER_INTERVAL_SECONDS', '300'))
    logging.info(f"Waiting {interval} seconds before retriggering DAG")
    time.sleep(interval)
    logging.info("Wait complete, proceeding to retrigger")
