"""
Unit tests for streaming_pipeline_dag structure.

Tests verify:
- DAG contains expected TaskGroups
- Each TaskGroup has required run tasks
- Dependencies are set correctly
- Optional test tasks work when added
"""

import pytest
import sys
import os
from pathlib import Path

# Add dags directory to path for imports
dags_path = Path(__file__).parent.parent.parent / 'dags'
sys.path.insert(0, str(dags_path))

# Set environment variables for DAG imports
os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(dags_path))
os.environ.setdefault('AIRFLOW__CORE__UNIT_TEST_MODE', 'True')

try:
    from airflow.models import DagBag
    from airflow.utils.task_group import TaskGroup
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    
    # Create mock test that documents the requirement
    def test_airflow_not_installed():
        """
        Airflow is not installed in the development environment.
        
        These tests are designed to run in the Airflow Docker container
        where Airflow is installed. To run these tests:
        
        1. Start Airflow with docker-compose up
        2. Execute tests inside the container:
           docker exec airflow-scheduler pytest /opt/airflow/tests/airflow/test_streaming_pipeline_dag.py
        
        The DAG structure can be verified manually by:
        1. Accessing Airflow UI at http://localhost:8080
        2. Checking that streaming_pipeline DAG appears
        3. Verifying TaskGroups in Graph view
        """
        pytest.skip("Airflow not installed - tests should run in Docker container")
    
    pytest.skip("Airflow not installed - tests should run in Docker container", allow_module_level=True)


class TestStreamingPipelineDAG:
    """Test suite for streaming_pipeline DAG structure."""
    
    @pytest.fixture(scope='class')
    def dagbag(self):
        """Load DAGs from dags directory."""
        return DagBag(dag_folder=str(dags_path), include_examples=False)
    
    @pytest.fixture(scope='class')
    def dag(self, dagbag):
        """Get streaming_pipeline DAG."""
        dag_id = 'streaming_pipeline'
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
        return dagbag.dags[dag_id]
    
    def test_dag_loaded(self, dagbag):
        """Test that streaming_pipeline DAG is loaded without errors."""
        assert 'streaming_pipeline' in dagbag.dags
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    def test_dag_has_correct_properties(self, dag):
        """Test DAG has correct configuration."""
        assert dag.dag_id == 'streaming_pipeline'
        assert dag.schedule_interval is None  # Manual trigger
        assert dag.catchup is False
        assert 'streaming' in dag.tags
        assert 'production' in dag.tags
    
    def test_dag_contains_task_groups(self, dag):
        """Test DAG contains expected TaskGroups."""
        # Get all task groups from the DAG
        task_groups = []
        for task in dag.tasks:
            if task.task_group and task.task_group.group_id != dag.dag_id:
                if task.task_group.group_id not in task_groups:
                    task_groups.append(task.task_group.group_id)
        
        # Verify expected task groups exist
        expected_groups = ['data_ingestion', 'trade_aggregation', 'technical_indicators']
        for group_id in expected_groups:
            assert group_id in task_groups, f"TaskGroup {group_id} not found in DAG"
    
    def test_data_ingestion_group_has_run_task(self, dag):
        """Test data_ingestion TaskGroup has run task."""
        # Find tasks in data_ingestion group
        ingestion_tasks = [
            task for task in dag.tasks
            if task.task_group and task.task_group.group_id == 'data_ingestion'
        ]
        
        assert len(ingestion_tasks) > 0, "data_ingestion TaskGroup has no tasks"
        
        # Check for run task (task_id includes group prefix like 'data_ingestion.run_binance_connector')
        task_ids = [task.task_id for task in ingestion_tasks]
        assert any('run_binance_connector' in tid for tid in task_ids), "run_binance_connector task not found"
    
    def test_trade_aggregation_group_has_run_task(self, dag):
        """Test trade_aggregation TaskGroup has run task."""
        # Find tasks in trade_aggregation group
        aggregation_tasks = [
            task for task in dag.tasks
            if task.task_group and task.task_group.group_id == 'trade_aggregation'
        ]
        
        assert len(aggregation_tasks) > 0, "trade_aggregation TaskGroup has no tasks"
        
        # Check for run task (task_id includes group prefix like 'trade_aggregation.run_trade_aggregation_job')
        task_ids = [task.task_id for task in aggregation_tasks]
        assert any('run_trade_aggregation_job' in tid for tid in task_ids), "run_trade_aggregation_job task not found"
    
    def test_technical_indicators_group_has_run_task(self, dag):
        """Test technical_indicators TaskGroup has run task."""
        # Find tasks in technical_indicators group
        indicators_tasks = [
            task for task in dag.tasks
            if task.task_group and task.task_group.group_id == 'technical_indicators'
        ]
        
        assert len(indicators_tasks) > 0, "technical_indicators TaskGroup has no tasks"
        
        # Check for run task (task_id includes group prefix like 'technical_indicators.run_technical_indicators_job')
        task_ids = [task.task_id for task in indicators_tasks]
        assert any('run_technical_indicators_job' in tid for tid in task_ids), "run_technical_indicators_job task not found"
    
    def test_dependencies_are_correct(self, dag):
        """Test dependencies between TaskGroups are set correctly."""
        # Get tasks
        run_connector = dag.get_task('data_ingestion.run_binance_connector')
        run_aggregation = dag.get_task('trade_aggregation.run_trade_aggregation_job')
        run_indicators = dag.get_task('technical_indicators.run_technical_indicators_job')
        
        # Check downstream dependencies
        # data_ingestion should be upstream of trade_aggregation
        assert run_aggregation in run_connector.get_flat_relatives(upstream=False)
        
        # trade_aggregation should be upstream of technical_indicators
        assert run_indicators in run_aggregation.get_flat_relatives(upstream=False)
        
        # Verify full chain: data_ingestion -> trade_aggregation -> technical_indicators
        all_downstream_from_ingestion = run_connector.get_flat_relatives(upstream=False)
        assert run_aggregation in all_downstream_from_ingestion
        assert run_indicators in all_downstream_from_ingestion
    
    def test_dag_has_no_cycles(self, dag):
        """Test DAG has no circular dependencies."""
        # Airflow's DagBag validation should catch cycles
        # But we can also verify by checking that no task is its own ancestor
        for task in dag.tasks:
            upstream_tasks = task.get_flat_relatives(upstream=True)
            assert task not in upstream_tasks, f"Task {task.task_id} has circular dependency"
    
    def test_default_args_configured(self, dag):
        """Test DAG has proper default_args."""
        assert dag.default_args is not None
        assert 'owner' in dag.default_args
        assert dag.default_args['owner'] == 'data-engineering'
        assert 'retries' in dag.default_args
        assert dag.default_args['retries'] == 2
