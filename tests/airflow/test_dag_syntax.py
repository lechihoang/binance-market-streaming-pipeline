"""
Syntax and structure tests for DAG files.

These tests verify DAG files can be imported and have correct structure
without requiring Airflow to be installed.
"""

import pytest
import ast
from pathlib import Path


class TestStreamingPipelineDAGSyntax:
    """Test streaming_pipeline_dag.py syntax and structure."""
    
    @pytest.fixture
    def dag_file_path(self):
        """Get path to streaming_pipeline_dag.py."""
        return Path(__file__).parent.parent.parent / 'dags' / 'streaming_pipeline_dag.py'
    
    @pytest.fixture
    def dag_ast(self, dag_file_path):
        """Parse DAG file into AST."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        return ast.parse(content)
    
    def test_dag_file_exists(self, dag_file_path):
        """Test that streaming_pipeline_dag.py exists."""
        assert dag_file_path.exists(), "streaming_pipeline_dag.py not found"
    
    def test_dag_file_has_valid_syntax(self, dag_file_path):
        """Test DAG file has valid Python syntax."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        try:
            ast.parse(content)
        except SyntaxError as e:
            pytest.fail(f"DAG file has syntax error: {e}")
    
    def test_dag_imports_required_modules(self, dag_ast):
        """Test DAG imports required Airflow modules."""
        imports = []
        for node in ast.walk(dag_ast):
            if isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        # Check for required imports
        assert 'airflow' in imports or any('airflow' in imp for imp in imports), \
            "DAG must import from airflow"
        assert any('operators' in imp for imp in imports), \
            "DAG must import operators"
    
    def test_dag_defines_default_args(self, dag_ast):
        """Test DAG defines default_args dictionary."""
        has_default_args = False
        
        for node in ast.walk(dag_ast):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'default_args':
                        has_default_args = True
                        break
        
        assert has_default_args, "DAG must define default_args"
    
    def test_dag_has_task_groups(self, dag_file_path):
        """Test DAG file contains TaskGroup definitions."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Check for TaskGroup usage
        assert 'TaskGroup' in content, "DAG must use TaskGroup"
        assert 'data_ingestion' in content, "DAG must have data_ingestion TaskGroup"
        assert 'trade_aggregation' in content, "DAG must have trade_aggregation TaskGroup"
        assert 'technical_indicators' in content, "DAG must have technical_indicators TaskGroup"
    
    def test_dag_has_run_tasks(self, dag_file_path):
        """Test DAG has run tasks for each TaskGroup."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Check for run tasks
        assert 'run_binance_connector' in content, "data_ingestion must have run task"
        assert 'run_trade_aggregation' in content, "trade_aggregation must have run task"
        assert 'run_technical_indicators' in content, "technical_indicators must have run task"
    
    def test_dag_sets_dependencies(self, dag_file_path):
        """Test DAG sets dependencies between TaskGroups."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Check for dependency operators
        assert '>>' in content, "DAG must set dependencies using >> operator"
        
        # Check for specific dependency chain (may be on multiple lines)
        has_ingestion_to_aggregation = 'data_ingestion_group' in content and 'trade_aggregation_group' in content
        has_aggregation_to_downstream = 'trade_aggregation_group' in content and (
            'technical_indicators_group' in content or 'anomaly_detection_group' in content
        )
        
        assert has_ingestion_to_aggregation, \
            "DAG must set dependency: data_ingestion_group >> trade_aggregation_group"
        assert has_aggregation_to_downstream, \
            "DAG must set dependency from trade_aggregation_group to downstream groups"
    
    def test_dag_has_correct_dag_id(self, dag_file_path):
        """Test DAG has correct dag_id."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert "dag_id='streaming_pipeline'" in content or 'dag_id="streaming_pipeline"' in content, \
            "DAG must have dag_id='streaming_pipeline'"
    
    def test_dag_has_manual_schedule(self, dag_file_path):
        """Test DAG is configured for manual triggering."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'schedule_interval=None' in content, \
            "DAG must have schedule_interval=None for manual triggering"
    
    def test_dag_has_tags(self, dag_file_path):
        """Test DAG has appropriate tags."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'tags=' in content, "DAG must have tags"
        assert 'streaming' in content, "DAG must have 'streaming' tag"
