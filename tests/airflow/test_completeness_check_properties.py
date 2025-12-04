"""
Property-based tests for completeness check functionality.

**Feature: airflow-simple-orchestration**

These tests use Hypothesis to verify correctness properties for
data completeness checks.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import MagicMock

# Import functions to test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))
from data_quality import check_completeness


# Strategy for generating data with missing values
def generate_data_with_missing_values():
    """Generate data with null or empty values in required fields."""
    required_fields = st.lists(
        st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('Ll',))),
        min_size=1,
        max_size=5,
        unique=True
    )
    
    def create_records(fields):
        # Generate records with some null/empty values
        complete_record = st.fixed_dictionaries({
            field: st.one_of(
                st.floats(min_value=0.01, max_value=1000, allow_nan=False, allow_infinity=False),
                st.text(min_size=1, max_size=10)
            )
            for field in fields
        })
        
        incomplete_record = st.fixed_dictionaries({
            field: st.one_of(
                st.none(),
                st.just(''),
                st.floats(min_value=0.01, max_value=1000, allow_nan=False, allow_infinity=False),
                st.text(min_size=1, max_size=10)
            )
            for field in fields
        })
        
        return st.lists(st.one_of(complete_record, incomplete_record), min_size=1, max_size=20)
    
    return required_fields.flatmap(lambda fields: st.tuples(st.just(fields), create_records(fields)))


@settings(max_examples=100)
@given(data=generate_data_with_missing_values())
def test_property_completeness_check_detects_missing_values(data):
    """
    **Feature: airflow-simple-orchestration, Property 3: Completeness check detects missing values**
    **Validates: Requirements 3.3, 5.3**
    
    For any dataset and list of required fields, when completeness check is performed
    on data with null or empty values in required fields, the check should fail and
    report the incomplete records.
    """
    required_fields, records = data
    
    # Create mock context
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    # Check if data has missing values
    has_missing = False
    for record in records:
        for field in required_fields:
            value = record.get(field)
            if value is None or value == '':
                has_missing = True
                break
        if has_missing:
            break
    
    if has_missing:
        # Should raise exception for incomplete data
        with pytest.raises(Exception) as exc_info:
            check_completeness(records, required_fields, **context)
        
        # Verify error message mentions incomplete records
        assert 'incomplete' in str(exc_info.value).lower()
        
        # Verify incomplete records were logged
        assert mock_task_instance.log.error.called
    else:
        # Should succeed for complete data
        result = check_completeness(records, required_fields, **context)
        assert result['incomplete_records'] == 0
        assert result['completeness_pct'] == 100.0
        
        # Verify metrics pushed to XCom
        mock_task_instance.xcom_push.assert_called_once()


@settings(max_examples=100)
@given(
    num_records=st.integers(min_value=1, max_value=100)
)
def test_property_completeness_check_passes_for_complete_data(num_records):
    """
    **Feature: airflow-simple-orchestration, Property 3: Completeness check detects missing values**
    **Validates: Requirements 3.3, 5.3**
    
    For any complete dataset with no null or empty values, completeness check
    should pass and write metrics to XCom.
    """
    # Create mock context
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    # Generate complete data
    complete_data = [
        {
            'price': float(i + 1),
            'quantity': float(i + 1),
            'symbol': f'SYM{i}'
        }
        for i in range(num_records)
    ]
    
    required_fields = ['price', 'quantity', 'symbol']
    
    # Should succeed
    result = check_completeness(complete_data, required_fields, **context)
    
    # Verify results
    assert result['incomplete_records'] == 0
    assert result['complete_records'] == num_records
    assert result['completeness_pct'] == 100.0
    
    # Verify metrics pushed to XCom
    mock_task_instance.xcom_push.assert_called_once()
    call_args = mock_task_instance.xcom_push.call_args
    assert call_args[1]['key'] == 'completeness_metrics'
