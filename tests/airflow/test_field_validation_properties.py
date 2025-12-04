"""
Property-based tests for field validation functionality.

**Feature: airflow-simple-orchestration**

These tests use Hypothesis to verify correctness properties for
field validation against schema.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import MagicMock

# Import functions to test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))
from data_quality import validate_fields


# Strategy for generating data with schema violations
def generate_data_with_violations():
    """Generate data that violates schema constraints."""
    schema = st.fixed_dictionaries({
        'price': st.just(float),
        'quantity': st.just(float),
        'symbol': st.just(str)
    })
    
    # Generate records with violations
    valid_record = st.fixed_dictionaries({
        'price': st.floats(min_value=0.01, max_value=100000, allow_nan=False, allow_infinity=False),
        'quantity': st.floats(min_value=0.01, max_value=1000, allow_nan=False, allow_infinity=False),
        'symbol': st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('Lu',)))
    })
    
    # Record with missing field
    missing_field_record = st.fixed_dictionaries({
        'price': st.floats(min_value=0.01, max_value=100000, allow_nan=False, allow_infinity=False),
        'quantity': st.floats(min_value=0.01, max_value=1000, allow_nan=False, allow_infinity=False)
        # 'symbol' is missing
    })
    
    # Record with wrong type
    wrong_type_record = st.fixed_dictionaries({
        'price': st.text(min_size=1, max_size=5),  # Should be float
        'quantity': st.floats(min_value=0.01, max_value=1000, allow_nan=False, allow_infinity=False),
        'symbol': st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('Lu',)))
    })
    
    return st.tuples(
        schema,
        st.lists(st.one_of(valid_record, missing_field_record, wrong_type_record), min_size=1, max_size=20)
    )


@settings(max_examples=100)
@given(data=generate_data_with_violations())
def test_property_field_validation_catches_schema_violations(data):
    """
    **Feature: airflow-simple-orchestration, Property 2: Field validation catches schema violations**
    **Validates: Requirements 3.2, 5.2**
    
    For any dataset and schema definition, when field validation is performed on data
    that violates the schema (missing fields or type mismatches), the validation should
    fail and report the violations.
    """
    schema, records = data
    
    # Create mock context
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    # Check if data has violations
    has_violations = False
    for record in records:
        for field_name, field_type in schema.items():
            if field_name not in record:
                has_violations = True
                break
            if not isinstance(record[field_name], field_type):
                has_violations = True
                break
        if has_violations:
            break
    
    if has_violations:
        # Should raise exception for invalid data
        with pytest.raises(Exception) as exc_info:
            validate_fields(records, schema, **context)
        
        # Verify error message mentions violations
        assert 'violation' in str(exc_info.value).lower()
        
        # Verify violations were logged
        assert mock_task_instance.log.error.called
    else:
        # Should succeed for valid data
        result = validate_fields(records, schema, **context)
        assert result['violations'] == 0
        assert result['valid_records'] == len(records)
        
        # Verify metrics pushed to XCom
        mock_task_instance.xcom_push.assert_called_once()


@settings(max_examples=100)
@given(
    num_records=st.integers(min_value=1, max_value=100)
)
def test_property_field_validation_passes_for_valid_data(num_records):
    """
    **Feature: airflow-simple-orchestration, Property 2: Field validation catches schema violations**
    **Validates: Requirements 3.2, 5.2**
    
    For any valid dataset that conforms to the schema, validation should pass
    and write metrics to XCom.
    """
    # Create mock context
    mock_task_instance = MagicMock()
    mock_task_instance.log = MagicMock()
    context = {'task_instance': mock_task_instance}
    
    # Generate valid data
    valid_data = [
        {
            'price': float(i + 1),
            'quantity': float(i + 1),
            'symbol': f'SYM{i}'
        }
        for i in range(num_records)
    ]
    
    schema = {
        'price': float,
        'quantity': float,
        'symbol': str
    }
    
    # Should succeed
    result = validate_fields(valid_data, schema, **context)
    
    # Verify results
    assert result['violations'] == 0
    assert result['valid_records'] == num_records
    assert result['total_records'] == num_records
    
    # Verify metrics pushed to XCom
    mock_task_instance.xcom_push.assert_called_once()
    call_args = mock_task_instance.xcom_push.call_args
    assert call_args[1]['key'] == 'validation_metrics'
