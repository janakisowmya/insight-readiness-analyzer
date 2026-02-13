"""Test the new policy schema validations."""
import pytest
from ira.correction.policy_schema import load_policy, validate_against_dataset, PolicyLoadError


def test_invalid_column_type_raises_error(tmp_path):
    """Test that invalid column types in parsing.column_types raise PolicyLoadError."""
    policy_file = tmp_path / "bad_policy.yaml"
    policy_file.write_text("""
parsing:
  column_types:
    price: "money"  # Invalid type - should be "numeric"
    qty: "integer"  # Valid
""")
    
    with pytest.raises(PolicyLoadError) as exc_info:
        load_policy(policy_file)
    
    assert "Invalid column type 'money'" in str(exc_info.value)
    assert "Must be one of" in str(exc_info.value)


def test_valid_column_types_accepted(tmp_path):
    """Test that all valid column types are accepted."""
    policy_file = tmp_path / "good_policy.yaml"
    policy_file.write_text("""
parsing:
  column_types:
    flag: "boolean"
    price: "numeric"
    qty: "integer"
    rate: "float"
    created_at: "datetime"
    birth_date: "date"
    event_time: "timestamp"
""")
    
    policy = load_policy(policy_file)
    assert policy["parsing"]["column_types"]["flag"] == "boolean"
    assert policy["parsing"]["column_types"]["price"] == "numeric"
    assert policy["parsing"]["column_types"]["qty"] == "integer"


def test_validate_against_dataset_detects_missing_columns():
    """Test that validate_against_dataset detects policy references to non-existent columns."""
    policy_dict = {
        "roles": {
            "critical_columns": ["order_id", "missing_col"],
            "protected_columns": ["customer_id"],
        },
        "parsing": {
            "column_types": {
                "price": "numeric",
                "nonexistent": "float",
            }
        },
        "deduplication": {
            "keys": ["order_id", "fake_key"],
            "order_by": "timestamp",
        },
    }
    
    dataset_columns = ["order_id", "customer_id", "price", "qty"]
    
    warnings = validate_against_dataset(policy_dict, dataset_columns)
    
    # Should detect missing columns
    assert any("missing_col" in w for w in warnings)
    assert any("nonexistent" in w for w in warnings)
    assert any("fake_key" in w for w in warnings)
    assert any("timestamp" in w for w in warnings)
    
    # Should have 4 warnings total
    assert len(warnings) == 4


def test_validate_against_dataset_no_warnings_when_all_exist():
    """Test that no warnings are returned when all policy columns exist."""
    policy_dict = {
        "roles": {
            "critical_columns": ["order_id"],
            "protected_columns": ["customer_id"],
        },
        "parsing": {
            "column_types": {
                "price": "numeric",
            }
        },
    }
    
    dataset_columns = ["order_id", "customer_id", "price", "qty"]
    
    warnings = validate_against_dataset(policy_dict, dataset_columns)
    
    assert len(warnings) == 0
