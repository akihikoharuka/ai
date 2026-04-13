"""Tests for the Validator Agent (deterministic checks — no LLM needed)."""

import os
import tempfile

import pandas as pd
import pytest

from backend.agents.validator_agent import validate
from backend.agents.state import Phase


def _make_state(tables, generation_order, data_dir, row_counts=None):
    """Helper to build a minimal state dict for the validator."""
    full_data_paths = {}
    for table_name, df in tables.items():
        path = os.path.join(data_dir, f"{table_name}.csv")
        df.to_csv(path, index=False)
        full_data_paths[table_name] = path

    return {
        "parsed_tables": [
            {
                "name": "parents",
                "columns": [
                    {"name": "id", "data_type": "INT", "nullable": False, "is_primary_key": True, "is_unique": True, "default": None, "check_constraint": None},
                    {"name": "name", "data_type": "VARCHAR(50)", "nullable": False, "is_primary_key": False, "is_unique": False, "default": None, "check_constraint": None},
                ],
                "primary_keys": ["id"],
                "foreign_keys": [],
                "check_constraints": [],
                "unique_constraints": [],
            },
            {
                "name": "children",
                "columns": [
                    {"name": "id", "data_type": "INT", "nullable": False, "is_primary_key": True, "is_unique": True, "default": None, "check_constraint": None},
                    {"name": "parent_id", "data_type": "INT", "nullable": False, "is_primary_key": False, "is_unique": False, "default": None, "check_constraint": None},
                    {"name": "value", "data_type": "DECIMAL(10,2)", "nullable": True, "is_primary_key": False, "is_unique": False, "default": None, "check_constraint": None},
                ],
                "primary_keys": ["id"],
                "foreign_keys": [{"column": "parent_id", "references_table": "parents", "references_column": "id"}],
                "check_constraints": [],
                "unique_constraints": [],
            },
        ],
        "generation_order": generation_order,
        "full_data_paths": full_data_paths,
        "row_counts": row_counts or {"parents": 5, "children": 5},
        "real_data_paths": {},
        "validation_retry_count": 0,
        "script_retry_count": 0,
    }


class TestValidatorChecks:
    """Test individual validation checks."""

    def test_valid_data_passes(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 2, 3, 1, 2], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        result = validate(state)
        assert result["phase"] == Phase.COMPLETE
        assert result["validation_result"]["passed"] is True

    def test_null_in_not_null_column(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["A", None, "C", "D", "E"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 2, 3, 1, 2], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        result = validate(state)
        checks = result["validation_result"]["checks"]
        not_null_failures = [c for c in checks if c["check_name"] == "not_null" and not c["passed"]]
        assert len(not_null_failures) >= 1
        assert "name" in not_null_failures[0]["message"]

    def test_duplicate_in_unique_column(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 1, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 1, 3, 4, 5], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        result = validate(state)
        checks = result["validation_result"]["checks"]
        unique_failures = [c for c in checks if c["check_name"] == "unique_constraint" and not c["passed"]]
        assert len(unique_failures) >= 1

    def test_referential_integrity_violation(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 2, 999, 998, 5], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        result = validate(state)
        checks = result["validation_result"]["checks"]
        fk_failures = [c for c in checks if c["check_name"] == "referential_integrity" and not c["passed"]]
        assert len(fk_failures) >= 1
        assert fk_failures[0]["details"]["orphan_count"] == 2

    def test_wrong_row_count(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 2, 3, 1, 2], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        result = validate(state)
        checks = result["validation_result"]["checks"]
        count_failures = [c for c in checks if c["check_name"] == "row_count" and not c["passed"]]
        assert len(count_failures) >= 1
        assert "parents" in count_failures[0]["message"]

    def test_missing_csv_file(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        # Only create parents, not children
        state = _make_state(
            {"parents": parents_df},
            ["parents", "children"],
            str(tmp_path),
        )
        # Manually add children to full_data_paths with non-existent file
        state["full_data_paths"]["children"] = os.path.join(str(tmp_path), "children.csv")

        result = validate(state)
        checks = result["validation_result"]["checks"]
        file_failures = [c for c in checks if c["check_name"] == "file_exists" and not c["passed"]]
        assert len(file_failures) >= 1


class TestValidatorRouting:
    """Test that validation failures route correctly."""

    def test_simple_failure_routes_to_script(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 1, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 1, 3, 4, 5], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        result = validate(state)
        # Simple failures should route to GENERATING_SCRIPT
        assert result["phase"] == Phase.GENERATING_SCRIPT

    def test_max_retries_completes(self, tmp_path):
        parents_df = pd.DataFrame({"id": [1, 1, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        children_df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "parent_id": [1, 1, 3, 4, 5], "value": [10.0, 20.0, 30.0, 40.0, 50.0]})

        state = _make_state(
            {"parents": parents_df, "children": children_df},
            ["parents", "children"],
            str(tmp_path),
        )
        state["script_retry_count"] = 10  # Exceeded max retries
        result = validate(state)
        # Should complete with warnings when max retries exceeded
        assert result["phase"] == Phase.COMPLETE
