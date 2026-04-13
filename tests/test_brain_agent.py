"""Tests for the Brain Agent (parse_schema node — no LLM needed)."""

import pytest

from backend.agents.brain_agent import parse_schema
from backend.agents.state import Phase


class TestParseSchemaNode:
    """Test the parse_schema graph node."""

    def test_successful_parse(self, healthcare_ddl):
        state = {"raw_ddl": healthcare_ddl, "row_counts": {}}
        result = parse_schema(state)

        assert result["phase"] == Phase.ANALYSIS
        assert len(result["parsed_tables"]) == 5
        assert len(result["generation_order"]) == 5
        assert "error_message" not in result or not result.get("error_message")

    def test_tables_are_dicts(self, healthcare_ddl):
        state = {"raw_ddl": healthcare_ddl, "row_counts": {}}
        result = parse_schema(state)

        for table in result["parsed_tables"]:
            assert isinstance(table, dict)
            assert "name" in table
            assert "columns" in table
            assert "primary_keys" in table
            assert "foreign_keys" in table

    def test_generation_order(self, healthcare_ddl):
        state = {"raw_ddl": healthcare_ddl, "row_counts": {}}
        result = parse_schema(state)

        order = result["generation_order"]
        # Parents before children
        assert order.index("patients") < order.index("encounters")
        assert order.index("providers") < order.index("encounters")
        assert order.index("encounters") < order.index("diagnoses")

    def test_default_row_counts(self, healthcare_ddl):
        state = {"raw_ddl": healthcare_ddl, "row_counts": {}}
        result = parse_schema(state)

        row_counts = result["row_counts"]
        assert len(row_counts) == 5
        for table_name, count in row_counts.items():
            assert count == 1000  # default

    def test_preserves_existing_row_counts(self, healthcare_ddl):
        state = {"raw_ddl": healthcare_ddl, "row_counts": {"patients": 500}}
        result = parse_schema(state)

        assert result["row_counts"]["patients"] == 500
        assert result["row_counts"]["providers"] == 1000  # default for others

    def test_produces_message(self, healthcare_ddl):
        state = {"raw_ddl": healthcare_ddl, "row_counts": {}}
        result = parse_schema(state)

        assert len(result["messages"]) == 1
        msg = result["messages"][0]
        assert "5 tables" in msg.content
        assert "patients" in msg.content

    def test_invalid_ddl_returns_error(self):
        state = {"raw_ddl": "NOT VALID SQL !!!", "row_counts": {}}
        result = parse_schema(state)

        assert result["phase"] == Phase.ERROR
        assert "error_message" in result
        assert result["error_message"]

    def test_simple_table(self, simple_ddl):
        state = {"raw_ddl": simple_ddl, "row_counts": {}}
        result = parse_schema(state)

        assert result["phase"] == Phase.ANALYSIS
        assert len(result["parsed_tables"]) == 1
        assert result["parsed_tables"][0]["name"] == "users"

    def test_two_table_fks(self, two_table_ddl):
        state = {"raw_ddl": two_table_ddl, "row_counts": {}}
        result = parse_schema(state)

        assert result["phase"] == Phase.ANALYSIS
        order = result["generation_order"]
        assert order.index("departments") < order.index("employees")

        emp = next(t for t in result["parsed_tables"] if t["name"] == "employees")
        assert len(emp["foreign_keys"]) == 1
        assert emp["foreign_keys"][0]["references_table"] == "departments"
