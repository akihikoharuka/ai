"""Tests for the SQL DDL parser."""

import pytest

from backend.core.sql_parser import parse_ddl, topological_sort, parse_and_sort


class TestParseDDL:
    """Test DDL parsing for various SQL constructs."""

    def test_parse_simple_table(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        assert len(tables) == 1
        t = tables[0]
        assert t.name == "users"
        assert len(t.columns) == 4

    def test_parse_primary_key(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        t = tables[0]
        assert t.primary_keys == ["user_id"]
        pk_col = next(c for c in t.columns if c.name == "user_id")
        assert pk_col.is_primary_key is True
        assert pk_col.nullable is False

    def test_parse_unique_constraint(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        t = tables[0]
        username_col = next(c for c in t.columns if c.name == "username")
        assert username_col.is_unique is True

    def test_parse_not_null(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        t = tables[0]
        email_col = next(c for c in t.columns if c.name == "email")
        assert email_col.nullable is False

    def test_parse_nullable_default(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        t = tables[0]
        created_col = next(c for c in t.columns if c.name == "created_at")
        assert created_col.nullable is True

    def test_parse_default_value(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        t = tables[0]
        created_col = next(c for c in t.columns if c.name == "created_at")
        assert created_col.default is not None

    def test_parse_data_types(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        t = tables[0]
        col_types = {c.name: c.data_type for c in t.columns}
        assert "INT" in col_types["user_id"]
        assert "VARCHAR" in col_types["username"]
        assert "TIMESTAMP" in col_types["created_at"]

    def test_parse_foreign_keys(self, two_table_ddl):
        tables = parse_ddl(two_table_ddl)
        emp_table = next(t for t in tables if t.name == "employees")
        assert len(emp_table.foreign_keys) == 1
        fk = emp_table.foreign_keys[0]
        assert fk.column == "dept_id"
        assert fk.references_table == "departments"
        assert fk.references_column == "dept_id"

    def test_parse_no_fk_on_parent(self, two_table_ddl):
        tables = parse_ddl(two_table_ddl)
        dept_table = next(t for t in tables if t.name == "departments")
        assert len(dept_table.foreign_keys) == 0

    def test_parse_healthcare_tables(self, healthcare_ddl):
        tables = parse_ddl(healthcare_ddl)
        table_names = [t.name for t in tables]
        assert "patients" in table_names
        assert "providers" in table_names
        assert "encounters" in table_names
        assert "diagnoses" in table_names
        assert "medications" in table_names

    def test_parse_healthcare_fks(self, healthcare_ddl):
        tables = parse_ddl(healthcare_ddl)
        encounters = next(t for t in tables if t.name == "encounters")
        fk_targets = {fk.column: fk.references_table for fk in encounters.foreign_keys}
        assert fk_targets["patient_id"] == "patients"
        assert fk_targets["provider_id"] == "providers"

    def test_parse_healthcare_check_constraints(self, healthcare_ddl):
        tables = parse_ddl(healthcare_ddl)
        patients = next(t for t in tables if t.name == "patients")
        assert len(patients.check_constraints) >= 1
        # Should have gender CHECK constraint
        assert any("gender" in c for c in patients.check_constraints)

    def test_parse_ecommerce_tables(self, ecommerce_ddl):
        tables = parse_ddl(ecommerce_ddl)
        table_names = [t.name for t in tables]
        assert "customers" in table_names
        assert "products" in table_names
        assert "orders" in table_names
        assert "order_items" in table_names
        assert "categories" in table_names

    def test_parse_self_referencing_fk(self, ecommerce_ddl):
        tables = parse_ddl(ecommerce_ddl)
        categories = next(t for t in tables if t.name == "categories")
        assert len(categories.foreign_keys) == 1
        fk = categories.foreign_keys[0]
        assert fk.column == "parent_category_id"
        assert fk.references_table == "categories"

    def test_parse_decimal_type(self, ecommerce_ddl):
        tables = parse_ddl(ecommerce_ddl)
        products = next(t for t in tables if t.name == "products")
        price_col = next(c for c in products.columns if c.name == "price")
        assert "DECIMAL" in price_col.data_type

    def test_parse_invalid_ddl(self):
        with pytest.raises(ValueError, match="Failed to parse"):
            parse_ddl("THIS IS NOT SQL AT ALL !!!")

    def test_parse_empty_ddl(self):
        result = parse_ddl("SELECT 1;")
        assert result == []

    def test_to_dict(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        d = tables[0].to_dict()
        assert d["name"] == "users"
        assert isinstance(d["columns"], list)
        assert all(isinstance(c, dict) for c in d["columns"])
        assert "primary_keys" in d
        assert "foreign_keys" in d


class TestTopologicalSort:
    """Test topological sort for generation order."""

    def test_simple_dependency(self, two_table_ddl):
        tables = parse_ddl(two_table_ddl)
        order = topological_sort(tables)
        dept_idx = order.index("departments")
        emp_idx = order.index("employees")
        assert dept_idx < emp_idx

    def test_healthcare_order(self, healthcare_ddl):
        tables = parse_ddl(healthcare_ddl)
        order = topological_sort(tables)
        # Patients and providers must come before encounters
        assert order.index("patients") < order.index("encounters")
        assert order.index("providers") < order.index("encounters")
        # Encounters must come before diagnoses and medications
        assert order.index("encounters") < order.index("diagnoses")
        assert order.index("encounters") < order.index("medications")

    def test_ecommerce_order(self, ecommerce_ddl):
        tables = parse_ddl(ecommerce_ddl)
        order = topological_sort(tables)
        # Customers before orders
        assert order.index("customers") < order.index("orders")
        # Orders and products before order_items
        assert order.index("orders") < order.index("order_items")
        assert order.index("products") < order.index("order_items")
        # Categories before products
        assert order.index("categories") < order.index("products")

    def test_self_reference_handled(self, ecommerce_ddl):
        tables = parse_ddl(ecommerce_ddl)
        order = topological_sort(tables)
        # Self-referencing FK should not break sort
        assert "categories" in order

    def test_no_fk_tables(self, simple_ddl):
        tables = parse_ddl(simple_ddl)
        order = topological_sort(tables)
        assert order == ["users"]

    def test_all_tables_included(self, healthcare_ddl):
        tables = parse_ddl(healthcare_ddl)
        order = topological_sort(tables)
        assert len(order) == len(tables)
        assert set(order) == {t.name for t in tables}


class TestParseAndSort:
    """Test the combined parse + sort function."""

    def test_returns_tuple(self, healthcare_ddl):
        tables, order = parse_and_sort(healthcare_ddl)
        assert isinstance(tables, list)
        assert isinstance(order, list)
        assert len(tables) == len(order)
