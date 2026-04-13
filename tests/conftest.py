"""Shared test fixtures."""

import os
import sys

import pytest

# Ensure project root is on the path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

SAMPLE_SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "sample_schemas")


@pytest.fixture
def healthcare_ddl():
    with open(os.path.join(SAMPLE_SCHEMAS_DIR, "healthcare.sql")) as f:
        return f.read()


@pytest.fixture
def ecommerce_ddl():
    with open(os.path.join(SAMPLE_SCHEMAS_DIR, "ecommerce.sql")) as f:
        return f.read()


@pytest.fixture
def simple_ddl():
    return """
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """


@pytest.fixture
def two_table_ddl():
    return """
    CREATE TABLE departments (
        dept_id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

    CREATE TABLE employees (
        emp_id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        dept_id INT NOT NULL,
        salary DECIMAL(10, 2),
        FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
    );
    """
