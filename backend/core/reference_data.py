"""Reference data loader for domain-specific curated lists."""

from __future__ import annotations

import csv
import json
import os

_REF_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "reference_data")


def get_reference_data_path(source: str) -> str:
    """Get the full path to a reference data file."""
    # Try CSV first, then JSON
    for ext in [".csv", ".json"]:
        path = os.path.join(_REF_DATA_DIR, f"{source}{ext}")
        if os.path.exists(path):
            return path
    return os.path.join(_REF_DATA_DIR, f"{source}.csv")


def load_reference_list(source: str, column: str | None = None) -> list[str]:
    """Load a list of values from a reference data file."""
    path = get_reference_data_path(source)

    if not os.path.exists(path):
        return []

    if path.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return [str(item) for item in data]
            elif isinstance(data, dict):
                if column and column in data:
                    return [str(item) for item in data[column]]
                # Return all values
                return [str(v) for v in data.values()]
    elif path.endswith(".csv"):
        values = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if column and column in row:
                    values.append(row[column])
                else:
                    # Use first column
                    values.append(list(row.values())[0])
        return values

    return []


def list_available_sources() -> list[str]:
    """List available reference data sources."""
    if not os.path.exists(_REF_DATA_DIR):
        return []
    sources = []
    for filename in os.listdir(_REF_DATA_DIR):
        name, ext = os.path.splitext(filename)
        if ext in (".csv", ".json"):
            sources.append(name)
    return sources
