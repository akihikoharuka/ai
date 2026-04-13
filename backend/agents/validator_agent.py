"""Validator Agent: Logic checks, referential integrity, statistical comparison, privacy."""

from __future__ import annotations

import logging
import os

import pandas as pd
from langchain_core.messages import AIMessage

from backend.agents.state import Phase, SyntheticDataState, ValidationCheck, ValidationResult
from backend.config import settings
from backend.core.statistical import check_privacy_leakage, compare_distributions

logger = logging.getLogger(__name__)


def validate(state: SyntheticDataState) -> dict:
    """Run all validation checks on the generated data."""
    full_data_paths = state.get("full_data_paths", {})
    parsed_tables = state["parsed_tables"]
    generation_order = state["generation_order"]
    validation_retry_count = state.get("validation_retry_count", 0)
    real_data_paths = state.get("real_data_paths") or {}
    logger.info(
        "validate: starting — tables=%s real_data=%s retry=%d",
        generation_order,
        list(real_data_paths.keys()),
        validation_retry_count,
    )

    checks: list[ValidationCheck] = []
    table_map = {t["name"]: t for t in parsed_tables}
    generated_dfs: dict[str, pd.DataFrame] = {}

    # Load all generated data
    for table_name, path in full_data_paths.items():
        if os.path.exists(path):
            generated_dfs[table_name] = pd.read_csv(path)

    # Run checks for each table
    for table_name in generation_order:
        schema = table_map.get(table_name)
        df = generated_dfs.get(table_name)

        if df is None:
            checks.append({
                "check_name": "file_exists",
                "passed": False,
                "severity": "simple",
                "message": f"Table {table_name}: CSV file not found",
                "details": {},
            })
            continue

        if schema is None:
            continue

        # Check 1: All columns present
        expected_cols = {c["name"] for c in schema["columns"]}
        actual_cols = set(df.columns)
        missing = expected_cols - actual_cols
        if missing:
            checks.append({
                "check_name": "schema_conformance",
                "passed": False,
                "severity": "simple",
                "message": f"Table {table_name}: Missing columns: {missing}",
                "details": {"missing_columns": list(missing)},
            })

        # Check 2: NOT NULL constraints
        for col_info in schema["columns"]:
            col_name = col_info["name"]
            if col_name not in df.columns:
                continue
            if not col_info["nullable"]:
                null_count = df[col_name].isna().sum()
                if null_count > 0:
                    checks.append({
                        "check_name": "not_null",
                        "passed": False,
                        "severity": "simple",
                        "message": f"Table {table_name}.{col_name}: {null_count} NULL values in NOT NULL column",
                        "details": {"null_count": int(null_count)},
                    })

        # Check 3: UNIQUE constraints
        for col_info in schema["columns"]:
            col_name = col_info["name"]
            if col_name not in df.columns:
                continue
            if col_info["is_unique"] or col_info["is_primary_key"]:
                dup_count = df[col_name].duplicated().sum()
                if dup_count > 0:
                    checks.append({
                        "check_name": "unique_constraint",
                        "passed": False,
                        "severity": "simple",
                        "message": f"Table {table_name}.{col_name}: {dup_count} duplicate values in UNIQUE column",
                        "details": {"duplicate_count": int(dup_count)},
                    })

        # Check 4: Referential integrity
        for fk in schema.get("foreign_keys", []):
            fk_col = fk["column"]
            parent_table = fk["references_table"]
            parent_col = fk["references_column"]

            if fk_col not in df.columns:
                continue

            parent_df = generated_dfs.get(parent_table)
            if parent_df is None or parent_col not in parent_df.columns:
                continue

            child_values = set(df[fk_col].dropna().unique())
            parent_values = set(parent_df[parent_col].dropna().unique())
            orphans = child_values - parent_values

            if orphans:
                checks.append({
                    "check_name": "referential_integrity",
                    "passed": False,
                    "severity": "simple",
                    "message": f"Table {table_name}.{fk_col}: {len(orphans)} orphaned FK values not in {parent_table}.{parent_col}",
                    "details": {"orphan_count": len(orphans), "sample_orphans": list(orphans)[:5]},
                })

        # Check 5: Row count
        expected_rows = state.get("row_counts", {}).get(table_name, settings.default_row_count)
        actual_rows = len(df)
        if actual_rows != expected_rows:
            checks.append({
                "check_name": "row_count",
                "passed": False,
                "severity": "simple",
                "message": f"Table {table_name}: Expected {expected_rows} rows, got {actual_rows}",
                "details": {"expected": expected_rows, "actual": actual_rows},
            })

        # Check 6: Statistical comparison (if real data provided)
        if table_name in real_data_paths:
            real_path = real_data_paths[table_name]
            if os.path.exists(real_path):
                real_df = pd.read_csv(real_path)

                # Compare distributions for each common column
                common_cols = list(set(real_df.columns) & set(df.columns))
                for col_name in common_cols:
                    dist_result = compare_distributions(real_df[col_name], df[col_name], col_name)
                    if not dist_result["passed"]:
                        checks.append({
                            "check_name": "distribution_match",
                            "passed": False,
                            "severity": "semantic",
                            "message": f"Table {table_name}.{col_name}: {dist_result['message']}",
                            "details": dist_result,
                        })

                # Privacy check
                privacy_issues = check_privacy_leakage(real_df, df, table_name)
                checks.extend(privacy_issues)

    # Determine overall result
    all_passed = all(c["passed"] for c in checks) if checks else True
    failed_checks = [c for c in checks if not c["passed"]]
    validation_result: ValidationResult = {
        "passed": all_passed,
        "checks": checks,
    }

    logger.info(
        "validate: complete — %d checks run, %d failed, passed=%s",
        len(checks),
        len(failed_checks),
        all_passed,
    )
    for fc in failed_checks:
        logger.warning("validate: [%s] %s", fc["severity"].upper(), fc["message"])

    if all_passed:
        return {
            "validation_result": validation_result,
            "phase": Phase.COMPLETE,
            "messages": [AIMessage(content="All validation checks passed! Your data is ready for download.")],
        }

    # Determine routing based on failure severity
    semantic_failures = [c for c in checks if not c["passed"] and c["severity"] == "semantic"]
    simple_failures = [c for c in checks if not c["passed"] and c["severity"] == "simple"]

    failure_summary = "Validation issues found:\n"
    for c in checks:
        if not c["passed"]:
            failure_summary += f"- [{c['severity'].upper()}] {c['message']}\n"

    if semantic_failures and validation_retry_count < settings.max_validation_retries:
        return {
            "validation_result": validation_result,
            "validation_retry_count": validation_retry_count + 1,
            "script_error": failure_summary,
            "phase": Phase.ANALYSIS,
            "messages": [AIMessage(content=f"Semantic validation issues found. Re-analyzing...\n\n{failure_summary}")],
        }
    elif simple_failures and state.get("script_retry_count", 0) < settings.max_script_retries:
        return {
            "validation_result": validation_result,
            "script_error": failure_summary,
            "phase": Phase.GENERATING_SCRIPT,
            "messages": [AIMessage(content=f"Validation issues found. Fixing script...\n\n{failure_summary}")],
        }
    else:
        # Max retries exceeded — deliver with warnings
        return {
            "validation_result": validation_result,
            "phase": Phase.COMPLETE,
            "messages": [AIMessage(content=f"Validation completed with warnings (max retries reached):\n\n{failure_summary}\n\nData is available for download but may have issues.")],
        }


def save_output(state: SyntheticDataState) -> dict:
    """Save final output and mark as complete."""
    return {
        "phase": Phase.COMPLETE,
        "messages": [AIMessage(content="Data generation complete! You can download your files from the left panel.")],
    }
