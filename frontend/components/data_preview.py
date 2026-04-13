"""Center panel: Data preview, schema info, validation results."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def render_data_preview():
    """Render the center data preview panel."""
    st.markdown("### Data Preview")

    selected_table = st.session_state.get("selected_table")
    tables = st.session_state.get("tables")

    if not tables:
        st.info("Describe your tables in the chat on the right to get started.")
        return

    if not selected_table:
        st.info("Select a table from the left panel to preview.")
        # Auto-select first table
        if tables:
            st.session_state.selected_table = tables[0]["name"]
            st.rerun()
        return

    # Find the table schema
    table_schema = None
    for t in tables:
        if t["name"] == selected_table:
            table_schema = t
            break

    if not table_schema:
        st.warning(f"Table '{selected_table}' not found.")
        return

    # Tabs
    tab_preview, tab_schema, tab_validation = st.tabs(["Preview", "Schema", "Validation"])

    with tab_preview:
        _render_preview_tab(selected_table)

    with tab_schema:
        _render_schema_tab(table_schema)

    with tab_validation:
        _render_validation_tab(selected_table)


def _render_preview_tab(table_name: str):
    """Render preview data as a dataframe."""
    preview_data = st.session_state.get("preview_data") or {}
    full_paths = st.session_state.get("full_data_paths") or {}

    if table_name in preview_data:
        data = preview_data[table_name]
        if data:
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True, height=400)
            st.caption(f"Showing {len(df)} preview rows")
        else:
            st.info("Preview data is empty.")
    elif table_name in full_paths:
        # Show from full data
        import os
        path = full_paths[table_name]
        if os.path.exists(path):
            df = pd.read_csv(path, nrows=100)
            st.dataframe(df, use_container_width=True, height=400)
            # Get full row count
            full_count = sum(1 for _ in open(path)) - 1
            st.caption(f"Showing first 100 of {full_count} rows")

            # Download buttons
            session_id = st.session_state.session_id
            from frontend.api_client import download_table_url
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"[Download CSV]({download_table_url(session_id, table_name, 'csv')})")
            with col2:
                st.markdown(f"[Download Parquet]({download_table_url(session_id, table_name, 'parquet')})")
    else:
        phase = st.session_state.get("phase", "")
        if phase in ("generating_script", "generating_full", "validating"):
            st.info("Data is being generated...")
        else:
            st.info("No preview data yet. Approve the generation plan to see preview data.")


def _render_schema_tab(table_schema: dict):
    """Render column definitions and constraints."""
    cols = table_schema.get("columns", [])
    if cols:
        schema_data = []
        for col in cols:
            schema_data.append({
                "Column": col["name"],
                "Type": col["data_type"],
                "Nullable": "Yes" if col["nullable"] else "No",
                "PK": "Yes" if col["is_primary_key"] else "",
                "Unique": "Yes" if col["is_unique"] else "",
                "Default": col.get("default") or "",
                "Check": col.get("check_constraint") or "",
            })
        df = pd.DataFrame(schema_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Foreign keys
    fks = table_schema.get("foreign_keys", [])
    if fks:
        st.markdown("**Foreign Keys:**")
        for fk in fks:
            st.markdown(f"- `{fk['column']}` → `{fk['references_table']}.{fk['references_column']}`")

    # Check constraints
    checks = table_schema.get("check_constraints", [])
    if checks:
        st.markdown("**Check Constraints:**")
        for check in checks:
            st.markdown(f"- `{check}`")


def _render_validation_tab(table_name: str):
    """Render validation results for the selected table."""
    validation_result = st.session_state.get("validation_result")
    if not validation_result:
        st.info("Validation results will appear here after data generation.")
        return

    checks = validation_result.get("checks", [])
    table_checks = [c for c in checks if table_name in c.get("message", "")]

    if not table_checks:
        if validation_result.get("passed"):
            st.success("All checks passed!")
        else:
            st.info("No specific checks for this table.")
        return

    for check in table_checks:
        if check["passed"]:
            st.success(f"✅ **{check['check_name']}**: {check['message']}")
        else:
            severity_icon = "🔴" if check["severity"] == "semantic" else "🟡"
            st.error(f"{severity_icon} **{check['check_name']}** [{check['severity']}]: {check['message']}")
