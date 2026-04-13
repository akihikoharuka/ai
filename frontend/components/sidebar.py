"""Left panel: Schema upload, table list, row counts, downloads."""

from __future__ import annotations

import streamlit as st

from frontend import api_client


def render_sidebar():
    """Render the left sidebar panel."""
    st.markdown("### Schema & Tables")

    # Show tables once parsed
    tables = st.session_state.get("tables")
    if tables:
        st.markdown("---")
        st.markdown("### Tables")
        for table in tables:
            table_name = table["name"]
            col_count = len(table.get("columns", []))
            fk_count = len(table.get("foreign_keys", []))

            # Status icon
            phase = st.session_state.get("phase", "")
            full_paths = st.session_state.get("full_data_paths") or {}
            preview_data = st.session_state.get("preview_data") or {}

            if table_name in full_paths:
                icon = "✅"
            elif table_name in preview_data:
                icon = "👁️"
            else:
                icon = "📋"

            if st.button(
                f"{icon} {table_name} ({col_count} cols{', ' + str(fk_count) + ' FKs' if fk_count else ''})",
                key=f"table_{table_name}",
                use_container_width=True,
            ):
                st.session_state.selected_table = table_name

        # Row count configuration
        st.markdown("---")
        st.markdown("### Row Counts")
        row_counts = st.session_state.get("row_counts", {})
        updated = False
        for table in tables:
            table_name = table["name"]
            current = row_counts.get(table_name, 1000)
            new_val = st.number_input(
                table_name,
                min_value=10,
                max_value=1000000,
                value=current,
                step=100,
                key=f"rows_{table_name}",
            )
            if new_val != current:
                row_counts[table_name] = new_val
                updated = True

        if updated:
            st.session_state.row_counts = row_counts
            session_id = st.session_state.get("session_id")
            if session_id:
                try:
                    api_client.set_row_counts(session_id, row_counts)
                except Exception:
                    pass

    # Download section
    full_paths = st.session_state.get("full_data_paths") or {}
    if full_paths:
        st.markdown("---")
        st.markdown("### Download")
        session_id = st.session_state.session_id

        col1, col2 = st.columns(2)
        with col1:
            csv_url = api_client.download_all_url(session_id, "csv")
            st.markdown(f"[Download All (CSV)]({csv_url})")
        with col2:
            parquet_url = api_client.download_all_url(session_id, "parquet")
            st.markdown(f"[Download All (Parquet)]({parquet_url})")

    # New session button
    if st.session_state.get("session_id"):
        st.markdown("---")
        if st.button("🔄 New Session", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
