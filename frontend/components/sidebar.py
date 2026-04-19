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

    # Export section — dropdown enabled once validation completes.
    # Decoupled from preview: shows up as soon as tables are known, greyed out
    # until the full dataset is generated and validated.
    if st.session_state.get("session_id"):
        st.markdown("---")
        st.markdown("### Export Data")

        full_paths = st.session_state.get("full_data_paths") or {}
        phase = st.session_state.get("phase", "")
        validation_done = st.session_state.get("validation_result") is not None
        export_ready = bool(full_paths) and (phase == "complete" or validation_done)

        format_label = st.selectbox(
            "Format",
            ["CSV", "Parquet"],
            key="export_format",
            disabled=not export_ready,
            help=None if export_ready else "Available after data generation and validation complete",
        )

        if export_ready:
            session_id = st.session_state.session_id
            fmt = "csv" if format_label == "CSV" else "parquet"
            url = api_client.download_all_url(session_id, fmt)
            st.link_button(
                f"⬇️ Export All as {format_label}",
                url,
                use_container_width=True,
            )
        else:
            st.button(
                "⬇️ Export All",
                disabled=True,
                use_container_width=True,
                help="Available after data generation and validation complete",
            )

    # New session button
    if st.session_state.get("session_id"):
        st.markdown("---")
        if st.button("🔄 New Session", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
