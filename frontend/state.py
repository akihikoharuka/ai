"""Streamlit session state management helpers."""

from __future__ import annotations

import streamlit as st


def init_state():
    """Initialize all session state variables."""
    defaults = {
        "session_id": None,
        "phase": "upload",
        "tables": None,
        "generation_order": None,
        "selected_table": None,
        "chat_messages": [],
        "preview_data": None,
        "full_data_paths": None,
        "validation_result": None,
        "interrupt_data": None,
        "error_message": None,
        "row_counts": {},
        "polling": False,
        "poll_error_count": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
