"""Streamlit main application — 3-panel layout for synthetic data generation."""

from __future__ import annotations

import time

import requests
import streamlit as st

# Must be first Streamlit command
st.set_page_config(
    page_title="Synthetic Data Generator",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

from frontend import api_client
from frontend.components.chatbot import render_chatbot
from frontend.components.data_preview import render_data_preview
from frontend.components.sidebar import render_sidebar
from frontend.state import init_state

# Initialize session state
init_state()


_MAX_POLL_ERRORS = 5  # stop polling after this many consecutive failures


def poll_status():
    """Poll backend for status updates.

    Returns True when the UI should rerender immediately (phase changed or
    interrupt arrived).  On transient connection errors the error counter is
    incremented; after _MAX_POLL_ERRORS consecutive failures polling is stopped
    so we don't loop forever with a dead backend.
    """
    session_id = st.session_state.get("session_id")
    if not session_id:
        return False

    try:
        status = api_client.get_status(session_id)

        # Successful response — reset the error counter
        st.session_state.poll_error_count = 0

        # Update session state from backend
        new_phase = status.get("phase", st.session_state.phase)
        phase_changed = new_phase != st.session_state.phase

        st.session_state.phase = new_phase
        st.session_state.error_message = status.get("error_message")

        # Update tables if available
        if status.get("tables"):
            st.session_state.tables = status["tables"]
            st.session_state.generation_order = status.get("generation_order")

        # Update preview data
        if status.get("preview_data"):
            st.session_state.preview_data = status["preview_data"]

        # Update full data paths
        if status.get("full_data_paths"):
            st.session_state.full_data_paths = status["full_data_paths"]

        # Update validation result
        if status.get("validation_result"):
            st.session_state.validation_result = status["validation_result"]

        # Update interrupt data
        st.session_state.interrupt_data = status.get("interrupt_data")

        # Sync messages from backend
        backend_messages = status.get("messages", [])
        if len(backend_messages) > len(st.session_state.chat_messages):
            st.session_state.chat_messages = backend_messages

        # Stop polling if complete, errored, or waiting for user input
        if new_phase in ("complete", "error") or status.get("interrupt_data"):
            st.session_state.polling = False

        return phase_changed or status.get("interrupt_data") is not None

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        st.session_state.poll_error_count = st.session_state.get("poll_error_count", 0) + 1
        if st.session_state.poll_error_count >= _MAX_POLL_ERRORS:
            st.session_state.polling = False
            st.session_state.error_message = (
                "Lost connection to the backend after several retries. "
                "Please check that the backend is still running, then refresh the page."
            )
        # Transient error — keep polling, but signal no change
        return False

    except Exception:
        # Unexpected error — stop polling to avoid an infinite error loop
        st.session_state.polling = False
        return False


# Main layout
st.title("🧬 Synthetic Data Generator")

# Three-column layout
left_col, center_col, right_col = st.columns([1, 2, 1.5])

with left_col:
    render_sidebar()

with center_col:
    render_data_preview()

with right_col:
    render_chatbot()

# Error display
if st.session_state.get("error_message"):
    st.error(f"Error: {st.session_state.error_message}")

# Status indicator in sidebar
phase = st.session_state.get("phase", "upload")
phase_labels = {
    "upload": "📤 Ready to upload",
    "parsing": "⏳ Parsing schema...",
    "analysis": "🧠 Analyzing schema...",
    "awaiting_user_confirmation": "💬 Waiting for your approval",
    "generating_script": "⚙️ Generating script...",
    "awaiting_preview_approval": "👁️ Review preview data",
    "generating_full": "🏭 Generating full dataset...",
    "validating": "🔍 Validating data...",
    "complete": "✅ Complete!",
    "error": "❌ Error",
}
with left_col:
    st.markdown("---")
    st.markdown(f"**Status:** {phase_labels.get(phase, phase)}")

# Polling logic
if st.session_state.get("polling") and st.session_state.get("session_id"):
    time.sleep(2)
    changed = poll_status()
    if changed or st.session_state.get("polling"):
        st.rerun()
