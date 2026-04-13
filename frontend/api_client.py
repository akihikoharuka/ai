"""HTTP client for communicating with the FastAPI backend."""

from __future__ import annotations

import requests

API_BASE = "http://localhost:8000/api"

# Short timeout for status polls so a slow backend never freezes Streamlit.
# Long-running work happens in background tasks on the backend, so the status
# endpoint itself should always respond quickly.
_STATUS_TIMEOUT = 8   # seconds
_WRITE_TIMEOUT = 10   # seconds for POST/PUT requests


def create_session() -> str:
    """Create a new session and return session_id."""
    resp = requests.post(f"{API_BASE}/sessions", timeout=_WRITE_TIMEOUT)
    resp.raise_for_status()
    return resp.json()["session_id"]


def upload_schema(session_id: str, ddl: str) -> dict:
    """Upload SQL DDL to start the pipeline."""
    resp = requests.post(
        f"{API_BASE}/sessions/{session_id}/upload-schema",
        json={"ddl": ddl},
        timeout=_WRITE_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def send_message(session_id: str, content: str) -> dict:
    """Send a user message (resumes graph from interrupt)."""
    resp = requests.post(
        f"{API_BASE}/sessions/{session_id}/message",
        json={"content": content},
        timeout=_WRITE_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_status(session_id: str) -> dict:
    """Get current session status."""
    resp = requests.get(
        f"{API_BASE}/sessions/{session_id}/status",
        timeout=_STATUS_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def set_row_counts(session_id: str, row_counts: dict[str, int]) -> dict:
    """Set row counts for tables."""
    resp = requests.post(
        f"{API_BASE}/sessions/{session_id}/row-counts",
        json={"row_counts": row_counts},
        timeout=_WRITE_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_tables(session_id: str) -> dict:
    """Get parsed table list."""
    resp = requests.get(
        f"{API_BASE}/sessions/{session_id}/tables",
        timeout=_STATUS_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_preview(session_id: str, table_name: str) -> dict:
    """Get preview data for a table."""
    resp = requests.get(
        f"{API_BASE}/sessions/{session_id}/preview/{table_name}",
        timeout=_STATUS_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def download_table_url(session_id: str, table_name: str, format: str = "csv") -> str:
    """Get download URL for a table."""
    return f"{API_BASE}/sessions/{session_id}/download/{table_name}?format={format}"


def download_all_url(session_id: str, format: str = "csv") -> str:
    """Get download URL for all tables as ZIP."""
    return f"{API_BASE}/sessions/{session_id}/download-all?format={format}"
