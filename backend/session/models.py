"""Session data models for API serialization."""

from __future__ import annotations

from pydantic import BaseModel


class SessionInfo(BaseModel):
    session_id: str
    phase: str
    tables: list[dict] | None = None
    messages: list[dict] = []
    preview_data: dict | None = None
    interrupt_data: dict | None = None
