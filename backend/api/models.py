"""Pydantic request/response models for the API."""

from __future__ import annotations

from pydantic import BaseModel


class CreateSessionResponse(BaseModel):
    session_id: str


class UploadSchemaRequest(BaseModel):
    ddl: str


class MessageRequest(BaseModel):
    content: str


class RowCountsRequest(BaseModel):
    row_counts: dict[str, int]


class StatusResponse(BaseModel):
    session_id: str
    phase: str
    tables: list[dict] | None = None
    generation_order: list[str] | None = None
    messages: list[dict] = []
    preview_data: dict | None = None
    full_data_paths: dict | None = None
    validation_result: dict | None = None
    interrupt_data: dict | None = None
    error_message: str | None = None
    preview_error: str | None = None
    full_generation_error: str | None = None
