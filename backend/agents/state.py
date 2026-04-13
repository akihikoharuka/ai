"""LangGraph state definition for the synthetic data generation pipeline."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Optional, TypedDict

from langgraph.graph.message import add_messages


class Phase(str, Enum):
    UPLOAD = "upload"
    PARSING = "parsing"
    ANALYSIS = "analysis"
    AWAITING_USER_CONFIRMATION = "awaiting_user_confirmation"
    GENERATING_SCRIPT = "generating_script"
    AWAITING_PREVIEW_APPROVAL = "awaiting_preview_approval"
    GENERATING_FULL = "generating_full"
    VALIDATING = "validating"
    COMPLETE = "complete"
    ERROR = "error"


class ColumnStrategy(TypedDict):
    table: str
    column: str
    strategy: str  # faker, reference_data, foreign_key, distribution, sequential, computed, custom
    details: dict[str, Any]  # Strategy-specific config
    semantic_type: str  # medical_code, email, name, date, currency, etc.


class ValidationCheck(TypedDict):
    check_name: str
    passed: bool
    severity: str  # simple or semantic
    message: str
    details: dict[str, Any]


class ValidationResult(TypedDict):
    passed: bool
    checks: list[ValidationCheck]


class SyntheticDataState(TypedDict):
    # Chat messages (LangGraph managed, append-only)
    messages: Annotated[list, add_messages]

    # Session
    session_id: str

    # Phase 1: Raw parsing output
    raw_ddl: str
    parsed_tables: list[dict]  # Serialized TableSchema dicts
    generation_order: list[str]  # Topologically sorted table names

    # Phase 2: Brain analysis output
    column_strategies: list[ColumnStrategy]
    analysis_summary: str
    clarifying_questions: list[str]
    user_answers: dict[str, str]

    # User configuration
    row_counts: dict[str, int]  # {table_name: num_rows}
    real_data_paths: dict[str, str]  # {table_name: file_path}

    # Python Agent output
    generated_script: str
    script_error: str
    script_retry_count: int
    preview_data: dict[str, list[dict]]  # {table_name: [{row_dict}, ...]}
    full_data_paths: dict[str, str]  # {table_name: file_path}

    # Validator output
    validation_result: Optional[ValidationResult]
    validation_retry_count: int

    # Control flow
    phase: Phase
    error_message: str
