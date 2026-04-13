"""REST API endpoints for the synthetic data generator."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import zipfile

from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from langgraph.types import Command

from backend.api.models import (
    CreateSessionResponse,
    MessageRequest,
    RowCountsRequest,
    StatusResponse,
    UploadSchemaRequest,
)
from backend.session.manager import session_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

# Track running graph tasks
_running_tasks: dict[str, asyncio.Task] = {}


@router.post("/sessions", response_model=CreateSessionResponse)
async def create_session():
    """Create a new generation session."""
    session = session_manager.create_session()
    logger.info("POST /sessions — new session_id=%s", session.id)
    return CreateSessionResponse(session_id=session.id)


@router.post("/sessions/{session_id}/upload-schema")
async def upload_schema(session_id: str, request: UploadSchemaRequest):
    """Upload SQL DDL and start the pipeline."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    logger.info(
        "POST /sessions/%s/upload-schema — DDL length=%d chars",
        session_id,
        len(request.ddl),
    )
    # Start the graph in a background task
    initial_state = {
        "messages": [],
        "session_id": session_id,
        "raw_ddl": request.ddl,
        "parsed_tables": [],
        "generation_order": [],
        "column_strategies": [],
        "analysis_summary": "",
        "clarifying_questions": [],
        "user_answers": {},
        "row_counts": {},
        "real_data_paths": {},
        "generated_script": "",
        "script_error": "",
        "script_retry_count": 0,
        "preview_data": {},
        "full_data_paths": {},
        "validation_result": None,
        "validation_retry_count": 0,
        "phase": "upload",
        "error_message": "",
    }

    task = asyncio.create_task(_run_graph(session, initial_state))
    _running_tasks[session_id] = task

    return {"status": "started", "session_id": session_id}


@router.post("/sessions/{session_id}/upload-schema-file")
async def upload_schema_file(session_id: str, file: UploadFile = File(...)):
    """Upload SQL DDL file and start the pipeline."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    content = await file.read()
    ddl = content.decode("utf-8")

    request = UploadSchemaRequest(ddl=ddl)
    return await upload_schema(session_id, request)


@router.post("/sessions/{session_id}/message")
async def send_message(session_id: str, request: MessageRequest):
    """Send a user message (resumes graph from interrupt)."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    logger.info(
        "POST /sessions/%s/message — content_length=%d",
        session_id,
        len(request.content),
    )
    # Resume the graph with user input
    task = asyncio.create_task(_resume_graph(session, request.content))
    _running_tasks[session_id] = task

    return {"status": "resumed"}


@router.post("/sessions/{session_id}/row-counts")
async def set_row_counts(session_id: str, request: RowCountsRequest):
    """Set row counts for tables."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Update row counts in the graph state
    state = session.graph.get_state(session.config)
    if state and state.values:
        session.graph.update_state(
            session.config,
            {"row_counts": request.row_counts},
        )

    return {"status": "updated"}


@router.get("/sessions/{session_id}/status", response_model=StatusResponse)
async def get_status(session_id: str):
    """Get current session status including phase, messages, and data."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Get current graph state
    state = session.graph.get_state(session.config)

    if not state or not state.values:
        return StatusResponse(
            session_id=session_id,
            phase="upload",
        )

    values = state.values

    # Check for interrupts
    interrupt_data = None
    if state.next:
        # Graph is paused at an interrupt
        try:
            if state.tasks and state.tasks[0].interrupts:
                interrupt_data = state.tasks[0].interrupts[0].value
        except (IndexError, AttributeError):
            pass

    # Format messages for the frontend
    messages = []
    for msg in values.get("messages", []):
        role = "assistant" if hasattr(msg, "type") and msg.type == "ai" else "user"
        content = msg.content if hasattr(msg, "content") else str(msg)
        messages.append({"role": role, "content": content})

    return StatusResponse(
        session_id=session_id,
        phase=values.get("phase", "upload"),
        tables=values.get("parsed_tables"),
        generation_order=values.get("generation_order"),
        messages=messages,
        preview_data=values.get("preview_data"),
        full_data_paths=values.get("full_data_paths"),
        validation_result=values.get("validation_result"),
        interrupt_data=interrupt_data,
        error_message=values.get("error_message"),
    )


@router.get("/sessions/{session_id}/tables")
async def get_tables(session_id: str):
    """Get parsed table list."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    state = session.graph.get_state(session.config)
    if not state or not state.values:
        return {"tables": []}

    return {
        "tables": state.values.get("parsed_tables", []),
        "generation_order": state.values.get("generation_order", []),
    }


@router.get("/sessions/{session_id}/preview/{table_name}")
async def get_preview(session_id: str, table_name: str):
    """Get preview data for a specific table."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    state = session.graph.get_state(session.config)
    if not state or not state.values:
        raise HTTPException(status_code=404, detail="No preview data available")

    preview_data = state.values.get("preview_data", {})
    if table_name not in preview_data:
        raise HTTPException(status_code=404, detail=f"No preview for table {table_name}")

    return {"table_name": table_name, "data": preview_data[table_name]}


@router.get("/sessions/{session_id}/download/{table_name}")
async def download_table(session_id: str, table_name: str, format: str = "csv"):
    """Download generated data for a table."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    state = session.graph.get_state(session.config)
    if not state or not state.values:
        raise HTTPException(status_code=404, detail="No data available")

    full_data_paths = state.values.get("full_data_paths", {})
    if table_name not in full_data_paths:
        raise HTTPException(status_code=404, detail=f"No data for table {table_name}")

    file_path = full_data_paths[table_name]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    if format == "parquet":
        import pandas as pd
        df = pd.read_csv(file_path)
        parquet_path = file_path.replace(".csv", ".parquet")
        df.to_parquet(parquet_path, index=False)
        return FileResponse(
            parquet_path,
            media_type="application/octet-stream",
            filename=f"{table_name}.parquet",
        )

    return FileResponse(
        file_path,
        media_type="text/csv",
        filename=f"{table_name}.csv",
    )


@router.get("/sessions/{session_id}/download-all")
async def download_all(session_id: str, format: str = "csv"):
    """Download all generated tables as a ZIP file."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    state = session.graph.get_state(session.config)
    if not state or not state.values:
        raise HTTPException(status_code=404, detail="No data available")

    full_data_paths = state.values.get("full_data_paths", {})
    if not full_data_paths:
        raise HTTPException(status_code=404, detail="No data generated")

    # Create ZIP in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for table_name, file_path in full_data_paths.items():
            if os.path.exists(file_path):
                if format == "parquet":
                    import pandas as pd
                    df = pd.read_csv(file_path)
                    parquet_path = file_path.replace(".csv", ".parquet")
                    df.to_parquet(parquet_path, index=False)
                    zf.write(parquet_path, f"{table_name}.parquet")
                else:
                    zf.write(file_path, f"{table_name}.csv")

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=synthetic_data_{session_id}.zip"},
    )


@router.post("/sessions/{session_id}/upload-real-data/{table_name}")
async def upload_real_data(session_id: str, table_name: str, file: UploadFile = File(...)):
    """Upload real data CSV for distribution comparison."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Save the uploaded file
    real_data_dir = os.path.join(session.dir, "real_data")
    os.makedirs(real_data_dir, exist_ok=True)
    file_path = os.path.join(real_data_dir, f"{table_name}.csv")

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    # Update state
    state = session.graph.get_state(session.config)
    if state and state.values:
        real_data_paths = state.values.get("real_data_paths", {})
        real_data_paths[table_name] = file_path
        session.graph.update_state(
            session.config,
            {"real_data_paths": real_data_paths},
        )

    return {"status": "uploaded", "table_name": table_name, "path": file_path}


async def _run_graph(session, initial_state: dict):
    """Run the graph asynchronously."""
    logger.info("Graph starting: session_id=%s", session.id)
    try:
        async for event in session.graph.astream(
            initial_state,
            session.config,
            stream_mode="updates",
        ):
            logger.info("Graph event: session_id=%s nodes=%s", session.id, list(event.keys()))
        logger.info("Graph finished: session_id=%s", session.id)
    except Exception as e:
        logger.error("Graph execution error: session_id=%s error=%s", session.id, e, exc_info=True)


async def _resume_graph(session, user_message: str):
    """Resume graph from interrupt with user input."""
    logger.info("Graph resuming: session_id=%s", session.id)
    try:
        async for event in session.graph.astream(
            Command(resume=user_message),
            session.config,
            stream_mode="updates",
        ):
            logger.info("Graph resume event: session_id=%s nodes=%s", session.id, list(event.keys()))
        logger.info("Graph resumed and finished: session_id=%s", session.id)
    except Exception as e:
        logger.error("Graph resume error: session_id=%s error=%s", session.id, e, exc_info=True)
