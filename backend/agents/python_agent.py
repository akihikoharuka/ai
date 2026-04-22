"""Python Coder Agent: Generates and executes data generation scripts."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re

import pandas as pd
from langchain_core.messages import AIMessage, SystemMessage
from langchain_openai import AzureChatOpenAI

from backend.agents.prompts import (
    PYTHON_AGENT_GENERATION_PROMPT,
    PYTHON_AGENT_SELF_HEAL_PROMPT,
)
from backend.agents.state import Phase, SyntheticDataState
from backend.config import settings
from backend.core.script_runner import run_script

logger = logging.getLogger(__name__)


def generate_script(state: SyntheticDataState) -> dict:
    """Generate a Python script for synthetic data creation."""
    parsed_tables = state["parsed_tables"]
    generation_order = state["generation_order"]
    column_strategies = state.get("column_strategies", [])
    row_counts = state.get("row_counts", {})
    script_error = state.get("script_error", "")
    previous_script = state.get("generated_script", "")
    validation_result = state.get("validation_result")
    retry_count = state.get("script_retry_count", 0)
    is_retry = bool(script_error and previous_script)
    logger.info(
        "generate_script: attempt %d/%d — tables=%s is_retry=%s",
        retry_count + 1,
        settings.max_script_retries + 1,
        generation_order,
        is_retry,
    )

    # Determine reference data directory (absolute path)
    ref_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "reference_data"))

    # List available reference data files so the LLM uses real filenames
    available_ref_files: list[str] = []
    if os.path.exists(ref_data_dir):
        for fname in sorted(os.listdir(ref_data_dir)):
            if fname.endswith((".csv", ".json")):
                available_ref_files.append(fname)
    available_reference_files = "\n".join(f"  - {f}" for f in available_ref_files) or "  (none)"

    # Build error context for self-healing
    error_context = ""
    if script_error and previous_script:
        validation_failures = ""
        if validation_result and not validation_result.get("passed", True):
            failed_checks = [c for c in validation_result.get("checks", []) if not c.get("passed")]
            validation_failures = json.dumps(failed_checks, indent=2)

        prompt_text = PYTHON_AGENT_SELF_HEAL_PROMPT.format(
            previous_script=previous_script,
            error_message=script_error,
            validation_failures=validation_failures or "None",
            reference_data_dir=ref_data_dir,
            available_reference_files=available_reference_files,
        )
    else:
        prompt_text = PYTHON_AGENT_GENERATION_PROMPT.format(
            schema_json=json.dumps(parsed_tables, indent=2),
            generation_order=json.dumps(generation_order),
            strategies_json=json.dumps(column_strategies, indent=2),
            row_counts=json.dumps(row_counts),
            reference_data_dir=ref_data_dir,
            available_reference_files=available_reference_files,
            error_context="",
        )

    llm = AzureChatOpenAI(
        azure_endpoint=settings.azure_openai_endpoint,
        azure_deployment=settings.llm_model,
        api_key=settings.azure_openai_api_key,
        api_version=settings.azure_openai_api_version,
        max_tokens=8192,
    )

    response = llm.invoke([SystemMessage(content=prompt_text)])

    # Extract Python code from response
    script = _extract_python_code(response.content)
    if not script:
        logger.error("generate_script: could not extract Python code from LLM response")
        return {
            "script_error": "Failed to extract Python code from LLM response",
            "script_retry_count": retry_count + 1,
            "phase": Phase.ERROR if retry_count >= settings.max_script_retries else Phase.GENERATING_SCRIPT,
            "messages": [AIMessage(content="Error generating script. Retrying...")],
        }

    logger.info("generate_script: script extracted (%d chars)", len(script))
    return {
        "generated_script": script,
        "script_error": "",
        "phase": Phase.GENERATING_SCRIPT,
        "messages": [AIMessage(content="Script generated. Running preview and full generation...")],
    }


def run_preview(script: str, preview_dir: str) -> object:
    """Run the generated script with a small row count for preview."""
    logger.info(
        "run_preview: starting preview run — output_dir=%s row_count=%d",
        preview_dir,
        settings.preview_row_count,
    )
    return run_script(script, preview_dir, row_count=settings.preview_row_count)


def run_full_generation(script: str, final_dir: str) -> object:
    """Run the generated script for the full dataset."""
    logger.info("run_full_generation: starting full generation — output_dir=%s", final_dir)
    return run_script(script, final_dir)


async def run_preview_and_full_generation_async(script: str, preview_dir: str, final_dir: str, row_counts: dict, session_id: str):
    """Run preview and full generation concurrently."""
    preview_task = asyncio.to_thread(run_preview, script, preview_dir)
    full_task = asyncio.to_thread(run_full_generation, script, final_dir)

    logger.info(
        "run_preview_and_full_generation_async: launched preview and full generation tasks for session=%s",
        session_id,
    )

    preview_result, full_result = await asyncio.gather(preview_task, full_task)
    return preview_result, full_result


def run_preview_and_full_generation(state: SyntheticDataState) -> dict:
    """Run preview and full generation in parallel."""
    script = state["generated_script"]
    session_id = state["session_id"]
    row_counts = state.get("row_counts", {})

    preview_dir = os.path.abspath(os.path.join(settings.output_dir, session_id, "preview"))
    final_dir = os.path.abspath(os.path.join(settings.output_dir, session_id, "final"))

    logger.info("run_preview_and_full_generation: starting parallel runs for session=%s", session_id)

    # Run both asynchronously
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        preview_result, full_result = loop.run_until_complete(
            run_preview_and_full_generation_async(script, preview_dir, final_dir, row_counts, session_id)
        )
    finally:
        loop.close()

    # Handle preview results
    preview_data = {}
    if preview_result.success:
        # Read preview CSVs
        for table_name in state["generation_order"]:
            csv_path = os.path.join(preview_dir, f"{table_name}.csv")
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path, nrows=settings.preview_row_count)
                    preview_data[table_name] = df.to_dict(orient="records")
                except Exception as e:
                    logger.warning("run_preview_and_full_generation: failed to read preview %s — %s", csv_path, e)
    else:
        logger.error("run_preview_and_full_generation: preview failed — %s", preview_result.error)

    # Handle full generation results
    full_data_paths = {}
    if full_result.success:
        for table_name in state["generation_order"]:
            csv_path = os.path.join(final_dir, f"{table_name}.csv")
            if os.path.exists(csv_path):
                full_data_paths[table_name] = csv_path
    else:
        logger.error("run_preview_and_full_generation: full generation failed — %s", full_result.error)

    # Determine next phase
    if not full_result.success:
        return {
            "preview_data": preview_data,
            "full_data_paths": full_data_paths,
            "preview_error": preview_result.error if not preview_result.success else None,
            "full_generation_error": full_result.error,
            "phase": Phase.GENERATING_SCRIPT,  # Retry
            "messages": [AIMessage(content=f"Full generation failed: {full_result.error}. Retrying...")],
        }

    logger.info("run_preview_and_full_generation: complete — preview tables: %s, full tables: %s", list(preview_data.keys()), list(full_data_paths.keys()))
    return {
        "preview_data": preview_data,
        "full_data_paths": full_data_paths,
        "preview_error": None,
        "full_generation_error": None,
        "phase": Phase.VALIDATING,
        "messages": [AIMessage(content="Preview and full data generated. Running validation...")],
    }


def _extract_python_code(response: str) -> str | None:
    """Extract Python code from LLM response."""
    # Try ```python ... ``` blocks
    match = re.search(r"```python\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try ``` ... ``` blocks
    match = re.search(r"```\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()

    # If the entire response looks like Python code
    if "import " in response and "def " in response:
        return response.strip()

    return None
