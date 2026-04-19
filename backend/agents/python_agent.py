"""Python Coder Agent: Generates and executes data generation scripts."""

from __future__ import annotations

import json
import logging
import os
import re

import pandas as pd
from langchain_core.messages import AIMessage, SystemMessage
from langchain_openai import ChatOpenAI

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

    llm = ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
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
        "messages": [AIMessage(content="Script generated. Running preview...")],
    }


def run_preview(state: SyntheticDataState) -> dict:
    """Run the generated script with a small row count for preview."""
    script = state["generated_script"]
    session_id = state["session_id"]
    retry_count = state.get("script_retry_count", 0)

    preview_dir = os.path.abspath(os.path.join(settings.output_dir, session_id, "preview"))
    logger.info("run_preview: session=%s row_count=%d", session_id, settings.preview_row_count)
    result = run_script(script, preview_dir, row_count=settings.preview_row_count)

    if not result.success:
        logger.error("run_preview: script failed (attempt %d) — %s", retry_count + 1, result.error)
        if retry_count >= settings.max_script_retries:
            return {
                "script_error": result.error,
                "script_retry_count": retry_count + 1,
                "phase": Phase.ERROR,
                "error_message": f"Script failed after {retry_count + 1} attempts: {result.error}",
                "messages": [AIMessage(content=f"Script failed after multiple attempts. Last error: {result.error}")],
            }
        return {
            "script_error": result.error,
            "script_retry_count": retry_count + 1,
            "phase": Phase.GENERATING_SCRIPT,
            "messages": [AIMessage(content=f"Script error: {result.error}. Fixing and retrying...")],
        }

    # Read preview CSVs into preview_data.
    # Primary: use generation_order so table ordering is preserved.
    # Fallback: scan the output directory for any CSVs the script produced
    # (handles the case where generation_order is empty or a table name differs).
    # CSV read errors are non-fatal — the preview step must never block the
    # downstream full-generation / validation / export flow.
    preview_data = {}
    for table_name in state["generation_order"]:
        csv_path = os.path.join(preview_dir, f"{table_name}.csv")
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path, nrows=settings.preview_row_count)
                preview_data[table_name] = df.to_dict(orient="records")
            except Exception as e:
                logger.warning("run_preview: failed to read %s — %s", csv_path, e)

    if not preview_data:
        # Fallback: pick up any CSVs written by the script that we didn't expect
        try:
            for fname in os.listdir(preview_dir):
                if fname.endswith(".csv") and not fname.startswith("_"):
                    table_name = fname[:-4]
                    csv_path = os.path.join(preview_dir, fname)
                    try:
                        df = pd.read_csv(csv_path, nrows=settings.preview_row_count)
                        preview_data[table_name] = df.to_dict(orient="records")
                    except Exception as e:
                        logger.warning("run_preview: failed to read %s — %s", csv_path, e)
        except Exception as e:
            logger.warning("run_preview: failed to scan output dir — %s", e)

        if preview_data:
            logger.warning(
                "run_preview: generation_order was empty/mismatched — "
                "discovered tables from output dir: %s",
                list(preview_data.keys()),
            )

    logger.info("run_preview: complete — tables previewed: %s", list(preview_data.keys()))
    # Decoupled flow: advance straight to full generation. The preview data is
    # still in state so the UI can show sample rows while the full run happens.
    return {
        "preview_data": preview_data,
        "script_error": "",
        "phase": Phase.GENERATING_FULL,
        "messages": [AIMessage(
            content=f"Preview generated with {settings.preview_row_count} rows per table. "
                    f"Proceeding to full data generation..."
        )],
    }


def run_full_generation(state: SyntheticDataState) -> dict:
    """Run the script with full row counts."""
    script = state["generated_script"]
    session_id = state["session_id"]
    retry_count = state.get("script_retry_count", 0)

    final_dir = os.path.abspath(os.path.join(settings.output_dir, session_id, "final"))
    row_counts = state.get("row_counts", {})
    logger.info(
        "run_full_generation: session=%s row_counts=%s",
        session_id,
        {t: row_counts.get(t) for t in state.get("generation_order", [])},
    )
    result = run_script(script, final_dir)

    if not result.success:
        logger.error("run_full_generation: failed (attempt %d) — %s", retry_count + 1, result.error)
        if retry_count >= settings.max_script_retries:
            return {
                "script_error": result.error,
                "phase": Phase.ERROR,
                "error_message": f"Full generation failed: {result.error}",
                "messages": [AIMessage(content=f"Full generation failed: {result.error}")],
            }
        return {
            "script_error": result.error,
            "script_retry_count": retry_count + 1,
            "phase": Phase.GENERATING_SCRIPT,
            "messages": [AIMessage(content=f"Full generation error: {result.error}. Fixing...")],
        }

    # Record paths
    full_data_paths = {}
    for table_name in state["generation_order"]:
        csv_path = os.path.join(final_dir, f"{table_name}.csv")
        if os.path.exists(csv_path):
            full_data_paths[table_name] = csv_path

    logger.info("run_full_generation: complete — files: %s", list(full_data_paths.keys()))
    return {
        "full_data_paths": full_data_paths,
        "script_error": "",
        "phase": Phase.VALIDATING,
        "messages": [AIMessage(content=f"Full dataset generated. Running validation...")],
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
