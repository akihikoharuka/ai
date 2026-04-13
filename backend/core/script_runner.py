"""Sandbox for executing generated Python scripts in a subprocess."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass

from backend.config import settings

logger = logging.getLogger(__name__)


@dataclass
class ScriptResult:
    success: bool
    stdout: str
    error: str
    tables_generated: dict[str, dict] | None = None  # {table_name: {rows, file}}


def run_script(
    script: str,
    output_dir: str,
    row_count: int | None = None,
    timeout: int | None = None,
) -> ScriptResult:
    """Write script to temp file, execute in subprocess, return result."""
    if timeout is None:
        timeout = settings.script_timeout_seconds

    os.makedirs(output_dir, exist_ok=True)

    # Write script to a file in the output directory
    script_path = os.path.join(output_dir, "_generate.py")
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script)

    # Build command
    python_exe = sys.executable
    cmd = [python_exe, script_path, "--output-dir", output_dir]
    if row_count is not None:
        cmd.extend(["--row-count", str(row_count)])

    logger.info(
        "Running generation script: output_dir=%s row_count=%s timeout=%ss",
        output_dir,
        row_count if row_count is not None else "default",
        timeout,
    )

    # On Windows, isolate child from the parent's console process group so that
    # Ctrl+C in the terminal doesn't propagate into the generated script and
    # cause a spurious KeyboardInterrupt during startup.
    popen_kwargs: dict = {}
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=os.path.dirname(os.path.abspath(output_dir)),
            **popen_kwargs,
        )

        if result.returncode != 0:
            logger.error(
                "Script exited with code %d. stderr: %s",
                result.returncode,
                (result.stderr or "").strip()[:500],
            )
            return ScriptResult(
                success=False,
                stdout=result.stdout,
                error=result.stderr or f"Script exited with code {result.returncode}",
            )

        # Try to parse the JSON status from stdout
        tables_generated = None
        try:
            # Find the last line that looks like JSON
            for line in reversed(result.stdout.strip().split("\n")):
                line = line.strip()
                if line.startswith("{"):
                    status = json.loads(line)
                    if status.get("status") == "success":
                        tables_generated = status.get("tables", {})
                    break
        except (json.JSONDecodeError, IndexError):
            pass

        logger.info(
            "Script completed successfully. Tables generated: %s",
            list(tables_generated.keys()) if tables_generated else "unknown",
        )
        return ScriptResult(
            success=True,
            stdout=result.stdout,
            error="",
            tables_generated=tables_generated,
        )

    except subprocess.TimeoutExpired:
        logger.error("Script timed out after %s seconds (output_dir=%s)", timeout, output_dir)
        return ScriptResult(
            success=False,
            stdout="",
            error=f"Script timed out after {timeout} seconds. Consider reducing row count.",
        )
    except Exception as e:
        logger.error("Failed to execute script: %s", e, exc_info=True)
        return ScriptResult(
            success=False,
            stdout="",
            error=f"Failed to execute script: {str(e)}",
        )
