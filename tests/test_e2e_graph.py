"""End-to-end graph test with mocked LLM calls.

Reproduces the hang we saw in production logs after run_preview. Runs the
compiled graph directly, bypassing the API, so we can see exactly where (if
anywhere) LangGraph's astream loop stalls.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage
from langgraph.types import Command

from backend.agents.graph import compile_graph
from backend.agents.state import Phase


# A minimal, guaranteed-runnable script the Python agent "generates".
# Writes the customer table as a CSV so run_preview / run_full_generation
# both succeed without touching any LLM or network.
FAKE_SCRIPT = '''
import argparse
import csv
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", required=True)
parser.add_argument("--row-count", type=int, default=100)
args = parser.parse_args()

os.makedirs(args.output_dir, exist_ok=True)
out = os.path.join(args.output_dir, "customer.csv")
with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["cus_id", "name", "email"])
    for i in range(args.row_count):
        w.writerow([i + 1, f"User{i}", f"user{i}@example.com"])

print(json.dumps({"status": "success", "tables": {"customer": {"rows": args.row_count, "file": out}}}))
'''


ANALYSIS_JSON = json.dumps({
    "summary": "Single customer table, sequential id, faker name, faker email.",
    "column_strategies": [
        {"table": "customer", "column": "cus_id", "strategy": "sequential", "details": {"start": 1}, "semantic_type": "id"},
        {"table": "customer", "column": "name", "strategy": "faker", "details": {"method": "name"}, "semantic_type": "name"},
        {"table": "customer", "column": "email", "strategy": "faker", "details": {"method": "email"}, "semantic_type": "email"},
    ],
    "clarifying_questions": [],
})


SCHEMA_JSON = json.dumps({
    "tables": [
        {
            "name": "customer",
            "columns": [
                {"name": "cus_id", "data_type": "INT", "nullable": False, "is_primary_key": True, "is_unique": True, "default": None, "check_constraint": None},
                {"name": "name", "data_type": "VARCHAR", "nullable": True, "is_primary_key": False, "is_unique": False, "default": None, "check_constraint": None},
                {"name": "email", "data_type": "VARCHAR", "nullable": False, "is_primary_key": False, "is_unique": False, "default": None, "check_constraint": None},
            ],
            "foreign_keys": [],
            "check_constraints": [],
        }
    ],
    "generation_order": ["customer"],
})


def _fake_llm_invoke(messages):
    """Return canned LLM responses based on which prompt is being sent."""
    # messages is a list containing a SystemMessage — look at its content
    content = messages[0].content if messages else ""

    if "JSON describing the schema" in content or "plain English" in content or "infer" in content.lower():
        return AIMessage(content=f"```json\n{SCHEMA_JSON}\n```")

    if "column_strategies" in content or "Analyze this schema" in content or "clarifying_questions" in content:
        return AIMessage(content=f"```json\n{ANALYSIS_JSON}\n```")

    # Python agent prompt — return the fake script
    return AIMessage(content=f"```python\n{FAKE_SCRIPT}\n```")


async def _run_graph_to_completion(initial_state: dict, max_steps: int = 30):
    """Run the graph, capturing every event. Returns list of event dicts."""
    graph = compile_graph()
    config = {"configurable": {"thread_id": initial_state["session_id"]}}

    events = []
    step_count = 0

    # Phase 1: stream until first interrupt (summary approval)
    async for event in graph.astream(initial_state, config, stream_mode="updates"):
        events.append(event)
        step_count += 1
        node_name = next((k for k in event.keys() if k != "__interrupt__"), "__interrupt__")
        import sys
        sys.stderr.write(f"[step {step_count}] {node_name}\n")
        sys.stderr.flush()
        if step_count >= max_steps:
            raise RuntimeError(f"Graph exceeded {max_steps} steps without reaching interrupt")
        if "__interrupt__" in event:
            break

    # Phase 2: resume with "yes" approval, stream to completion
    async for event in graph.astream(Command(resume="yes"), config, stream_mode="updates"):
        events.append(event)
        step_count += 1
        node_name = next((k for k in event.keys() if k != "__interrupt__"), "__interrupt__")
        import sys
        sys.stderr.write(f"[step {step_count}] {node_name}\n")
        sys.stderr.flush()
        if step_count >= max_steps:
            raise RuntimeError(f"Graph exceeded {max_steps} steps — likely hanging/looping")

    # Get final state
    final_state = graph.get_state(config)
    return events, final_state


@pytest.mark.asyncio
async def test_graph_runs_to_completion_with_mocked_llm(tmp_path, monkeypatch):
    """The graph must complete end-to-end without hanging after run_preview.

    This is the exact failure mode we saw in production: the log ended at
    `run_preview: complete` with no subsequent event. If this test hangs or
    times out at that step, we've reproduced the bug.
    """
    # Point the output dir at a tmp location so we don't pollute the real one
    monkeypatch.setattr("backend.config.settings.output_dir", str(tmp_path))

    # Mock ChatOpenAI.invoke wherever it's used
    with patch("backend.agents.brain_agent.ChatOpenAI") as brain_llm_cls, \
         patch("backend.agents.python_agent.ChatOpenAI") as py_llm_cls, \
         patch("backend.agents.validator_agent.ChatOpenAI", create=True) as val_llm_cls:
        fake = MagicMock()
        fake.invoke.side_effect = _fake_llm_invoke
        brain_llm_cls.return_value = fake
        py_llm_cls.return_value = fake
        val_llm_cls.return_value = fake

        initial_state = {
            "messages": [],
            "session_id": "test_sess",
            "raw_ddl": "customer table with id, name, email",
            "parsed_tables": [],
            "generation_order": [],
            "column_strategies": [],
            "analysis_summary": "",
            "clarifying_questions": [],
            "user_answers": {},
            "row_counts": {"customer": 100},
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

        # Hard timeout — if the graph hangs we want a fast failure, not a test
        # suite that wedges indefinitely.
        try:
            events, final_state = await asyncio.wait_for(
                _run_graph_to_completion(initial_state),
                timeout=60.0,
            )
        except asyncio.TimeoutError:
            pytest.fail(
                "Graph hung — did not reach completion within 60s. "
                "This reproduces the production bug."
            )

        # Collect every node name that emitted an event
        node_names = []
        for ev in events:
            for key in ev.keys():
                if key != "__interrupt__":
                    node_names.append(key)

        print(f"\nNodes executed (in order): {node_names}")
        print(f"Final phase: {final_state.values.get('phase')}")

        # The critical assertion: we must have executed run_preview AND what
        # comes after it (run_full_generation → validate → save_output).
        assert "run_preview" in node_names, "run_preview never ran"
        assert "run_full_generation" in node_names, (
            "run_full_generation never ran — graph hung after run_preview "
            "(this is the production bug)"
        )
        assert "validate" in node_names, "validate never ran"
        assert "save_output" in node_names, "save_output never ran"

        # And the final state should have full_data_paths populated
        full_paths = final_state.values.get("full_data_paths") or {}
        assert full_paths, "full_data_paths is empty — nothing to export"
        assert "customer" in full_paths
