"""LangGraph graph construction for the synthetic data generation pipeline."""

from __future__ import annotations

from typing import Literal

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from backend.agents.brain_agent import analyze_schema, parse_schema, present_summary
from backend.agents.python_agent import (
    generate_script,
    present_preview,
    run_full_generation,
    run_preview,
)
from backend.agents.state import Phase, SyntheticDataState
from backend.agents.validator_agent import save_output, validate


def route_after_parse(state: SyntheticDataState) -> Literal["analyze_schema", "__end__"]:
    """Route after schema parsing."""
    if state.get("phase") == Phase.ERROR:
        return END
    return "analyze_schema"


def route_after_analysis(state: SyntheticDataState) -> Literal["present_summary", "__end__"]:
    """Route after schema analysis."""
    if state.get("phase") == Phase.ERROR:
        return END
    return "present_summary"


def route_after_summary(
    state: SyntheticDataState,
) -> Literal["generate_script", "analyze_schema"]:
    """Route after user reviews summary."""
    if state.get("phase") == Phase.ANALYSIS:
        return "analyze_schema"  # User wants changes
    return "generate_script"


def route_after_script(
    state: SyntheticDataState,
) -> Literal["run_preview", "generate_script", "__end__"]:
    """Route after script generation."""
    if state.get("phase") == Phase.ERROR:
        return END
    if state.get("script_error"):
        return "generate_script"  # Self-heal
    return "run_preview"


def route_after_preview_run(
    state: SyntheticDataState,
) -> Literal["present_preview", "generate_script", "__end__"]:
    """Route after running preview."""
    if state.get("phase") == Phase.ERROR:
        return END
    if state.get("phase") == Phase.GENERATING_SCRIPT:
        return "generate_script"  # Script failed, self-heal
    return "present_preview"


def route_after_preview_approval(
    state: SyntheticDataState,
) -> Literal["run_full_generation", "generate_script"]:
    """Route after user reviews preview."""
    if state.get("phase") == Phase.GENERATING_SCRIPT:
        return "generate_script"  # User wants changes
    return "run_full_generation"


def route_after_full_gen(
    state: SyntheticDataState,
) -> Literal["validate", "generate_script", "__end__"]:
    """Route after full data generation."""
    if state.get("phase") == Phase.ERROR:
        return END
    if state.get("phase") == Phase.GENERATING_SCRIPT:
        return "generate_script"
    return "validate"


def route_after_validation(
    state: SyntheticDataState,
) -> Literal["save_output", "generate_script", "analyze_schema"]:
    """Route after validation."""
    phase = state.get("phase")
    if phase == Phase.ANALYSIS:
        return "analyze_schema"  # Semantic failure -> re-analyze
    if phase == Phase.GENERATING_SCRIPT:
        return "generate_script"  # Simple failure -> fix script
    return "save_output"  # Passed or max retries exceeded


def build_graph() -> StateGraph:
    """Build and compile the LangGraph state graph."""
    graph = StateGraph(SyntheticDataState)

    # Add nodes
    graph.add_node("parse_schema", parse_schema)
    graph.add_node("analyze_schema", analyze_schema)
    graph.add_node("present_summary", present_summary)
    graph.add_node("generate_script", generate_script)
    graph.add_node("run_preview", run_preview)
    graph.add_node("present_preview", present_preview)
    graph.add_node("run_full_generation", run_full_generation)
    graph.add_node("validate", validate)
    graph.add_node("save_output", save_output)

    # Set entry point
    graph.set_entry_point("parse_schema")

    # Add conditional edges
    graph.add_conditional_edges("parse_schema", route_after_parse)
    graph.add_conditional_edges("analyze_schema", route_after_analysis)
    graph.add_conditional_edges("present_summary", route_after_summary)
    graph.add_conditional_edges("generate_script", route_after_script)
    graph.add_conditional_edges("run_preview", route_after_preview_run)
    graph.add_conditional_edges("present_preview", route_after_preview_approval)
    graph.add_conditional_edges("run_full_generation", route_after_full_gen)
    graph.add_conditional_edges("validate", route_after_validation)

    # save_output always ends
    graph.add_edge("save_output", END)

    return graph


def compile_graph():
    """Compile the graph with a memory checkpointer."""
    graph = build_graph()
    memory = MemorySaver()
    return graph.compile(checkpointer=memory)
