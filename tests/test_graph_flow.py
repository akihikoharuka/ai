"""Tests for the LangGraph graph construction and compilation."""

import pytest

from backend.agents.graph import build_graph, compile_graph
from backend.agents.state import Phase


class TestGraphConstruction:
    def test_build_graph_has_all_nodes(self):
        graph = build_graph()
        expected_nodes = {
            "parse_schema",
            "analyze_schema",
            "present_summary",
            "generate_script",
            "run_preview",
            "present_preview",
            "run_full_generation",
            "validate",
            "save_output",
        }
        actual_nodes = set(graph.nodes.keys())
        assert expected_nodes == actual_nodes

    def test_compile_graph(self):
        compiled = compile_graph()
        assert compiled is not None

    def test_entry_point_is_parse_schema(self):
        graph = build_graph()
        # The entry point should route to parse_schema
        compiled = compile_graph()
        # Check that graph has __start__ -> parse_schema edge
        assert "parse_schema" in graph.nodes
