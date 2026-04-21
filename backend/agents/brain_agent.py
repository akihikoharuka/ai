"""Brain Agent: Schema parsing (mechanical) + semantic analysis (LLM)."""

from __future__ import annotations

import json
import logging

from langchain_core.messages import AIMessage, SystemMessage
from langchain_openai import ChatOpenAI

from backend.agents.prompts import BRAIN_AGENT_ANALYSIS_PROMPT, PLAIN_ENGLISH_TO_SCHEMA_PROMPT
from backend.agents.state import Phase, SyntheticDataState
from backend.config import settings
from backend.core.sql_parser import parse_and_sort

logger = logging.getLogger(__name__)


def parse_schema(state: SyntheticDataState) -> dict:
    """Phase 1: Parse schema input — SQL DDL or plain English description."""
    print("--- BRAIN IS THINKING ---")
    raw_ddl = state["raw_ddl"]
    logger.info("parse_schema: input received (%d chars)", len(raw_ddl))

    try:
        tables, generation_order = parse_and_sort(raw_ddl)
        parsed = [t.to_dict() for t in tables]

        # Set default row counts if not provided
        row_counts = state.get("row_counts") or {}
        for table in tables:
            if table.name not in row_counts:
                row_counts[table.name] = settings.default_row_count

        logger.info(
            "parse_schema: SQL parsed — %d tables, order: %s",
            len(tables),
            " -> ".join(generation_order),
        )
        return {
            "parsed_tables": parsed,
            "generation_order": generation_order,
            "row_counts": row_counts,
            "phase": Phase.ANALYSIS,
            "messages": [AIMessage(content=f"Schema parsed. Found {len(tables)} tables: {', '.join(t.name for t in tables)}.\nGeneration order: {' -> '.join(generation_order)}")],
        }
    except Exception:
        # Not valid SQL — infer schema from plain English via LLM
        logger.info("parse_schema: not valid SQL, falling back to LLM inference")
        return _infer_schema_from_text(state)


def _infer_schema_from_text(state: SyntheticDataState) -> dict:
    """Use the LLM to turn a plain-English description into a structured schema."""
    raw_input = state["raw_ddl"]

    llm = ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        base_url=settings.llm_base_url,
        max_tokens=4096,
    )

    prompt = PLAIN_ENGLISH_TO_SCHEMA_PROMPT.format(user_input=raw_input)
    response = llm.invoke([SystemMessage(content=prompt)])

    try:
        content = response.content.strip()
        # Strip markdown fences if present
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        schema_data = json.loads(content.strip())

        parsed_tables = schema_data.get("tables", [])
        generation_order = schema_data.get(
            "generation_order", [t["name"] for t in parsed_tables]
        )

        if not parsed_tables:
            raise ValueError("LLM returned no tables")

        # Set default row counts
        row_counts = state.get("row_counts") or {}
        for table in parsed_tables:
            if table["name"] not in row_counts:
                row_counts[table["name"]] = settings.default_row_count

        table_names = ", ".join(t["name"] for t in parsed_tables)
        logger.info(
            "_infer_schema_from_text: inferred %d tables: %s",
            len(parsed_tables),
            table_names,
        )
        return {
            "parsed_tables": parsed_tables,
            "generation_order": generation_order,
            "row_counts": row_counts,
            "phase": Phase.ANALYSIS,
            "messages": [AIMessage(
                content=f"Got it! I inferred {len(parsed_tables)} tables from your description: "
                        f"{table_names}.\nGeneration order: {' -> '.join(generation_order)}"
            )],
        }
    except Exception as e:
        logger.error("_infer_schema_from_text: failed — %s", e)
        return {
            "phase": Phase.ERROR,
            "error_message": f"Could not understand the schema description: {str(e)}",
            "messages": [AIMessage(
                content="I couldn't understand your schema description. "
                        "Try something like:\n\n"
                        "> users (id, name, email, created_at), orders (id, user_id, total, status)"
                        "\n\nor paste a SQL CREATE TABLE statement."
            )],
        }


def analyze_schema(state: SyntheticDataState) -> dict:
    """Phase 2 (LLM): Semantic analysis of schema columns."""
    print("--- BRAIN IS THINKING ---")
    parsed_tables = state["parsed_tables"]
    generation_order = state["generation_order"]
    user_answers = state.get("user_answers") or {}
    logger.info(
        "analyze_schema: starting LLM analysis for %d tables (model=%s)",
        len(parsed_tables),
        settings.llm_model,
    )

    # Build user context from any previous answers
    user_context = ""
    if user_answers:
        user_context = "## Additional Context from User\n"
        for q, a in user_answers.items():
            user_context += f"Q: {q}\nA: {a}\n"

    # Build the prompt
    prompt = BRAIN_AGENT_ANALYSIS_PROMPT.format(
        schema_json=json.dumps(parsed_tables, indent=2),
        generation_order=json.dumps(generation_order),
        user_context=user_context,
    )

    llm = ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        base_url=settings.llm_base_url,
        max_tokens=4096,
    )

    response = llm.invoke([SystemMessage(content=prompt)])

    # Parse the JSON response from the LLM
    try:
        # Extract JSON from the response (handle markdown code blocks)
        content = response.content
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        analysis = json.loads(content)

        column_strategies = analysis.get("column_strategies", [])
        questions = analysis.get("clarifying_questions", [])
        summary = analysis.get("summary", "Analysis complete.")

        logger.info(
            "analyze_schema: complete — %d column strategies, %d clarifying questions",
            len(column_strategies),
            len(questions),
        )
        return {
            "column_strategies": column_strategies,
            "clarifying_questions": questions,
            "analysis_summary": summary,
            "phase": Phase.AWAITING_USER_CONFIRMATION,
            "messages": [AIMessage(content=summary)],
        }
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error("analyze_schema: failed to parse LLM response — %s", e)
        logger.error("analyze_schema: raw response snippet: %s", response.content[:500])
        return {
            "phase": Phase.ERROR,
            "error_message": f"Failed to parse analysis: {str(e)}",
            "messages": [AIMessage(content="I had trouble analyzing the schema. Let me try again...")],
        }


def present_summary(state: SyntheticDataState) -> dict:
    """Present analysis summary and wait for user confirmation via interrupt."""
    from langgraph.types import interrupt

    summary = state.get("analysis_summary", "")
    questions = state.get("clarifying_questions", [])

    msg = f"## Generation Plan\n\n{summary}"
    if questions:
        msg += "\n\n### Questions\n" + "\n".join(f"- {q}" for q in questions)
    msg += "\n\nDo you approve this plan? You can also ask me to change anything."

    # Interrupt and wait for user response
    user_response = interrupt({"type": "approval_request", "message": msg})

    # When resumed, process user's response
    approval_keywords = {"yes", "approve", "ok", "go", "proceed", "looks good", "lgtm", "sure", "correct", "confirmed"}
    is_approved = any(kw in user_response.lower() for kw in approval_keywords)

    if is_approved:
        return {
            "user_answers": {**(state.get("user_answers") or {}), "_approval": user_response},
            "phase": Phase.GENERATING_SCRIPT,
            "messages": [AIMessage(content="Great! Starting data generation...")],
        }
    else:
        # User wants changes — re-analyze with their feedback
        return {
            "user_answers": {**(state.get("user_answers") or {}), "_feedback": user_response},
            "phase": Phase.ANALYSIS,
            "messages": [AIMessage(content="Got it, let me revise the plan based on your feedback.")],
        }
