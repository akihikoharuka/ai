"""Session lifecycle management."""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime

from backend.agents.graph import compile_graph
from backend.config import settings

logger = logging.getLogger(__name__)


@dataclass
class Session:
    id: str
    dir: str
    graph: object  # Compiled LangGraph
    config: dict
    created_at: datetime = field(default_factory=datetime.utcnow)
    phase: str = "upload"
    tables: list[dict] | None = None
    messages: list[dict] = field(default_factory=list)
    preview_data: dict | None = None
    full_data_paths: dict | None = None
    validation_result: dict | None = None


class SessionManager:
    def __init__(self):
        self.sessions: dict[str, Session] = {}

    def create_session(self) -> Session:
        session_id = uuid.uuid4().hex[:8]
        session_dir = os.path.abspath(os.path.join(settings.output_dir, session_id))

        for subdir in ["scripts", "preview", "final", "validation"]:
            os.makedirs(os.path.join(session_dir, subdir), exist_ok=True)

        graph = compile_graph()
        config = {"configurable": {"thread_id": session_id}}

        session = Session(
            id=session_id,
            dir=session_dir,
            graph=graph,
            config=config,
        )
        self.sessions[session_id] = session
        logger.info("Session created: id=%s dir=%s", session_id, session_dir)
        return session

    def get_session(self, session_id: str) -> Session | None:
        session = self.sessions.get(session_id)
        if session is None:
            logger.debug("Session lookup miss: id=%s", session_id)
        return session

    def delete_session(self, session_id: str) -> None:
        if session_id in self.sessions:
            self.sessions.pop(session_id)
            logger.info("Session deleted: id=%s", session_id)

    def list_sessions(self) -> list[str]:
        return list(self.sessions.keys())


# Global session manager instance
session_manager = SessionManager()
