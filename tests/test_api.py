"""Tests for FastAPI endpoints."""

import pytest
from fastapi.testclient import TestClient

from backend.main import app


@pytest.fixture
def client():
    return TestClient(app)


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "healthy"}


class TestSessionEndpoints:
    def test_create_session(self, client):
        resp = client.post("/api/sessions")
        assert resp.status_code == 200
        data = resp.json()
        assert "session_id" in data
        assert len(data["session_id"]) == 8

    def test_get_status_new_session(self, client):
        # Create session
        resp = client.post("/api/sessions")
        session_id = resp.json()["session_id"]

        # Check status
        resp = client.get(f"/api/sessions/{session_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["session_id"] == session_id
        assert data["phase"] == "upload"

    def test_get_status_nonexistent(self, client):
        resp = client.get("/api/sessions/nonexistent/status")
        assert resp.status_code == 404

    def test_upload_schema(self, client, two_table_ddl):
        # Create session
        resp = client.post("/api/sessions")
        session_id = resp.json()["session_id"]

        # Upload schema
        resp = client.post(
            f"/api/sessions/{session_id}/upload-schema",
            json={"ddl": two_table_ddl},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"

    def test_upload_schema_nonexistent_session(self, client, two_table_ddl):
        resp = client.post(
            "/api/sessions/nonexistent/upload-schema",
            json={"ddl": two_table_ddl},
        )
        assert resp.status_code == 404

    def test_set_row_counts(self, client):
        resp = client.post("/api/sessions")
        session_id = resp.json()["session_id"]

        resp = client.post(
            f"/api/sessions/{session_id}/row-counts",
            json={"row_counts": {"users": 500}},
        )
        assert resp.status_code == 200

    def test_get_tables_empty(self, client):
        resp = client.post("/api/sessions")
        session_id = resp.json()["session_id"]

        resp = client.get(f"/api/sessions/{session_id}/tables")
        assert resp.status_code == 200
        assert resp.json()["tables"] == []

    def test_download_nonexistent(self, client):
        resp = client.post("/api/sessions")
        session_id = resp.json()["session_id"]

        resp = client.get(f"/api/sessions/{session_id}/download/users")
        assert resp.status_code == 404

    def test_send_message_nonexistent(self, client):
        resp = client.post(
            "/api/sessions/nonexistent/message",
            json={"content": "hello"},
        )
        assert resp.status_code == 404
