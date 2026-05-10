"""
Shared pytest fixtures for IGEM-Server backend tests.

All fixtures use SQLite + tmp_path for isolation and speed; no external
services (PG, Redis, etc.) are required to run the suite.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def sqlite_uri(tmp_path: Path) -> str:
    """A SQLite URI pointing at a fresh DB file inside tmp_path."""
    return f"sqlite:///{tmp_path / 'test.db'}"


@pytest.fixture
def empty_engine(sqlite_uri: str):
    """(engine, uri) pair against a fresh, empty SQLite DB."""
    engine = create_engine(sqlite_uri, future=True)
    yield engine, sqlite_uri
    engine.dispose()


@pytest.fixture
def empty_engine_with_session(sqlite_uri: str):
    """
    (engine, uri, sessionmaker) — for tests that exercise the
    `_mirror_revision_to_metadata` defensive path.
    """
    engine: Engine = create_engine(sqlite_uri, future=True)
    session_factory = sessionmaker(
        bind=engine, future=True, expire_on_commit=False,
    )
    yield engine, sqlite_uri, session_factory
    engine.dispose()


@pytest.fixture
def fake_snapshot_dir(tmp_path: Path) -> Path:
    """
    Minimal valid snapshot directory: contains an empty manifest.json
    so `Database._connect_snapshot()` accepts it as read-only mode.
    """
    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "manifest.json").write_text(
        json.dumps(
            {
                "snapshot_version": "test",
                "schema_version": "0.1.0",
                "exported_at": "2026-05-10T00:00:00Z",
                "tables": {},
            }
        )
    )
    return snap
