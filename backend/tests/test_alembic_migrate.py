"""
Unit tests for `igem_backend.modules.db.migrate`.

Covers all four `run_migration` actions across the three meaningful DB
states (empty, divergent-versioned, at-head), plus the defensive
behavior of `_mirror_revision_to_metadata` when the `igem_metadata`
table is missing.

All tests use SQLite + tmp_path; no external services required.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from igem_backend.modules.db.migrate import (
    _mirror_revision_to_metadata,
    get_head_revision,
    get_script_location,
    get_status,
    is_db_versioned,
    run_migration,
)

# The 0.1.0 baseline revision is a fixed point in history.
BASELINE_REV = "7a8b9c0d1e2f"

# The current head is resolved dynamically — it advances whenever a
# new migration is added. Tests assert behaviour against `HEAD_REV`
# rather than hardcoded strings so they keep passing through schema
# evolution.
HEAD_REV = get_head_revision(get_script_location())


# ---------------------------------------------------------------------------
# Helpers / sanity
# ---------------------------------------------------------------------------

def test_head_revision_is_non_empty_hex():
    """Repo head must be a 12-char hex alembic revision."""
    head = get_head_revision(get_script_location())
    assert isinstance(head, str)
    assert len(head) == 12
    assert all(c in "0123456789abcdef" for c in head)


def test_baseline_is_first_revision():
    """The fixed 0.1.0 baseline must remain reachable in the migration graph."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config()
    cfg.set_main_option("script_location", get_script_location())
    script = ScriptDirectory.from_config(cfg)
    all_revisions = {r.revision for r in script.walk_revisions()}
    assert BASELINE_REV in all_revisions


# ---------------------------------------------------------------------------
# get_status / is_db_versioned
# ---------------------------------------------------------------------------

def test_status_on_empty_db(empty_engine):
    engine, uri = empty_engine
    st = get_status(engine, uri)
    assert st.is_versioned is False
    assert st.current is None
    assert st.head == HEAD_REV
    assert st.is_up_to_date is False


def test_is_db_versioned_false_on_empty(empty_engine):
    engine, _ = empty_engine
    assert is_db_versioned(engine) is False


# ---------------------------------------------------------------------------
# action: status
# ---------------------------------------------------------------------------

def test_action_status_returns_true_and_prints(empty_engine, capsys):
    engine, uri = empty_engine
    assert run_migration(engine=engine, db_uri=uri, action="status") is True
    out = capsys.readouterr().out
    assert "Alembic status" in out
    assert HEAD_REV in out


# ---------------------------------------------------------------------------
# action: stamp-head
# ---------------------------------------------------------------------------

def test_stamp_head_on_empty_db_creates_alembic_version(empty_engine):
    engine, uri = empty_engine
    run_migration(engine=engine, db_uri=uri, action="stamp-head")

    st = get_status(engine, uri)
    assert st.is_versioned is True
    assert st.current == HEAD_REV
    assert st.is_up_to_date is True


def test_stamp_head_is_idempotent_when_at_head(empty_engine, capsys):
    engine, uri = empty_engine
    run_migration(engine=engine, db_uri=uri, action="stamp-head")
    capsys.readouterr()  # discard first-run output

    run_migration(engine=engine, db_uri=uri, action="stamp-head")
    out = capsys.readouterr().out
    assert "Already at head" in out


def test_stamp_head_refuses_versioned_at_different_revision(empty_engine):
    engine, uri = empty_engine
    # Simulate a divergent state: alembic_version exists but holds an
    # arbitrary other revision.
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE alembic_version "
                "(version_num VARCHAR(32) PRIMARY KEY)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO alembic_version (version_num) "
                "VALUES ('deadbeef0000')"
            )
        )

    with pytest.raises(RuntimeError, match="Refusing to stamp"):
        run_migration(engine=engine, db_uri=uri, action="stamp-head")


def test_stamp_head_with_force_overwrites_divergent_revision(empty_engine):
    engine, uri = empty_engine
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE alembic_version "
                "(version_num VARCHAR(32) PRIMARY KEY)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO alembic_version (version_num) "
                "VALUES ('deadbeef0000')"
            )
        )

    run_migration(
        engine=engine, db_uri=uri, action="stamp-head", force=True,
    )

    st = get_status(engine, uri)
    assert st.current == HEAD_REV


# ---------------------------------------------------------------------------
# action: upgrade
# ---------------------------------------------------------------------------

def test_upgrade_refuses_unversioned_db(empty_engine):
    engine, uri = empty_engine
    with pytest.raises(RuntimeError, match="not alembic-versioned"):
        run_migration(engine=engine, db_uri=uri, action="upgrade")


def test_upgrade_at_head_is_noop(empty_engine, capsys):
    engine, uri = empty_engine
    run_migration(engine=engine, db_uri=uri, action="stamp-head")
    capsys.readouterr()

    run_migration(engine=engine, db_uri=uri, action="upgrade")
    out = capsys.readouterr().out
    assert "up-to-date" in out.lower()


# ---------------------------------------------------------------------------
# action: dry-run
# ---------------------------------------------------------------------------

def test_dry_run_does_not_create_alembic_version(empty_engine):
    """
    Offline SQL generation must NOT touch the DB — even an empty,
    unstamped one. After dry-run the DB should remain unversioned.
    """
    engine, uri = empty_engine
    run_migration(engine=engine, db_uri=uri, action="dry-run")

    st = get_status(engine, uri)
    assert st.is_versioned is False
    assert st.current is None


# ---------------------------------------------------------------------------
# action: errors
# ---------------------------------------------------------------------------

def test_unknown_action_raises_value_error(empty_engine):
    engine, uri = empty_engine
    with pytest.raises(ValueError, match="Unknown action"):
        run_migration(engine=engine, db_uri=uri, action="bogus")


def test_run_migration_requires_engine():
    with pytest.raises(ValueError, match="engine is required"):
        run_migration(
            engine=None, db_uri="sqlite:///:memory:", action="status",
        )


def test_run_migration_requires_db_uri(empty_engine):
    engine, _ = empty_engine
    with pytest.raises(ValueError, match="db_uri is required"):
        run_migration(engine=engine, db_uri="", action="status")


# ---------------------------------------------------------------------------
# Defensive mirror
# ---------------------------------------------------------------------------

def test_mirror_revision_safe_when_metadata_table_missing(
    empty_engine_with_session,
):
    """
    The mirror must NOT raise when `igem_metadata` table is absent
    (brand-new DB right after `command.stamp` and before seeds).
    `alembic_version` is the source of truth — mirror is best-effort.
    """
    _engine, _uri, session_factory = empty_engine_with_session
    # No tables created — igem_metadata absent.
    _mirror_revision_to_metadata(session_factory, BASELINE_REV)


def test_mirror_revision_safe_when_session_factory_none():
    """Calling with None session_factory short-circuits cleanly."""
    _mirror_revision_to_metadata(None, BASELINE_REV)
