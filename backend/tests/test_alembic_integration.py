"""
Integration tests:

  - `db create` end-to-end produces a stamped, versioned DB with
    `IgemMetadata.schema_revision` mirrored to the head revision.
  - CLI write commands (`db status`, `db stamp-head`, `db upgrade`,
    `db migrate-dry-run`) refuse to run against a snapshot URI.

Snapshot tests use a minimal valid manifest.json so the connect path
flips into read-only mode without needing real Parquet files.
"""
from __future__ import annotations

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine, text

from igem_backend.modules.db.migrate import (
    get_head_revision,
    get_script_location,
    get_status,
)

# Head revision resolved at import time — auto-updates as new migrations land.
HEAD_REV = get_head_revision(get_script_location())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_db(uri: str):
    """Drive `db create` programmatically and return the GE instance."""
    from igem_backend.ge import GE

    ge = GE(db_uri=uri, debug_mode=False, auto_connect=False)
    ge.db.create(overwrite=False)
    return ge


@pytest.fixture
def cli_main():
    from igem_backend.api.cli.main import main
    return main


@pytest.fixture
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# db create — Phase 3 integration: auto-stamp + mirror
# ---------------------------------------------------------------------------

def test_db_create_produces_stamped_versioned_db(sqlite_uri):
    _create_db(sqlite_uri)
    # Re-open a fresh engine to assert against the post-creation file.
    engine = create_engine(sqlite_uri, future=True)
    try:
        st = get_status(engine, sqlite_uri)
    finally:
        engine.dispose()

    assert st.is_versioned is True
    assert st.current == HEAD_REV
    assert st.is_up_to_date is True


def test_db_create_mirrors_revision_to_igem_metadata(sqlite_uri):
    _create_db(sqlite_uri)
    engine = create_engine(sqlite_uri, future=True)
    try:
        with engine.connect() as conn:
            rev = conn.execute(
                text("SELECT schema_revision FROM igem_metadata")
            ).scalar()
    finally:
        engine.dispose()
    assert rev == HEAD_REV


# ---------------------------------------------------------------------------
# CLI refusals on snapshot (read-only) URIs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "subcommand",
    ["status", "stamp-head", "upgrade", "migrate-dry-run"],
)
def test_cli_db_command_refuses_snapshot(
    fake_snapshot_dir, cli_main, runner, subcommand,
):
    """
    All four migration-related db subcommands must refuse to run when
    the active backend is a read-only snapshot.
    """
    snap_uri = f"parquet://{fake_snapshot_dir}"
    result = runner.invoke(
        cli_main,
        ["db", subcommand],
        env={"IGEM_DB_URI": snap_uri},
    )
    assert result.exit_code != 0, (
        f"`db {subcommand}` should fail on snapshot, "
        f"got exit_code={result.exit_code}\n{result.output}"
    )
    assert "snapshot" in result.output.lower(), (
        f"Refusal message should mention 'snapshot':\n{result.output}"
    )


# ---------------------------------------------------------------------------
# CLI happy path on a writable SQLite DB
# ---------------------------------------------------------------------------

def test_cli_db_status_on_stamped_db(sqlite_uri, cli_main, runner):
    """After `db create`, `db status` must report up-to-date."""
    _create_db(sqlite_uri)

    result = runner.invoke(
        cli_main,
        ["db", "status"],
        env={"IGEM_DB_URI": sqlite_uri},
    )
    assert result.exit_code == 0, result.output
    assert "up-to-date" in result.output.lower()
    assert HEAD_REV in result.output
