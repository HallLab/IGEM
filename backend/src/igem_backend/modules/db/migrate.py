"""
High-level Alembic wrapper for IGEM-Server.

Exposes four actions used by the CLI (`igem-server db ...`) and any
programmatic caller:

  - ``status``      — print current vs head revision
  - ``upgrade``     — apply pending migrations (default: to head)
  - ``stamp-head``  — mark DB as being at head WITHOUT running DDL
                      (used to baseline an existing production DB)
  - ``dry-run``     — print SQL that would be applied; no execution

The Alembic-managed `alembic_version` table is the **source of truth**
for the current schema revision. As a convenience, we also mirror the
revision into ``IgemMetadata.schema_revision`` so it is visible to
external SQL clients / dashboards without needing to query Alembic.

See ``docs/caderno/2026-05-10__001_*`` for the full design.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from typing import Optional

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory


# ---------------------------------------------------------------------------
# Paths / Alembic helpers
# ---------------------------------------------------------------------------

def get_script_location() -> str:
    """
    Resolve the absolute path of the ``alembic/`` directory shipped with
    the ``igem_backend`` package. Works in editable installs and wheels.
    """
    return str(files("igem_backend") / "alembic")


def _make_alembic_config(script_location: str, db_uri: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", script_location)
    cfg.set_main_option("sqlalchemy.url", db_uri)
    return cfg


def get_head_revision(script_location: str) -> str:
    """The single head revision declared by the migration scripts."""
    cfg = Config()
    cfg.set_main_option("script_location", script_location)
    script = ScriptDirectory.from_config(cfg)
    heads = script.get_heads()
    if len(heads) != 1:
        raise RuntimeError(f"Expected single Alembic head, got: {heads}")
    return heads[0]


def get_db_revision(engine) -> Optional[str]:
    """Read the current revision from the DB. Returns None if unstamped."""
    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn)
        return ctx.get_current_revision()


def is_db_versioned(engine) -> bool:
    """True if the DB has the ``alembic_version`` table."""
    try:
        with engine.connect() as conn:
            return engine.dialect.has_table(conn, "alembic_version")
    except Exception:
        return get_db_revision(engine) is not None


@dataclass
class MigrationStatus:
    script_location: str
    head: str
    current: Optional[str]
    is_versioned: bool

    @property
    def is_up_to_date(self) -> bool:
        return self.current is not None and self.current == self.head


def get_status(engine, db_uri: str) -> MigrationStatus:
    script_location = get_script_location()
    head = get_head_revision(script_location)
    current = get_db_revision(engine)
    versioned = is_db_versioned(engine)
    return MigrationStatus(
        script_location=script_location,
        head=head,
        current=current,
        is_versioned=versioned,
    )


# ---------------------------------------------------------------------------
# Mirror revision into IgemMetadata for external visibility
# ---------------------------------------------------------------------------

def _mirror_revision_to_metadata(session_factory, revision: str) -> None:
    """
    Update ``IgemMetadata.schema_revision`` to match the Alembic head.

    Best-effort and **non-fatal**: ``alembic_version`` is the source of
    truth. If the ``igem_metadata`` table doesn't exist (brand-new DB),
    has no rows yet (un-seeded), or the column is missing (older
    schema), we silently skip — the mirror is purely for human
    visibility and must not break the migration operation.
    """
    if not session_factory:
        return

    try:
        from igem_backend.modules.db.models.model_config import (
            IgemMetadata,
        )
        session = session_factory()
        try:
            meta = session.query(IgemMetadata).first()
            if meta is not None:
                meta.schema_revision = revision
                session.commit()
        except Exception:
            # Table missing, schema older, etc. — non-fatal.
            session.rollback()
        finally:
            session.close()
    except Exception:
        # Defensive: never let mirror failure propagate.
        pass


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_migration(
    *,
    engine,
    db_uri: str,
    session_factory=None,
    action: str = "upgrade",
    target: str = "head",
    force: bool = False,
) -> bool:
    """
    Execute the requested migration action.

    Parameters
    ----------
    engine:
        Active SQLAlchemy engine (already connected to the target DB).
    db_uri:
        Connection URI — passed to Alembic so it can open its own
        connections during the run.
    session_factory:
        Optional ``sessionmaker``. When provided, the head revision is
        mirrored into ``IgemMetadata.schema_revision`` after upgrade /
        stamp, for visibility from outside Alembic.
    action:
        One of ``status``, ``upgrade``, ``stamp-head``, ``dry-run``.
    target:
        Revision target for upgrade / dry-run. Default ``"head"``.
    force:
        Allow stamping an already-versioned DB (rare; e.g. after a
        manual edit of ``alembic_version``).
    """
    if engine is None:
        raise ValueError("engine is required")
    if not db_uri:
        raise ValueError("db_uri is required")

    st = get_status(engine, db_uri)

    # ----- STATUS -----
    if action == "status":
        print("Alembic status")
        print(f"  DB URI         : {db_uri}")
        print(f"  Script location: {st.script_location}")
        print(f"  Repo head      : {st.head}")
        print(f"  DB revision    : {st.current}")
        print(f"  Versioned?     : {st.is_versioned}")
        if st.is_up_to_date:
            print("  → up-to-date.")
        elif not st.is_versioned:
            print(
                "  → DB is NOT alembic-versioned. "
                "Use `db stamp-head` for an existing production schema."
            )
        else:
            print("  → pending migrations available.")
        return True

    # ----- STAMP-HEAD -----
    if action == "stamp-head":
        if st.is_up_to_date:
            print(f"Already at head (revision={st.current}); nothing to do.")
            return True

        if st.is_versioned and not force:
            raise RuntimeError(
                "Refusing to stamp: DB already has an alembic_version row. "
                "Use force=True to overwrite (dangerous)."
            )

        cfg = _make_alembic_config(st.script_location, db_uri)
        print(f"Stamping DB to head revision: {st.head}")
        # When forcing over a divergent alembic_version row, purge=True
        # wipes the existing row first so alembic doesn't try to walk
        # from an unknown revision (which would raise CommandError).
        purge = bool(force and st.is_versioned)
        command.stamp(cfg, st.head, purge=purge)
        _mirror_revision_to_metadata(session_factory, st.head)
        print("Stamp complete.")
        return True

    # ----- DRY-RUN -----
    if action == "dry-run":
        cfg = _make_alembic_config(st.script_location, db_uri)
        print("Dry-run (SQL only — no execution)")
        print(f"  From: {st.current}")
        print(f"  To  : {target}")
        command.upgrade(cfg, target, sql=True)
        return True

    # ----- UPGRADE -----
    if action != "upgrade":
        raise ValueError(f"Unknown action: {action}")

    if st.is_up_to_date and target in ("head", st.head):
        print(f"Schema up-to-date (revision={st.current}); nothing to do.")
        return True

    if not st.is_versioned and st.current is None and not force:
        raise RuntimeError(
            "Database is not alembic-versioned (no alembic_version row).\n"
            "If this is an existing production DB with the schema already "
            "in place, run `igem-server db stamp-head` first.\n"
            "If you really mean to run upgrade against an unversioned DB, "
            "set force=True."
        )

    cfg = _make_alembic_config(st.script_location, db_uri)
    print("Running Alembic migrations")
    print(f"  From: {st.current}")
    print(f"  To  : {target}")
    command.upgrade(cfg, target)

    st_after = get_status(engine, db_uri)
    if st_after.current:
        _mirror_revision_to_metadata(session_factory, st_after.current)

    print(f"Migration complete: {st.current} → {st_after.current}")
    return True
