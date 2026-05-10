"""
backend/src/igem_backend/alembic/env.py

Alembic environment script for IGEM-Server.

Goals
-----
- Work both from a checked-out repo and when installed from a wheel.
- Ensure all IGEM-Server models are loaded before autogenerate runs
  so the diff against the live DB is complete.
- Prevent autogenerate from "managing" objects that live outside
  Alembic's responsibility:
    * the pgvector `embedding` column on `entity_aliases`
    * future PostgreSQL partition children
      (e.g. `variant_masters_chr_1`)
- Allow overriding the DB URI at runtime via the `IGEM_DB_URI`
  environment variable.

See `docs/caderno/2026-05-10__001_*` for the design decisions behind
these filters.
"""

from __future__ import annotations

import os
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool


# ---------------------------------------------------------------------
# Alembic Config
# ---------------------------------------------------------------------

config = context.config

# Override sqlalchemy.url at runtime if IGEM_DB_URI is set.
db_uri_env = os.getenv("IGEM_DB_URI")
if db_uri_env:
    config.set_main_option("sqlalchemy.url", db_uri_env)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# ---------------------------------------------------------------------
# Target metadata
# ---------------------------------------------------------------------
# IMPORTANT: there is exactly ONE Base used by IGEM-Server. All ORM
# models attach to it. `bootstrap_models` triggers the import side
# effects that register every model into Base.metadata.

from igem_backend.modules.db.base import Base  # noqa: E402
from igem_backend.utils.db_loader import bootstrap_models  # noqa: E402

target_metadata = Base.metadata


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _ensure_metadata_loaded(connection) -> None:
    """
    Ensure all declarative models are registered into Base.metadata
    before Alembic compares DB vs metadata.
    """
    bootstrap_models(connection.engine)


def _include_object(
    object_: Any,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    """
    Filter objects that must NOT be managed by Alembic autogenerate.

    Returning False makes Alembic ignore the object during diff —
    autogenerate will not produce add/drop/alter statements for it.

    Excluded:

      1. `entity_aliases.embedding` — pgvector `Vector(768)` column.
         Alembic doesn't know about this type. Vector schema changes
         are rare and we manage them with hand-written migrations
         when needed.

      2. PostgreSQL partition children for variants
         (e.g. `variant_masters_chr_1`, `variant_snps_chr_X`). These
         are created via raw DDL inside their own migration, not by
         Alembic's table reflection.
    """
    if not name:
        return True

    # 1. PgVector column on entity_aliases
    if type_ == "column" and name == "embedding":
        return False

    # 2. Future PostgreSQL partition children for variants
    #    Pattern: variant_<base>_chr_<chrom>
    if (
        type_ == "table"
        and name.startswith("variant_")
        and "_chr_" in name
    ):
        return False

    return True


def _configure_context_offline(url: str) -> None:
    context.configure(
        url=url,
        target_metadata=target_metadata,
        include_object=_include_object,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )


def _configure_context_online(connection) -> None:
    _ensure_metadata_loaded(connection)
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=_include_object,
        compare_type=True,
        compare_server_default=True,
        # render_as_batch=True   # enable for SQLite ALTER TABLE if needed
    )


# ---------------------------------------------------------------------
# Entrypoints
# ---------------------------------------------------------------------
def run_migrations_offline() -> None:
    """Generate SQL script to stdout — no DB connection required."""
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError(
            "Missing sqlalchemy.url. Set it in alembic.ini or via "
            "the IGEM_DB_URI environment variable."
        )
    _configure_context_offline(url)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Connect to the DB and apply migrations."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        _configure_context_online(connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
