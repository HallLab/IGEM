from __future__ import annotations

from typing import Optional

from igem_backend.core.components.base_component import BaseComponent
from igem_backend.modules.db.database import Database


class DBComponent(BaseComponent):
    """
    Database lifecycle component.

    The only place where core.db is created or replaced.
    All other components consume core.db via core.require_db().
    """

    def connect(self, new_uri: Optional[str] = None) -> Database:
        """Connect to an existing database."""
        if new_uri:
            self.core.db_uri = new_uri

        # Idempotent: reuse if already connected to the same URI
        if (
            new_uri is None
            and self.core.db is not None
            and getattr(self.core.db, "connected", False)
            and getattr(self.core.db, "engine", None) is not None
        ):
            return self.core.db

        self.core.db = Database(self.core.db_uri)
        return self.core.db

    def create(self, db_uri: Optional[str] = None, overwrite: bool = False) -> bool:
        """Create a new database, bootstrap schema, and seed initial data."""
        if db_uri:
            self.core.db_uri = db_uri

        db = Database()
        db.db_uri = self.core.db_uri
        db.create_db(overwrite=overwrite)

        self.core.db = db
        return True

    def upgrade(self) -> bool:
        """Apply pending Alembic migrations and re-seed idempotently."""
        db = self.require_db()
        db.upgrade_db()
        return True

    def migrate(
        self,
        action: str,
        target: str = "head",
        force: bool = False,
    ) -> bool:
        """
        Run a low-level Alembic action against the active database.

        Wraps ``igem_backend.modules.db.migrate.run_migration``.

        Parameters
        ----------
        action:
            One of ``status``, ``upgrade``, ``stamp-head``, ``dry-run``.
        target:
            Revision target for ``upgrade`` / ``dry-run`` (default ``head``).
        force:
            Allow stamping a DB that already has an ``alembic_version`` row.
        """
        from igem_backend.modules.db.migrate import run_migration

        db = self.require_db()
        return run_migration(
            engine=db.engine,
            db_uri=db.db_uri,
            session_factory=db.SessionLocal,
            action=action,
            target=target,
            force=force,
        )

    def get_session(self):
        """Convenience passthrough to the shared session context manager."""
        return self.require_db().get_session()

    def session(self):
        """Alias for get_session() — preferred for REPL/API usage."""
        return self.require_db().get_session()

    def info(self) -> dict:
        """Return info about the current database connection."""
        db = self.require_db()
        info: dict = {
            "uri": db.db_uri,
            "backend": db.backend,
            "read_only": db.read_only,
            "connected": db.connected,
        }
        if db.backend == "sql":
            from sqlalchemy.engine.url import make_url
            url = make_url(db.db_uri)
            info["engine"] = url.drivername
            info["host"] = (
                "local file"
                if url.drivername.startswith("sqlite")
                else (url.host or "unknown")
            )
            info["database"] = url.database
        elif db.backend == "snapshot":
            info["snapshot_dir"] = (
                str(db.snapshot_dir) if db.snapshot_dir else None
            )
            if db.snapshot_dir:
                import json
                mf = db.snapshot_dir / "manifest.json"
                if mf.exists():
                    m = json.loads(mf.read_text())
                    info["snapshot_version"] = m.get("snapshot_version")
                    info["schema_version"] = m.get("schema_version")
                    info["tables_count"] = len(m.get("tables", {}))
                    info["exported_at"] = m.get("exported_at")
        return info

    def is_read_only(self) -> bool:
        """True if the active backend is read-only (snapshot mode)."""
        if self.core.db is None:
            return False
        return getattr(self.core.db, "read_only", False)

    def export_snapshot(
        self,
        output_dir: str,
        tables: list[str] | None = None,
        exclude: set[str] | None = None,
        chunksize: int = 50_000,
        compression: str = "zstd",
        snapshot_version: str | None = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Export the current database to a versioned Parquet snapshot dir.

        See `igem_backend.modules.db.snapshot_export.export_snapshot`
        for full parameter docs. Returns the manifest dict.
        """
        from igem_backend.modules.db.snapshot_export import (
            export_snapshot as _do_export,
        )
        db = self.require_db()
        return _do_export(
            engine=db.engine,
            output_dir=output_dir,
            tables=tables,
            exclude=exclude,
            chunksize=chunksize,
            compression=compression,
            snapshot_version=snapshot_version,
            igem_version=self.core.version,
            overwrite=overwrite,
        )
