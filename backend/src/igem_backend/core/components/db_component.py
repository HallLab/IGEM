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
        self.core.logger.log(f"Database created at: {self.core.db_uri}", "INFO")
        return True

    def upgrade(self) -> bool:
        """Re-apply seeds idempotently against an existing database."""
        db = self.require_db()
        db.upgrade_db()
        self.core.logger.log("Database upgraded (seeds applied).", "INFO")
        return True

    def get_session(self):
        """Convenience passthrough to the shared session context manager."""
        return self.require_db().get_session()

    def info(self) -> dict:
        """Return basic info about the current database connection."""
        db = self.require_db()
        from sqlalchemy.engine.url import make_url
        url = make_url(db.db_uri)
        return {
            "engine": url.drivername,
            "host": "local file" if url.drivername.startswith("sqlite") else url.host,
            "database": url.database,
            "connected": db.connected,
        }
