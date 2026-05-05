from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import Table, create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.orm import sessionmaker

from igem_backend.modules.db.base import Base
from igem_backend.modules.db.create_db_mixin import CreateDBMixin
from igem_backend.utils.db_loader import bootstrap_models
from igem_backend.utils.logger import Logger


class Database(CreateDBMixin):
    """
    Central DB access layer for IGEM.

    Two backends are supported:

    1. **SQL backend** (PostgreSQL or SQLite) — full feature set: ETL,
       writes, transactions, NLP runtime, curator. URI examples:
         postgresql://user:pass@host/igem
         sqlite:///path/to/igem.db

    2. **Snapshot backend** (DuckDB over Parquet) — read-only mode for
       HPC consumption and offline analysis. URI examples:
         parquet:///path/to/snapshot/   (explicit scheme)
         /path/to/snapshot/             (path inferred as snapshot)

    The backend is auto-detected from the URI by `_classify_uri()`.
    Write operations (ETL, db create/upgrade/export) raise in
    snapshot mode via the `read_only` flag.
    """

    def __init__(self, db_uri: Optional[str] = None, log_level: str = "INFO"):
        self.logger = Logger(log_level=log_level)
        self.db_uri: Optional[str] = db_uri
        self.engine: Optional[Engine] = None
        self.SessionLocal = None
        self.connected: bool = False
        self.read_only: bool = False
        self.backend: str = "unknown"        # "sql" | "snapshot"
        self.snapshot_dir: Optional[Path] = None
        self._tables: Dict[str, Table] = {}

        if self.db_uri:
            self.connect()

    # -------------------------------------------------------------------------
    # URI helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = os.getenv(name)
        try:
            return int(str(raw).strip()) if raw else int(default)
        except Exception:
            return int(default)

    def _engine_kwargs(self, url: URL | str) -> dict:
        parsed = make_url(str(url))
        kwargs: dict = {"future": True}

        if parsed.drivername.startswith("postgresql"):
            connect_args = {
                "connect_timeout": self._env_int("IGEM_DB_CONNECT_TIMEOUT", 10),
                "application_name": os.getenv("IGEM_DB_APPLICATION_NAME", "igem"),
                "keepalives": self._env_int("IGEM_DB_KEEPALIVES", 1),
                "keepalives_idle": self._env_int("IGEM_DB_KEEPALIVES_IDLE", 30),
                "keepalives_interval": self._env_int("IGEM_DB_KEEPALIVES_INTERVAL", 10),
                "keepalives_count": self._env_int("IGEM_DB_KEEPALIVES_COUNT", 5),
            }
            kwargs.update(
                {
                    "pool_pre_ping": True,
                    "pool_recycle": self._env_int("IGEM_DB_POOL_RECYCLE", 1800),
                    "connect_args": connect_args,
                }
            )

        return kwargs

    def _normalize_uri(self, uri: str) -> str:
        # Snapshot URI: keep as-is, paths are absolute below
        if uri.startswith("parquet://"):
            return uri
        # Bare path → check if it's a directory containing a manifest
        # (snapshot) before falling back to sqlite path semantics.
        if "://" not in uri:
            p = Path(uri).expanduser().resolve()
            if p.is_dir() and (p / "manifest.json").exists():
                # Path is absolute (starts with /); produce parquet://
                # + path → "parquet:///abs/path" total 3 slashes after
                # the colon, matching standard URI convention.
                return f"parquet://{p}"
            return f"sqlite:///{os.path.abspath(uri)}"
        return uri

    def _classify_uri(self, uri: str) -> str:
        """Return 'sql' or 'snapshot' for a normalized URI."""
        if uri.startswith("parquet://"):
            return "snapshot"
        return "sql"

    @staticmethod
    def _snapshot_path(uri: str) -> Path:
        """Extract the snapshot dir from a parquet:// URI."""
        # parquet:///abs/path  → strip "parquet://" leaving "/abs/path"
        rest = uri[len("parquet://"):]
        return Path(rest if rest.startswith("/") else f"/{rest}")

    # -------------------------------------------------------------------------
    # Connect / exists
    # -------------------------------------------------------------------------
    def connect(self, new_uri: Optional[str] = None, check_exists: bool = True) -> None:
        if new_uri:
            self.db_uri = new_uri

        if not self.db_uri:
            raise ValueError("db_uri must be provided to connect().")

        if self.engine is not None:
            try:
                self.engine.dispose()
            except Exception:
                pass

        self._tables.clear()
        self.db_uri = self._normalize_uri(self.db_uri)
        self.backend = self._classify_uri(self.db_uri)

        if self.backend == "snapshot":
            self._connect_snapshot()
            return

        # --- SQL backend (postgres / sqlite) ---
        if check_exists and not self.exists_db():
            msg = f"Database not found at {self.db_uri}"
            self.logger.log(msg, "ERROR")
            raise ValueError(msg)

        start = time.perf_counter()
        self.engine = create_engine(self.db_uri, **self._engine_kwargs(self.db_uri))

        if make_url(self.db_uri).drivername.startswith("sqlite"):
            @event.listens_for(self.engine, "connect")
            def _set_sqlite_pragmas(dbapi_conn, _record):
                cur = dbapi_conn.cursor()
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA synchronous=NORMAL")
                cur.execute("PRAGMA cache_size=-65536")   # 64 MB page cache
                cur.execute("PRAGMA temp_store=MEMORY")
                cur.close()

        bootstrap_models(self.engine)

        self.SessionLocal = sessionmaker(
            bind=self.engine, future=True, expire_on_commit=False
        )

        elapsed_ms = (time.perf_counter() - start) * 1000

        try:
            url = make_url(self.db_uri)
            engine_name = url.drivername
            host = "local file" if url.drivername.startswith("sqlite") else (url.host or "<unknown>")
            db_name = url.database or "<unknown>"
        except Exception:
            engine_name = host = db_name = "<unknown>"

        self.logger.log("Database connection established", "INFO")
        self.logger.log(f"  Engine: {engine_name} | Host: {host} | DB: {db_name} | {elapsed_ms:.1f}ms", "INFO")
        self.connected = True
        self.read_only = False

    # -------------------------------------------------------------------------
    # Snapshot (DuckDB + Parquet) connection
    # -------------------------------------------------------------------------
    def _connect_snapshot(self) -> None:
        """
        Build a DuckDB engine that exposes the snapshot's parquet files
        as virtual views with the same names as the original DB tables.
        ORM column-projection queries work transparently against this
        engine; writes will fail (read-only mode).
        """
        import json

        snap = self._snapshot_path(self.db_uri)
        if not snap.exists() or not snap.is_dir():
            raise ValueError(f"Snapshot directory not found: {snap}")

        manifest_path = snap / "manifest.json"
        if not manifest_path.exists():
            raise ValueError(
                f"Missing manifest.json in {snap} — not a valid snapshot."
            )
        manifest = json.loads(manifest_path.read_text())
        self.snapshot_dir = snap

        start = time.perf_counter()
        # In-memory DuckDB; views recreate on every fresh connection.
        # NullPool keeps things simple — single-process workloads.
        from sqlalchemy.pool import NullPool

        self.engine = create_engine(
            "duckdb:///:memory:",
            future=True,
            poolclass=NullPool,
        )
        # Stash the snapshot path on the engine so downstream code (e.g.
        # AliasDictionary) can find the pre-built NLP cache without
        # needing the Database wrapper directly.
        self.engine.snapshot_dir = snap  # type: ignore[attr-defined]

        # Each new connection needs the views set up. DuckDB creates
        # one connection per session in NullPool, so this listener
        # ensures every session sees the snapshot tables.
        snap_str = str(snap)
        manifest_tables = manifest.get("tables", {})

        @event.listens_for(self.engine, "connect")
        def _create_snapshot_views(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            for table_name, meta in manifest_tables.items():
                file_path = snap / meta.get("file", f"{table_name}.parquet")
                if not file_path.exists():
                    continue
                cur.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS "
                    f"SELECT * FROM read_parquet('{file_path}')"
                )
            cur.close()

        bootstrap_models(self.engine)

        self.SessionLocal = sessionmaker(
            bind=self.engine, future=True, expire_on_commit=False
        )

        elapsed_ms = (time.perf_counter() - start) * 1000
        n_tables = len(manifest_tables)
        version = manifest.get("snapshot_version", "?")

        self.logger.log("Snapshot connection established (read-only)", "INFO")
        self.logger.log(
            f"  Backend: parquet | Path: {snap} | "
            f"Version: {version} | Tables: {n_tables} | {elapsed_ms:.1f}ms",
            "INFO",
        )
        self.connected = True
        self.read_only = True

    def exists_db(self, new_db: bool = False) -> bool:
        if not self.db_uri:
            return False

        normalized = self._normalize_uri(self.db_uri)

        # Snapshot: directory + manifest must exist
        if normalized.startswith("parquet://"):
            snap = self._snapshot_path(normalized)
            return snap.is_dir() and (snap / "manifest.json").exists()

        try:
            url = make_url(normalized)
        except Exception:
            return False

        if url.drivername.startswith("sqlite"):
            path = url.database
            return bool(path) and Path(path).exists()

        if url.drivername.startswith("postgresql"):
            engine = self.engine or create_engine(url, **self._engine_kwargs(url))
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            except Exception as e:
                if not new_db:
                    self.logger.log(f"Could not connect to database: {e}", "ERROR")
                return False

        return False

    # -------------------------------------------------------------------------
    # Session / Table access
    # -------------------------------------------------------------------------
    def get_session(self):
        if not self.SessionLocal:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.SessionLocal()

    def table(self, name: str) -> Table:
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")
        if name in self._tables:
            return self._tables[name]
        if name in Base.metadata.tables:
            t = Base.metadata.tables[name]
        else:
            t = Table(name, Base.metadata, autoload_with=self.engine)
        self._tables[name] = t
        return t
