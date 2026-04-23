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

    Responsibilities:
    - Normalize & validate DB URI
    - Create SQLAlchemy Engine + Session factory
    - Bootstrap all ORM models into Base.metadata
    - Provide a unified Table resolver via db.table("name")
    """

    def __init__(self, db_uri: Optional[str] = None, log_level: str = "INFO"):
        self.logger = Logger(log_level=log_level)
        self.db_uri: Optional[str] = db_uri
        self.engine: Optional[Engine] = None
        self.SessionLocal = None
        self.connected: bool = False
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
        if "://" in uri:
            return uri
        return f"sqlite:///{os.path.abspath(uri)}"

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

    def exists_db(self, new_db: bool = False) -> bool:
        if not self.db_uri:
            return False

        try:
            url = make_url(self._normalize_uri(self.db_uri))
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
