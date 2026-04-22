from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from igem_backend.core.components.db_component import DBComponent
from igem_backend.core.components.etl_component import ETLComponent
from igem_backend.core.components.nlp_component import NLPComponent
from igem_backend.core.components.report_component import ReportComponent
from igem_backend.modules.db.database import Database
from igem_backend.utils.logger import Logger

__version__ = "0.1.0"


@dataclass
class GECore:
    """
    Shared state container for all GE components.

    Holds the single active Database instance so every component
    reuses the same engine, session factory, and bootstrapped metadata.
    """

    db_uri: Optional[str]
    debug_mode: bool = False
    version: str = field(default=__version__, init=False)

    # Populated after __post_init__
    logger: Logger = field(init=False)
    db: Optional[Database] = field(default=None, init=False)
    _settings_cache: dict = field(default_factory=dict, init=False, repr=False)

    @staticmethod
    def _redact_uri(uri: Optional[str]) -> Optional[str]:
        if not uri or "://" not in uri:
            return uri
        try:
            p = urlsplit(uri)
            if p.password is None:
                return uri
            userinfo = f"{p.username}:***@" if p.username else ""
            host = p.hostname or ""
            port = f":{p.port}" if p.port else ""
            return urlunsplit((p.scheme, f"{userinfo}{host}{port}", p.path, p.query, p.fragment))
        except Exception:
            return "<db_uri_redacted>"

    def __post_init__(self):
        self.logger = Logger(log_level="DEBUG" if self.debug_mode else "INFO")

        self.logger.log("=" * 44, "INFO")
        self.logger.log("GE — Genome-Exposome Platform", "INFO")
        self.logger.log(f"  Version    : {self.version}", "INFO")
        self.logger.log(f"  Debug mode : {self.debug_mode}", "INFO")
        self.logger.log(f"  DB URI     : {self._redact_uri(self.db_uri)}", "INFO")
        self.logger.log("=" * 44, "INFO")

    def require_db(self) -> Database:
        if not self.db:
            raise RuntimeError(
                "Database not connected. Call ge.db.connect() or "
                "ge.db.create() first."
            )
        return self.db

    def settings(self) -> dict:
        """
        Load SystemConfig from DB into a key→value dict.
        Cached per session — call settings.refresh() to reload.
        """
        if self._settings_cache:
            return self._settings_cache

        try:
            db = self.require_db()
            from igem_backend.modules.db.models.model_config import SystemConfig
            with db.get_session() as session:
                rows = session.query(SystemConfig).all()
                self._settings_cache = {r.key: r.value for r in rows}
        except Exception:
            self._settings_cache = {}

        return self._settings_cache

    def get(self, key: str, default: str = "") -> str:
        return self.settings().get(key, default)


class GE:
    """
    Genome-Exposome (GE) — main Python API facade.

    Usage:
        ge = GE("sqlite:///igem.db")
        ge.db.connect()

        # ETL
        ge.etl.run(data_sources=["hgnc"])
        ge.etl.status()

        # NLP
        ge.nlp.resolve("TP53")

        # Reports (coming soon)
        ge.report.run("oxo_summary")

    The `ge` instance is also the entry point for the igem-server CLI.
    """

    def __init__(
        self,
        db_uri: Optional[str] = None,
        debug_mode: bool = False,
        auto_connect: bool = True,
    ):
        self.core = GECore(db_uri=db_uri, debug_mode=debug_mode)

        self.db = DBComponent(self.core)
        self.etl = ETLComponent(self.core)
        self.nlp = NLPComponent(self.core)
        self.report = ReportComponent(self.core)

        if auto_connect and self.core.db_uri:
            self.db.connect()

        # Back-reference so components can cross-call if needed
        self.core.db_component = self.db

    def __repr__(self) -> str:
        connected = getattr(self.core.db, "connected", False)
        return f"<GE version={self.core.version} db_uri={self.core._redact_uri(self.core.db_uri)} connected={connected}>"
