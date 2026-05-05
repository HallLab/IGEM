from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from igem.config import settings
from igem.utils.logger import Logger

__version__ = "0.1.0"

EMBEDDED_SCHEME = "embedded://"


@dataclass
class IGEMCore:
    """
    Shared state container for all IGEM client components.

    Holds configuration (server URL, API key, debug flag), the singleton
    logger, and a lazily-created HTTP client. Components access this
    object through ``self.core`` (mirrors ``GECore`` in the backend).

    Three transport modes are supported, all sharing the same
    component-level API:

      http(s)://host         → real TCP via httpx.Client
      embedded:///path       → in-process FastAPI via TestClient
                               (zero network, snapshot-backed)

    The embedded mode is activated transparently by passing an
    ``embedded:///path/to/snapshot`` URL. The IGEM-Server is mounted
    in-process and reads the Parquet snapshot at ``path``; no extra
    process or port is required. See docs/caderno/2026-05-05_01.md.
    """

    server_url: str = field(default_factory=lambda: settings.default_server_url)
    api_key: Optional[str] = None
    debug_mode: bool = False
    timeout_seconds: float = field(
        default_factory=lambda: settings.request_timeout_seconds
    )
    version: str = field(default=__version__, init=False)

    logger: Logger = field(init=False)
    # Embedded mode uses fastapi.TestClient (still HTTP, but ASGI in-mem).
    # The type is left loose because TestClient is duck-compatible with
    # httpx.Client for our use (.get/.post/.close).
    _http: Optional[Any] = field(default=None, init=False, repr=False)

    @property
    def is_embedded(self) -> bool:
        return self.server_url.startswith(EMBEDDED_SCHEME)

    def __post_init__(self) -> None:
        self.logger = Logger(log_level="DEBUG" if self.debug_mode else "INFO")

        self.logger.log("=" * 44, "INFO")
        self.logger.log("IGEM — Client", "INFO")
        self.logger.log(f"  Version    : {self.version}", "INFO")
        self.logger.log(f"  Debug mode : {self.debug_mode}", "INFO")
        if self.is_embedded:
            self.logger.log(
                f"  Mode       : embedded (in-process FastAPI)", "INFO"
            )
            self.logger.log(
                f"  Snapshot   : {self.server_url[len(EMBEDDED_SCHEME):]}",
                "INFO",
            )
        else:
            self.logger.log(f"  Server URL : {self.server_url}", "INFO")
        self.logger.log("=" * 44, "INFO")

    @property
    def http(self) -> Any:
        """Lazily-initialised HTTP client. Reused across components."""
        if self._http is None:
            if self.is_embedded:
                self._http = self._build_embedded_http()
            else:
                self._http = self._build_network_http()
        return self._http

    # ------------------------------------------------------------------
    # Transport builders
    # ------------------------------------------------------------------
    def _build_network_http(self) -> httpx.Client:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return httpx.Client(
            base_url=self.server_url,
            timeout=self.timeout_seconds,
            headers=headers,
        )

    def _build_embedded_http(self) -> Any:
        """
        Mount the FastAPI app in-process and return a TestClient that
        speaks HTTP-over-ASGI. Requires `igem-backend` to be installed
        (extras = ["embedded"] when installing the client).
        """
        snapshot_path = self.server_url[len(EMBEDDED_SCHEME):]
        # Tell the backend's FastAPI lifespan to use the snapshot.
        # setdefault avoids stomping on a manually-set env var.
        os.environ.setdefault("IGEM_DB_URI", snapshot_path)

        try:
            from fastapi.testclient import TestClient
            from igem_backend.main import app
        except ImportError as exc:
            raise RuntimeError(
                "embedded:// transport requires igem-server to be "
                "installed alongside igem.\n"
                "    pip install igem-server\n"
                "(In local dev: `poetry install --with embedded` "
                "from the client/ directory.)"
            ) from exc

        # TestClient triggers FastAPI's lifespan only inside its own
        # `with` block. We enter it manually here so app.state.ge is
        # populated; close() does the matching __exit__ later.
        client = TestClient(app, base_url="http://embedded")
        client.__enter__()
        return client

    def close(self) -> None:
        if self._http is not None:
            try:
                if self.is_embedded:
                    # Trigger FastAPI lifespan shutdown to release the
                    # backend's GE / DB connection cleanly.
                    self._http.__exit__(None, None, None)
                else:
                    self._http.close()
            except Exception:
                pass
            self._http = None
