from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from igem_backend.api.http.routers import reports as reports_router
from igem_backend.config import get_db_uri_from_config, settings
from igem_backend.ge import GE


def _resolve_lifespan_db_uri() -> str | None:
    """
    Same precedence as the CLI:
      1. IGEM_DB_URI env var
      2. DATABASE_URL env var
      3. .igem.toml [database] uri
    Reading env vars makes embedded:// mode work — the client sets
    IGEM_DB_URI before importing the FastAPI app.
    """
    return (
        os.getenv("IGEM_DB_URI")
        or os.getenv("DATABASE_URL")
        or get_db_uri_from_config()
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    db_uri = _resolve_lifespan_db_uri()
    app.state.ge = GE(db_uri=db_uri, debug_mode=settings.debug)
    try:
        yield
    finally:
        app.state.ge = None


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


app.include_router(reports_router.router, prefix=settings.api_v1_prefix)
