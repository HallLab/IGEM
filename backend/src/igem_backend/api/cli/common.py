from __future__ import annotations

import os

import click


def _clean(value: str | None) -> str | None:
    return value.strip() if value and value.strip() else None


def try_resolve_db_uri(cli_db_uri: str | None = None) -> str | None:
    """
    Resolve DB URI in priority order:
    1) --db-uri flag (cli_db_uri)
    2) DATABASE_URL env var
    3) IGEM_DB_URI env var
    """
    return (
        _clean(cli_db_uri)
        or _clean(os.getenv("DATABASE_URL"))
        or _clean(os.getenv("IGEM_DB_URI"))
    )


def require_db_uri(ctx: click.Context, local_db_uri: str | None = None) -> str:
    """Resolve URI from local flag → ctx.obj → env vars. Raises UsageError if none found."""
    uri = try_resolve_db_uri(local_db_uri) or try_resolve_db_uri((ctx.obj or {}).get("db_uri"))
    if not uri:
        raise click.UsageError(
            "Database URI not set.\n"
            "  Use --db-uri, or set DATABASE_URL / IGEM_DB_URI environment variable.\n\n"
            "  Examples:\n"
            "    igem-server --db-uri sqlite:///igem.db db create\n"
            "    IGEM_DB_URI=postgresql://user:pass@localhost/igem igem-server db create"
        )
    return uri


def db_uri_option(fn):
    return click.option(
        "--db-uri",
        required=False,
        type=click.STRING,
        help="Database URI (sqlite:///path or postgresql://user:pass@host/db).",
    )(fn)


def debug_option(fn):
    return click.option("--debug", is_flag=True, help="Enable debug logging.")(fn)
