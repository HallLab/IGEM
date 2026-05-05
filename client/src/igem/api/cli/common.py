from __future__ import annotations

import os

import click

from igem.config import (
    get_api_key_from_config,
    get_server_url_from_config,
    settings,
)


def _clean(value: str | None) -> str | None:
    return value.strip() if value and value.strip() else None


def resolve_server_url(cli_url: str | None = None) -> str:
    """
    Resolve the IGEM server URL in priority order:
      1. ``--url`` flag
      2. IGEM_URL env var
      3. ``.igem.toml [client] server_url``
      4. ``settings.default_server_url``
    """
    return (
        _clean(cli_url)
        or _clean(os.getenv("IGEM_URL"))
        or get_server_url_from_config()
        or settings.default_server_url
    )


def resolve_api_key(cli_key: str | None = None) -> str | None:
    return (
        _clean(cli_key)
        or _clean(os.getenv("IGEM_API_KEY"))
        or get_api_key_from_config()
    )


def url_option(fn):
    return click.option(
        "--url",
        required=False,
        type=click.STRING,
        envvar="IGEM_URL",
        help="IGEM server base URL (also reads .igem.toml [client] server_url).",
    )(fn)


def api_key_option(fn):
    return click.option(
        "--api-key",
        required=False,
        type=click.STRING,
        envvar="IGEM_API_KEY",
        help="API key for authenticated requests (auth not enforced yet).",
    )(fn)


def debug_option(fn):
    return click.option(
        "--debug",
        is_flag=True,
        help="Enable debug logging.",
    )(fn)
