from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Optional

from pydantic import BaseModel


_FILENAME = ".igem.toml"


def _load_toml(path: Path) -> dict:
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(f"[igem] Invalid {path}: {exc}") from exc


def _find_toml(start: Path) -> tuple[dict, Path | None]:
    for directory in (start, *start.parents):
        candidate = directory / _FILENAME
        if candidate.exists():
            return _load_toml(candidate), directory
    return {}, None


def _merge(home: dict, local: dict) -> dict:
    merged: dict = {}
    for section in set(home) | set(local):
        merged[section] = {**(home.get(section, {})), **(local.get(section, {}))}
    return merged


def load_igem_config() -> dict:
    """Load .igem.toml by walking up from cwd, then merge with ~/.igem.toml."""
    home_cfg = _load_toml(Path.home() / _FILENAME)
    local_cfg, _ = _find_toml(Path.cwd())
    return _merge(home_cfg, local_cfg)


def get_server_url_from_config() -> Optional[str]:
    """
    Resolve the IGEM server URL in priority order:
      1. IGEM_URL env var
      2. .igem.toml [client] server_url
      3. None (caller decides default)
    """
    env = os.getenv("IGEM_URL")
    if env and env.strip():
        return env.strip()

    cfg = load_igem_config()
    return cfg.get("client", {}).get("server_url")


def get_api_key_from_config() -> Optional[str]:
    """
    Resolve the API key in priority order:
      1. IGEM_API_KEY env var
      2. .igem.toml [client] api_key
      3. None (auth not yet implemented; placeholder for future use)
    """
    env = os.getenv("IGEM_API_KEY")
    if env and env.strip():
        return env.strip()

    cfg = load_igem_config()
    return cfg.get("client", {}).get("api_key")


class ClientSettings(BaseModel):
    """Default values used when constructing IGEM without explicit args."""

    default_server_url: str = "http://localhost:8000"
    request_timeout_seconds: float = 60.0


settings = ClientSettings()
