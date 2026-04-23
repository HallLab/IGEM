from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


# ── Pydantic settings (env vars / .env file) ──────────────────────────────────

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "IGEM API"
    debug: bool = False
    api_v1_prefix: str = "/api/v1"


settings = Settings()


# ── .igem.toml loader ─────────────────────────────────────────────────────────

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
    """Walk up from *start* until .igem.toml is found or filesystem root."""
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
    """
    Load .igem.toml by walking up from cwd (monorepo-friendly),
    then merge with ~/.igem.toml. The closest file wins.
    """
    home_cfg = _load_toml(Path.home() / _FILENAME)
    local_cfg, _ = _find_toml(Path.cwd())
    return _merge(home_cfg, local_cfg)


def get_db_uri_from_config() -> str | None:
    """
    Return the database URI from .igem.toml.

    Relative SQLite paths are resolved relative to the directory that
    contains the .igem.toml file, not the current working directory.
    This makes `uri = "sqlite:///ge.db"` work correctly when the CLI is
    invoked from any subdirectory of the project.
    """
    home_cfg = _load_toml(Path.home() / _FILENAME)
    local_cfg, toml_dir = _find_toml(Path.cwd())
    uri: str | None = _merge(home_cfg, local_cfg).get("database", {}).get("uri")

    if uri and toml_dir and uri.lower().startswith("sqlite:///"):
        db_path = uri[len("sqlite:///"):]
        if db_path and not Path(db_path).is_absolute():
            uri = f"sqlite:///{(toml_dir / db_path).resolve()}"

    return uri
