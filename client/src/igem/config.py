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


# ----------------------------------------------------------------------
# Config write/discover helpers (used by `igem config set`)
# ----------------------------------------------------------------------
# CLI-friendly key → (toml section, attribute) mapping. Both kebab and
# snake-case spellings are accepted; the file is always written using
# the snake_case form to stay consistent with how the loaders read it.
_CONFIG_KEY_MAP: dict[str, tuple[str, str]] = {
    "server-url": ("client", "server_url"),
    "server_url": ("client", "server_url"),
    "api-key":    ("client", "api_key"),
    "api_key":    ("client", "api_key"),
}

# (section, attr) tuples whose value should be masked in `igem config show`.
# Add new sensitive keys here as they appear.
_SENSITIVE_CONFIG_KEYS: set[tuple[str, str]] = {
    ("client", "api_key"),
}

# Env-var overrides honoured when reading config values back. Used by
# `igem config get` so the user sees the value the CLI would actually
# use, not just the toml on disk.
_CONFIG_ENV_OVERRIDES: dict[tuple[str, str], str] = {
    ("client", "server_url"): "IGEM_URL",
    ("client", "api_key"):    "IGEM_API_KEY",
}


def cwd_config_path() -> Path:
    """Path to ``./.igem.toml`` in the current working directory."""
    return Path.cwd() / _FILENAME


def home_config_path() -> Path:
    """Path to ``~/.igem.toml`` (the user-global config)."""
    return Path.home() / _FILENAME


def is_sensitive_key(section: str, attr: str) -> bool:
    return (section, attr) in _SENSITIVE_CONFIG_KEYS


def supported_config_keys() -> list[str]:
    """Kebab-case keys accepted by ``igem config set``."""
    return sorted({k for k in _CONFIG_KEY_MAP if "-" in k})


def resolve_config_key(cli_key: str) -> tuple[str, str]:
    """
    Translate a CLI key (``server-url``) into ``(section, attr)`` for
    the toml file. Raises ``ValueError`` for unknown keys.
    """
    if cli_key not in _CONFIG_KEY_MAP:
        raise ValueError(
            f"Unknown config key {cli_key!r}. "
            f"Valid keys: {', '.join(supported_config_keys())}"
        )
    return _CONFIG_KEY_MAP[cli_key]


def find_local_config_path() -> Optional[Path]:
    """Closest ``.igem.toml`` walking up from cwd, or None if not found."""
    for directory in (Path.cwd(), *Path.cwd().parents):
        candidate = directory / _FILENAME
        if candidate.exists():
            return candidate
    return None


def find_any_config_path() -> Optional[Path]:
    """
    Closest ``.igem.toml`` from the cwd walk-up, or ``~/.igem.toml`` as
    fallback. Used by the no-arg help to decide whether to suggest
    ``igem config set ...``.
    """
    local = find_local_config_path()
    if local is not None:
        return local
    home = Path.home() / _FILENAME
    if home.exists():
        return home
    return None


def write_local_config(
    section: str,
    key: str,
    value: str,
    *,
    path: Optional[Path] = None,
) -> Path:
    """
    Set a single ``[section] key = value`` in a ``.igem.toml`` file.

    Defaults to ``./.igem.toml`` (creating it if absent) and preserves
    any other sections / keys that already exist. Pass ``path=`` to
    target ``~/.igem.toml`` (or any other location). Comments in the
    file are not preserved — the writer rebuilds the file from the
    parsed dict.
    """
    target = path or cwd_config_path()
    existing = _load_toml(target) if target.exists() else {}
    existing.setdefault(section, {})[key] = value
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_dump_toml(existing), encoding="utf-8")
    return target


def unset_local_config(
    section: str,
    key: str,
    *,
    path: Optional[Path] = None,
) -> tuple[Path, bool]:
    """
    Remove ``[section] key`` from a ``.igem.toml`` file.

    Returns ``(path, removed)``. ``removed`` is True when the file was
    actually changed. Empty sections are dropped after the key is
    removed; if the file becomes empty as a result it is deleted.
    No-ops when the file or the key is not present.
    """
    target = path or cwd_config_path()
    if not target.exists():
        return target, False

    data = _load_toml(target)
    if key not in data.get(section, {}):
        return target, False

    del data[section][key]
    if not data[section]:
        del data[section]

    if not data:
        target.unlink()
    else:
        target.write_text(_dump_toml(data), encoding="utf-8")
    return target, True


def get_resolved_config_value(cli_key: str) -> Optional[str]:
    """
    Resolve a CLI key (``server-url``) against env vars and the merged
    toml config (cwd walk + ``~/.igem.toml``).

    Mirrors the precedence used elsewhere in the client: env var first,
    then the toml. Returns ``None`` when neither source has a value.
    """
    section, attr = resolve_config_key(cli_key)

    env_var = _CONFIG_ENV_OVERRIDES.get((section, attr))
    if env_var:
        env_val = os.getenv(env_var)
        if env_val and env_val.strip():
            return env_val.strip()

    cfg = load_igem_config()
    val = cfg.get(section, {}).get(attr)
    return None if val is None else str(val)


def _dump_toml(data: dict) -> str:
    """
    Minimal TOML serializer for the IGEM config schema.

    Supports nested dicts as ``[section]`` headers with str / int /
    float / bool leaf values. Sections and keys are alphabetised so
    successive writes produce a stable file.
    """
    lines: list[str] = []
    for section in sorted(data):
        values = data[section]
        if not isinstance(values, dict):
            raise ValueError(
                f"only [section] dicts are supported in IGEM config; "
                f"got {type(values).__name__} for {section!r}"
            )
        if lines:
            lines.append("")
        lines.append(f"[{section}]")
        for key in sorted(values):
            lines.append(f"{key} = {_dump_value(values[key])}")
    return "\n".join(lines) + "\n"


def _dump_value(v: object) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    raise ValueError(
        f"unsupported value type for IGEM config: {type(v).__name__}"
    )
