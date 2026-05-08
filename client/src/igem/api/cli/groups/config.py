"""``igem config`` — manage local client configuration (.igem.toml)."""
from __future__ import annotations

import click

from igem.config import (
    cwd_config_path,
    find_local_config_path,
    get_resolved_config_value,
    home_config_path,
    is_sensitive_key,
    load_igem_config,
    resolve_config_key,
    supported_config_keys,
    unset_local_config,
    write_local_config,
)


def _global_option(fn):
    return click.option(
        "--global",
        "is_global",
        is_flag=True,
        help="Target ~/.igem.toml instead of ./.igem.toml.",
    )(fn)


@click.group("config")
def config_group():
    """Manage local IGEM client configuration (.igem.toml).

    The local config lives at ``./.igem.toml`` (cwd-scoped) and the
    user-global config lives at ``~/.igem.toml``. The CLI walks up
    from cwd to find the closest local file and merges it on top of
    the home file when reading.
    """


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------
@config_group.command("set")
@click.argument("key", metavar="KEY")
@click.argument("value", metavar="VALUE")
@_global_option
def config_set(key: str, value: str, is_global: bool) -> None:
    """Set a configuration value.

    KEY must be one of:

      \b
      server-url    IGEM server base URL (e.g. https://geneexposure.org/api)
      api-key       Bearer API key for authenticated requests

    Existing sections and keys in the file are preserved; comments are
    not (the writer rebuilds the file).

    Examples:

      \b
      igem config set server-url https://geneexposure.org/api
      igem config set api-key abc123
      igem config set --global server-url https://geneexposure.org/api
    """
    try:
        section, attr = resolve_config_key(key)
    except ValueError as exc:
        valid = ", ".join(supported_config_keys())
        raise click.UsageError(f"{exc}\n\n  Valid keys: {valid}") from exc

    target = home_config_path() if is_global else cwd_config_path()
    path = write_local_config(section, attr, value, path=target)
    click.echo(f"set {key} = {value}")
    click.echo(f"  → {path}")


# ---------------------------------------------------------------------------
# unset
# ---------------------------------------------------------------------------
@config_group.command("unset")
@click.argument("key", metavar="KEY")
@_global_option
def config_unset(key: str, is_global: bool) -> None:
    """Remove a configuration value.

    Idempotent — unsetting an absent key prints a notice and exits
    cleanly. Empty sections are dropped after the key is removed; if
    the file becomes empty as a result it is deleted.

    Examples:

      \b
      igem config unset server-url
      igem config unset --global api-key
    """
    try:
        section, attr = resolve_config_key(key)
    except ValueError as exc:
        valid = ", ".join(supported_config_keys())
        raise click.UsageError(f"{exc}\n\n  Valid keys: {valid}") from exc

    target = home_config_path() if is_global else cwd_config_path()
    path, removed = unset_local_config(section, attr, path=target)
    if removed:
        click.echo(f"unset {key}")
        if path.exists():
            click.echo(f"  → {path}")
        else:
            click.echo(f"  → {path} (file removed, was empty)")
    else:
        click.echo(f"{key} was not set in {path}; nothing to do")


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------
@config_group.command("get")
@click.argument("key", metavar="KEY")
def config_get(key: str) -> None:
    """Print the resolved value of KEY.

    Honours the same precedence as the CLI itself:

      \b
      1. env var (IGEM_URL for server-url, IGEM_API_KEY for api-key)
      2. ./.igem.toml (closest walking up from cwd)
      3. ~/.igem.toml

    Exits with status 1 when the key is not set anywhere — useful in
    shell scripts (``if igem config get server-url > /dev/null; ...``).
    """
    try:
        resolve_config_key(key)
    except ValueError as exc:
        valid = ", ".join(supported_config_keys())
        raise click.UsageError(f"{exc}\n\n  Valid keys: {valid}") from exc

    val = get_resolved_config_value(key)
    if val is None:
        click.echo(f"{key} is not set", err=True)
        raise click.exceptions.Exit(1)
    click.echo(val)


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------
@config_group.command("show")
def config_show() -> None:
    """Print the merged config from local + home .igem.toml.

    Sensitive keys (e.g. ``api_key``) are masked. Use
    ``igem config get api-key`` to read the actual value.
    """
    cfg = load_igem_config()
    local = find_local_config_path()
    home = home_config_path()

    click.echo("# resolved IGEM config")
    click.echo(f"# local: {local if local else '(none)'}")
    click.echo(f"# home : {home if home.exists() else '(none)'}")
    click.echo()

    if not cfg:
        click.echo("# (empty)")
        return

    for section in sorted(cfg):
        click.echo(f"[{section}]")
        for key in sorted(cfg[section]):
            value = cfg[section][key]
            if is_sensitive_key(section, key):
                rendered = '"***"'
            elif isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, str):
                rendered = f'"{value}"'
            else:
                rendered = str(value)
            click.echo(f"{key} = {rendered}")
        click.echo()
