from __future__ import annotations

import click

from igem import __version__
from igem.api.cli.common import (
    api_key_option,
    debug_option,
    resolve_api_key,
    resolve_server_url,
    url_option,
)
from igem.api.cli.groups.report import report_group
from igem.igem import IGEM


@click.group()
@click.version_option(__version__, prog_name="igem")
def main():
    """IGEM — Integrative Genome-Exposome Method client."""


@main.command("health")
@url_option
@api_key_option
@debug_option
def health(url: str | None, api_key: str | None, debug: bool):
    """Check server health."""
    with IGEM(
        server_url=resolve_server_url(url),
        api_key=resolve_api_key(api_key),
        debug_mode=debug,
    ) as igem:
        result = igem.health()
    click.echo(f"status: {result['status']}")


main.add_command(report_group)


if __name__ == "__main__":
    main()
