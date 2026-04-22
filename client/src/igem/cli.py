from __future__ import annotations

import click

from igem import IGEM, __version__


def _url_option(fn):
    return click.option(
        "--url",
        default="http://localhost:8000",
        show_default=True,
        envvar="IGEM_URL",
        help="IGEM server base URL.",
    )(fn)


@click.group()
@click.version_option(__version__, prog_name="igem")
def main():
    """IGEM — Integrative Genome-Exposome Method client."""


@main.command("health")
@_url_option
def health(url: str):
    """Check server health."""
    with IGEM(base_url=url) as igem:
        result = igem.health()
    click.echo(f"status: {result.status}")
