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
from igem.api.cli.groups.config import config_group
from igem.api.cli.groups.report import report_group
from igem.config import find_any_config_path
from igem.igem import IGEM


class IGEMGroup(click.Group):
    """
    Top-level Click group that appends a setup hint to the help output
    when no ``.igem.toml`` is reachable from the current directory or
    the user's home. The hint disappears once the user has configured
    a default server, so it stays useful for first-time users without
    nagging long-term ones.
    """

    def format_help(self, ctx: click.Context, formatter) -> None:
        super().format_help(ctx, formatter)
        if find_any_config_path() is not None:
            return
        formatter.write_paragraph()
        formatter.write_text(
            "Tip: no .igem.toml found in this directory (or any parent / "
            "your home). Set a default server URL so commands don't need "
            "--url every time:"
        )
        formatter.write_paragraph()
        with formatter.indentation():
            formatter.write_text(
                "igem config set server-url https://geneexposure.org/api"
            )


@click.group(cls=IGEMGroup, invoke_without_command=True)
@click.version_option(__version__, prog_name="igem")
@click.pass_context
def main(ctx: click.Context) -> None:
    """IGEM — Integrative Genome-Exposome Method client."""
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


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
main.add_command(config_group)


if __name__ == "__main__":
    main()
