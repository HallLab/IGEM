from __future__ import annotations

import click

from igem.api.cli.common import (
    api_key_option,
    debug_option,
    resolve_api_key,
    resolve_server_url,
    url_option,
)
from igem.core.errors import IGEMAPIError
from igem.igem import IGEM
from igem.modules.reports.manager import read_identifier_file


@click.group("report")
def report_group():
    """Run IGEM reports against the server."""


@report_group.command("list")
@url_option
@api_key_option
@debug_option
def report_list(url: str | None, api_key: str | None, debug: bool):
    """List reports available on the server."""
    with IGEM(
        server_url=resolve_server_url(url),
        api_key=resolve_api_key(api_key),
        debug_mode=debug,
    ) as igem:
        reports = igem.reports.list()

    if not reports:
        click.echo("No reports registered.")
        return

    name_w = max(len(r.name) for r in reports) + 2
    ver_w = 10
    click.echo(f"{'Name':<{name_w}}{'Version':<{ver_w}}Description")
    click.echo("-" * (name_w + ver_w + 40))
    for r in reports:
        click.echo(f"{r.name:<{name_w}}{r.version:<{ver_w}}{r.description}")


@report_group.command("run")
@url_option
@api_key_option
@click.option("--name", "report_name", required=True, help="Report name.")
@click.option(
    "--input",
    "input_values",
    multiple=True,
    metavar="VALUE",
    help="Input value(s). Repeat for multiple.",
)
@click.option(
    "--input-file",
    "input_file",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a text file with one identifier per line.",
)
@click.option(
    "--assembly",
    default="GRCh38.p14",
    show_default=True,
    help="Genome assembly name for coordinate columns.",
)
@click.option(
    "--columns",
    default=None,
    metavar="COL1,COL2,...",
    help="Comma-separated columns to keep.",
)
@click.option(
    "--output",
    "output_path",
    default=None,
    metavar="FILE",
    help="Save results to a CSV file.",
)
@debug_option
def report_run(
    url: str | None,
    api_key: str | None,
    report_name: str,
    input_values: tuple[str, ...],
    input_file: str | None,
    assembly: str,
    columns: str | None,
    output_path: str | None,
    debug: bool,
):
    """Run a report and print its result or save to CSV."""
    col_list: list[str] | None = None
    if columns:
        col_list = [c.strip() for c in columns.split(",") if c.strip()]

    try:
        with IGEM(
            server_url=resolve_server_url(url),
            api_key=resolve_api_key(api_key),
            debug_mode=debug,
        ) as igem:
            if report_name == "gene_annotations":
                result = igem.reports.gene_annotations(
                    input_values=list(input_values) if input_values else None,
                    input_file=input_file,
                    assembly=assembly,
                    columns=col_list,
                    output_path=output_path,
                )
            else:
                params: dict = {}
                if input_values:
                    params["input_values"] = list(input_values)
                if input_file:
                    params.setdefault("input_values", []).extend(
                        read_identifier_file(input_file)
                    )
                if assembly:
                    params["assembly"] = assembly
                result = igem.reports.run(
                    report_name, params=params, columns=col_list
                )
                if output_path:
                    result.save_csv(output_path)
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    click.echo(result.message)
    click.echo(f"elapsed: {result.elapsed_seconds:.2f}s")
    click.echo(f"stats:   {result.stats}")
    if output_path:
        click.echo(f"saved:   {output_path}")
    else:
        click.echo("")
        click.echo(result.df.to_string(index=False, max_colwidth=30))
