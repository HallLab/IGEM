"""
IGEM ``report`` CLI group.

Two ways to run a report from the terminal:

1. **Typed subcommand** — ``igem report <report_name> [opts]``. Each
   registered report has its own subcommand with named flags for the
   report-specific kwargs (``--namespace`` for ``go_annotations``,
   ``--group-filter`` for ``disease_annotations``, etc.).

2. **Generic fallback** — ``igem report run --name <report_name> [opts]``.
   For server-side reports that don't yet have a typed CLI subcommand
   (newly added on the backend, before the client CLI catches up).

Plus two read-only operations:

- ``igem report list`` — registry overview.
- ``igem report explain <name>`` — markdown documentation for one report.
"""
from __future__ import annotations

from typing import Optional

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
from igem.modules.report.manager import read_identifier_file


# ---------------------------------------------------------------------------
# Helpers shared by every subcommand
# ---------------------------------------------------------------------------
def _common_run_options(func):
    """Stack the input/output flags every typed subcommand wants."""
    func = click.option(
        "--output", "output_path",
        default=None, metavar="FILE",
        help="Save results to a CSV file.",
    )(func)
    func = click.option(
        "--columns",
        default=None, metavar="COL1,COL2,...",
        help="Comma-separated columns to keep.",
    )(func)
    func = click.option(
        "--input-file", "input_file",
        default=None,
        type=click.Path(exists=True, dir_okay=False),
        help="Path to a text file with one identifier per line.",
    )(func)
    func = click.option(
        "--input", "input_values",
        multiple=True, metavar="VALUE",
        help="Input value(s). Repeat for multiple.",
    )(func)
    func = debug_option(func)
    func = api_key_option(func)
    func = url_option(func)
    return func


def _parse_columns(columns: Optional[str]) -> Optional[list[str]]:
    if not columns:
        return None
    return [c.strip() for c in columns.split(",") if c.strip()]


def _input_list_or_none(values: tuple[str, ...]) -> Optional[list[str]]:
    return list(values) if values else None


def _print_result(result, output_path: Optional[str]) -> None:
    click.echo(result.message)
    click.echo(f"elapsed: {result.elapsed_seconds:.2f}s")
    click.echo(f"stats:   {result.stats}")
    if output_path:
        click.echo(f"saved:   {output_path}")
    else:
        click.echo("")
        click.echo(result.df.to_string(index=False, max_colwidth=30))


def _open_igem(url: Optional[str], api_key: Optional[str], debug: bool) -> IGEM:
    return IGEM(
        server_url=resolve_server_url(url),
        api_key=resolve_api_key(api_key),
        debug_mode=debug,
    )


# ---------------------------------------------------------------------------
# Top-level group
# ---------------------------------------------------------------------------
@click.group("report")
def report_group():
    """Run IGEM reports against the server."""


# ---------------------------------------------------------------------------
# Read-only: list, explain
# ---------------------------------------------------------------------------
@report_group.command("list")
@url_option
@api_key_option
@debug_option
def report_list(url: Optional[str], api_key: Optional[str], debug: bool):
    """List reports available on the server."""
    with _open_igem(url, api_key, debug) as igem:
        reports = igem.report.list()

    if not reports:
        click.echo("No reports registered.")
        return

    name_w = max(len(r.name) for r in reports) + 2
    ver_w = 10
    click.echo(f"{'Name':<{name_w}}{'Version':<{ver_w}}Description")
    click.echo("-" * (name_w + ver_w + 40))
    for r in reports:
        click.echo(f"{r.name:<{name_w}}{r.version:<{ver_w}}{r.description}")


@report_group.command("explain")
@click.argument("name")
@url_option
@api_key_option
@debug_option
def report_explain(
    name: str, url: Optional[str], api_key: Optional[str], debug: bool,
):
    """Print the markdown documentation for one report."""
    try:
        with _open_igem(url, api_key, debug) as igem:
            text = igem.report.explain(name)
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc
    click.echo(text)


# ---------------------------------------------------------------------------
# Generic fallback for reports without a typed subcommand yet
# ---------------------------------------------------------------------------
@report_group.command("run")
@click.option("--name", "report_name", required=True, help="Report name.")
@_common_run_options
def report_run(
    report_name: str,
    url: Optional[str], api_key: Optional[str], debug: bool,
    input_values: tuple[str, ...], input_file: Optional[str],
    columns: Optional[str], output_path: Optional[str],
):
    """Generic fallback — run any report by name with the basic flags."""
    col_list = _parse_columns(columns)
    try:
        with _open_igem(url, api_key, debug) as igem:
            params: dict = {}
            if input_values:
                params["input_values"] = list(input_values)
            if input_file:
                params.setdefault("input_values", []).extend(
                    read_identifier_file(input_file)
                )
            result = igem.report.run(report_name, params=params, columns=col_list)
            if output_path:
                result.save_csv(output_path)
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    _print_result(result, output_path)


# ---------------------------------------------------------------------------
# Typed subcommands — one per registered report
# ---------------------------------------------------------------------------
@report_group.command("gene_annotations")
@_common_run_options
@click.option(
    "--assembly", default="GRCh38.p14", show_default=True,
    help="Genome assembly name for coordinate columns.",
)
def report_gene_annotations(
    url: Optional[str], api_key: Optional[str], debug: bool,
    input_values: tuple[str, ...], input_file: Optional[str],
    columns: Optional[str], output_path: Optional[str],
    assembly: str,
):
    """Run the gene_annotations report."""
    col_list = _parse_columns(columns)
    try:
        with _open_igem(url, api_key, debug) as igem:
            result = igem.report.gene_annotations(
                input_values=_input_list_or_none(input_values),
                input_file=input_file,
                assembly=assembly,
                columns=col_list,
                output_path=output_path,
            )
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    _print_result(result, output_path)


@report_group.command("disease_annotations")
@_common_run_options
@click.option(
    "--group-filter", default=None,
    help='Restrict to a named disease group (e.g. "autoimmune").',
)
def report_disease_annotations(
    url: Optional[str], api_key: Optional[str], debug: bool,
    input_values: tuple[str, ...], input_file: Optional[str],
    columns: Optional[str], output_path: Optional[str],
    group_filter: Optional[str],
):
    """Run the disease_annotations report."""
    col_list = _parse_columns(columns)
    try:
        with _open_igem(url, api_key, debug) as igem:
            result = igem.report.disease_annotations(
                input_values=_input_list_or_none(input_values),
                input_file=input_file,
                group_filter=group_filter,
                columns=col_list,
                output_path=output_path,
            )
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    _print_result(result, output_path)


@report_group.command("go_annotations")
@_common_run_options
@click.option(
    "--namespace", default=None,
    type=click.Choice(["BP", "MF", "CC"], case_sensitive=False),
    help="Restrict to a single GO namespace.",
)
def report_go_annotations(
    url: Optional[str], api_key: Optional[str], debug: bool,
    input_values: tuple[str, ...], input_file: Optional[str],
    columns: Optional[str], output_path: Optional[str],
    namespace: Optional[str],
):
    """Run the go_annotations report."""
    col_list = _parse_columns(columns)
    try:
        with _open_igem(url, api_key, debug) as igem:
            result = igem.report.go_annotations(
                input_values=_input_list_or_none(input_values),
                input_file=input_file,
                namespace=namespace,
                columns=col_list,
                output_path=output_path,
            )
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    _print_result(result, output_path)


@report_group.command("pathway_annotations")
@_common_run_options
def report_pathway_annotations(
    url: Optional[str], api_key: Optional[str], debug: bool,
    input_values: tuple[str, ...], input_file: Optional[str],
    columns: Optional[str], output_path: Optional[str],
):
    """Run the pathway_annotations report."""
    col_list = _parse_columns(columns)
    try:
        with _open_igem(url, api_key, debug) as igem:
            result = igem.report.pathway_annotations(
                input_values=_input_list_or_none(input_values),
                input_file=input_file,
                columns=col_list,
                output_path=output_path,
            )
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    _print_result(result, output_path)


@report_group.command("protein_annotations")
@_common_run_options
@click.option(
    "--include-pfam-details/--no-include-pfam-details", default=False,
    help="Populate per-type Pfam accession lists.",
)
@click.option(
    "--max-pfam-ids-per-type", default=10, show_default=True, type=int,
    help="Cap the Pfam accessions listed per type.",
)
def report_protein_annotations(
    url: Optional[str], api_key: Optional[str], debug: bool,
    input_values: tuple[str, ...], input_file: Optional[str],
    columns: Optional[str], output_path: Optional[str],
    include_pfam_details: bool, max_pfam_ids_per_type: int,
):
    """Run the protein_annotations report."""
    col_list = _parse_columns(columns)
    try:
        with _open_igem(url, api_key, debug) as igem:
            result = igem.report.protein_annotations(
                input_values=_input_list_or_none(input_values),
                input_file=input_file,
                include_pfam_details=include_pfam_details,
                max_pfam_ids_per_type=max_pfam_ids_per_type,
                columns=col_list,
                output_path=output_path,
            )
    except IGEMAPIError as exc:
        raise click.UsageError(str(exc)) from exc

    _print_result(result, output_path)
