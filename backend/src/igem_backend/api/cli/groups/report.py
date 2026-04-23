from __future__ import annotations

import sys

import click

from igem_backend.api.cli.common import (
    db_uri_option,
    debug_option,
    require_db_uri,
)


@click.group("report")
def report_group():
    """Curated analytical report commands."""


@report_group.command("list")
@db_uri_option
@debug_option
@click.pass_context
def report_list(ctx: click.Context, db_uri: str | None, debug: bool):
    """List all available reports."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)

    reports = ge.report.list()
    if not reports:
        click.echo("No reports registered.")
        return

    w_name = max(len(r["name"]) for r in reports) + 2
    w_ver = 10
    click.echo(f"{'Name':<{w_name}}{'Version':<{w_ver}}Description")
    click.echo("-" * (w_name + w_ver + 40))
    for r in reports:
        click.echo(
            f"{r['name']:<{w_name}}{r['version']:<{w_ver}}{r['description']}"
        )


@report_group.command("explain")
@click.option(
    "--name",
    "report_name",
    required=True,
    metavar="NAME",
    help="Report name (see 'report list').",
)
@db_uri_option
@debug_option
@click.pass_context
def report_explain(
    ctx: click.Context,
    report_name: str,
    db_uri: str | None,
    debug: bool,
):
    """Show documentation for a report."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    click.echo(ge.report.explain(report_name))


@report_group.command("run")
@db_uri_option
@click.option(
    "--name",
    "report_name",
    required=True,
    metavar="NAME",
    help="Report name (see 'report list').",
)
@click.option(
    "--input",
    "input_values",
    multiple=True,
    metavar="VALUE",
    help="Input value(s). Repeat for multiple. Omit to run for all genes.",
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
    help="Comma-separated columns to display. Default: all.",
)
@click.option(
    "--output",
    "output_path",
    default=None,
    metavar="FILE",
    help="Save results to a CSV file instead of printing.",
)
@debug_option
@click.pass_context
def report_run(
    ctx: click.Context,
    db_uri: str | None,
    report_name: str,
    input_values: tuple[str, ...],
    assembly: str,
    columns: str | None,
    output_path: str | None,
    debug: bool,
):
    """Run a report and print or save results."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)

    kwargs: dict = {
        "input_values": list(input_values) if input_values else [],
        "assembly": assembly,
    }
    if columns:
        kwargs["columns"] = columns

    try:
        df = ge.report.run(report_name, **kwargs)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    if df.empty:
        click.echo("No results.")
        return

    # Column selection
    if columns:
        col_list = [c.strip() for c in columns.split(",") if c.strip()]
        available = df.columns.tolist()
        unknown = [c for c in col_list if c not in available]
        if unknown:
            raise click.UsageError(
                f"Unknown column(s): {', '.join(unknown)}. "
                f"Available: {', '.join(available)}"
            )
        df = df[col_list]

    if output_path:
        df.to_csv(output_path, index=False)
        click.echo(f"Saved {len(df)} rows to {output_path}")
    else:
        _print_table(df)


def _print_table(df) -> None:
    """Print DataFrame as a compact aligned text table."""
    cols = df.columns.tolist()
    col_widths = [
        max(len(str(c)), df[c].astype(str).str.len().max())
        for c in cols
    ]
    # Cap each column at 30 chars for readability
    col_widths = [min(w, 30) for w in col_widths]

    header = "  ".join(str(c).ljust(w) for c, w in zip(cols, col_widths))
    sep = "  ".join("-" * w for w in col_widths)
    click.echo(header)
    click.echo(sep)

    for _, row in df.iterrows():
        line = "  ".join(
            str(v)[:w].ljust(w) if v is not None else "".ljust(w)
            for v, w in zip(row, col_widths)
        )
        click.echo(line)
