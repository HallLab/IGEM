from __future__ import annotations

import click

from igem_backend.api.cli.common import db_uri_option, debug_option, require_db_uri


@click.group("etl")
def etl_group():
    """ETL pipeline management commands."""


@etl_group.command("run")
@db_uri_option
@click.option(
    "--source",
    "sources",
    multiple=True,
    metavar="NAME",
    help="Data source name(s) to run. Repeat for multiple. Omit to run all.",
)
@click.option(
    "--system",
    "source_system",
    default=None,
    help="Filter by source system name.",
)
@click.option(
    "--steps",
    default="extract,transform,load",
    show_default=True,
    help="Comma-separated steps to run (extract,transform,load).",
)
@click.option(
    "--force",
    "force_steps",
    default=None,
    help="Comma-separated steps to force even if hash unchanged.",
)
@debug_option
@click.pass_context
def etl_run(
    ctx: click.Context,
    db_uri: str | None,
    sources: tuple[str, ...],
    source_system: str | None,
    steps: str,
    force_steps: str | None,
    debug: bool,
):
    """Run ETL pipeline for one or more data sources."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)

    step_list = [s.strip() for s in steps.split(",")]
    force_list = [s.strip() for s in force_steps.split(",")] if force_steps else []
    source_list = list(sources) if sources else None

    if source_list:
        ge.etl.run(
            data_sources=source_list,
            source_system=source_system,
            steps=step_list,
            force_steps=force_list,
        )
    else:
        ge.etl.run_all(
            source_system=source_system,
            steps=step_list,
            force_steps=force_list,
        )


@etl_group.command("status")
@db_uri_option
@debug_option
@click.pass_context
def etl_status(ctx: click.Context, db_uri: str | None, debug: bool):
    """Show latest ETL run status for all data sources."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    rows = ge.etl.status()

    if not rows:
        click.echo("No ETL runs recorded.")
        return

    col_w = [20, 12, 10, 22, 22]
    header = (
        f"{'Data Source':<{col_w[0]}}"
        f"{'Step':<{col_w[1]}}"
        f"{'Status':<{col_w[2]}}"
        f"{'Started':<{col_w[3]}}"
        f"{'Finished':<{col_w[4]}}"
    )
    click.echo(header)
    click.echo("-" * sum(col_w))
    for row in rows:
        click.echo(
            f"{str(row.get('data_source', '')):<{col_w[0]}}"
            f"{str(row.get('step', '')):<{col_w[1]}}"
            f"{str(row.get('status', '')):<{col_w[2]}}"
            f"{str(row.get('started_at', '')):<{col_w[3]}}"
            f"{str(row.get('finished_at', '')):<{col_w[4]}}"
        )


@etl_group.command("rollback")
@db_uri_option
@click.option(
    "--source",
    "sources",
    multiple=True,
    metavar="NAME",
    help="Data source name(s) to roll back.",
)
@click.option(
    "--package-id",
    "package_ids",
    multiple=True,
    type=int,
    help="Specific ETLPackage IDs to roll back.",
)
@click.option("--delete-files", is_flag=True, help="Also delete downloaded/processed files.")
@debug_option
@click.pass_context
def etl_rollback(
    ctx: click.Context,
    db_uri: str | None,
    sources: tuple[str, ...],
    package_ids: tuple[int, ...],
    delete_files: bool,
    debug: bool,
):
    """Roll back ETL data for specified sources or package IDs."""
    from igem_backend.ge import GE

    if not sources and not package_ids:
        raise click.UsageError("Provide at least one --source or --package-id.")

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    ge.etl.rollback(
        data_sources=list(sources) or None,
        package_ids=list(package_ids) or None,
        delete_files=delete_files,
    )
    click.echo("Rollback completed.")
