from __future__ import annotations

import click

from igem_backend.api.cli.common import (
    db_uri_option,
    debug_option,
    require_db_uri,
)


@click.group("db")
def db_group():
    """Database management commands."""


@db_group.command("create")
@db_uri_option
@click.option(
    "--overwrite", is_flag=True, help="Drop and recreate existing database."
)
@debug_option
@click.pass_context
def db_create(
    ctx: click.Context, db_uri: str | None, overwrite: bool, debug: bool
):
    """Create the database schema and seed initial data."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug, auto_connect=False)
    ge.db.create(overwrite=overwrite)
    click.echo("Database created successfully.")


@db_group.command("upgrade")
@db_uri_option
@debug_option
@click.pass_context
def db_upgrade(ctx: click.Context, db_uri: str | None, debug: bool):
    """Apply pending schema upgrades and re-seed missing data."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    ge.db.upgrade()
    click.echo("Database upgraded successfully.")


@db_group.command("info")
@db_uri_option
@debug_option
@click.pass_context
def db_info(ctx: click.Context, db_uri: str | None, debug: bool):
    """Show database connection info and schema version."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    info = ge.db.info()
    click.echo(f"URI       : {info.get('uri', uri)}")
    click.echo(f"Backend   : {info.get('backend', 'unknown')}")
    click.echo(f"Exists    : {info.get('exists', False)}")
    click.echo(f"Version   : {info.get('schema_version', 'n/a')}")
    click.echo(f"Revision  : {info.get('schema_revision', 'n/a')}")
