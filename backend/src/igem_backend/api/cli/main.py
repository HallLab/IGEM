from __future__ import annotations

import click

from igem_backend import __version__
from igem_backend.api.cli.common import db_uri_option, debug_option
from igem_backend.api.cli.groups.db import db_group
from igem_backend.api.cli.groups.etl import etl_group


@click.group()
@db_uri_option
@debug_option
@click.version_option(__version__, prog_name="igem-server")
@click.pass_context
def main(ctx: click.Context, db_uri: str | None, debug: bool):
    """IGEM Server — database and ETL management."""
    ctx.ensure_object(dict)
    ctx.obj["db_uri"] = db_uri
    ctx.obj["debug"] = debug


main.add_command(db_group)
main.add_command(etl_group)
