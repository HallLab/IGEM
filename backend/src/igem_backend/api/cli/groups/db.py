from __future__ import annotations

import click
from igem_backend.api.cli.common import db_uri_option, debug_option, require_db_uri


def _refuse_if_read_only(ge, command_name: str) -> None:
    """Raise ClickException if the active backend is read-only (snapshot)."""
    if ge.db.is_read_only():
        raise click.ClickException(
            f"Cannot run '{command_name}' against a snapshot (read-only).\n"
            f"Configure --db-uri to point to a writable database "
            f"(postgresql://… or sqlite:///…)."
        )


def _refuse_if_snapshot_uri(uri: str, command_name: str) -> None:
    """
    Refuse writes against a snapshot URI BEFORE connecting.

    Used by commands (db create) that don't auto-connect, so the
    `Database.read_only` flag isn't set yet.
    """
    from pathlib import Path

    is_snapshot = False
    if uri.startswith("parquet://"):
        is_snapshot = True
    elif "://" not in uri:
        p = Path(uri).expanduser().resolve()
        if p.is_dir() and (p / "manifest.json").exists():
            is_snapshot = True

    if is_snapshot:
        raise click.ClickException(
            f"Cannot run '{command_name}' against a snapshot ({uri}).\n"
            f"Snapshots are read-only Parquet directories. "
            f"Use a writable URI instead "
            f"(postgresql://… or sqlite:///…)."
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
    _refuse_if_snapshot_uri(uri, "db create")
    ge = GE(db_uri=uri, debug_mode=debug, auto_connect=False)
    ge.db.create(overwrite=overwrite)


@db_group.command("upgrade")
@db_uri_option
@debug_option
@click.pass_context
def db_upgrade(ctx: click.Context, db_uri: str | None, debug: bool):
    """Apply pending schema upgrades and re-seed missing data."""
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    _refuse_if_read_only(ge, "db upgrade")
    ge.db.upgrade()


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

    click.echo(f"Backend     : {info.get('backend', 'unknown')}")
    click.echo(f"Read-only   : {info.get('read_only', False)}")
    click.echo(f"URI         : {info.get('uri', uri)}")

    if info.get("backend") == "sql":
        click.echo(f"Engine      : {info.get('engine', 'unknown')}")
        click.echo(f"Host        : {info.get('host', 'unknown')}")
        click.echo(f"Database    : {info.get('database', 'unknown')}")
    elif info.get("backend") == "snapshot":
        click.echo(f"Path        : {info.get('snapshot_dir', '?')}")
        click.echo(
            f"Version     : {info.get('snapshot_version', 'unknown')}"
        )
        click.echo(
            f"Schema      : {info.get('schema_version', 'unknown')}"
        )
        click.echo(f"Tables      : {info.get('tables_count', '?')}")
        click.echo(
            f"Exported at : {info.get('exported_at', 'unknown')}"
        )

    click.echo(f"Connected   : {info.get('connected', False)}")


@db_group.command("snapshot-download")
@click.option(
    "--url",
    default="https://geneexposure.org/downloads/latest/",
    show_default=True,
    help=(
        "Base URL of the snapshot directory. Must serve "
        "manifest.json at the root."
    ),
)
@click.option(
    "--output", "-o",
    required=True,
    type=click.Path(file_okay=False, dir_okay=True),
    help="Local directory to write the snapshot to (created if missing).",
)
@click.option(
    "--include-nlp",
    is_flag=True,
    default=False,
    help=(
        "Also download the pre-compiled NLP automaton cache "
        "(~3.5 GB). Off by default — opt-in only."
    ),
)
@click.option(
    "--workers",
    type=click.INT,
    default=4,
    show_default=True,
    help="Concurrent downloads.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Re-download files even if they already exist locally.",
)
@debug_option
def db_snapshot_download(
    url: str,
    output: str,
    include_nlp: bool,
    workers: int,
    overwrite: bool,
    debug: bool,
):
    """
    Download a Parquet snapshot from a remote HTTP endpoint.

    The snapshot's manifest.json is fetched first to discover the file
    list and integrity hashes; each table is then downloaded
    concurrently and verified against the manifest's sha256.

    Once complete, the local directory is a fully usable snapshot —
    point IGEM-Server at it via `--db-uri <output>`, or bind-mount it
    into an IGEM container at `/snapshot`.
    """
    from pathlib import Path

    from igem_backend.modules.db.snapshot_download import download_snapshot
    from igem_backend.utils.logger import Logger

    log = Logger(log_level="DEBUG" if debug else "INFO")
    manifest = download_snapshot(
        url=url,
        output_dir=Path(output),
        include_nlp=include_nlp,
        workers=workers,
        overwrite=overwrite,
        logger=log,
    )

    click.echo("")
    click.echo(f"Snapshot version : {manifest.get('snapshot_version')}")
    click.echo(f"Schema version   : {manifest.get('schema_version')}")
    click.echo(f"Output dir       : {output}")


@db_group.command("snapshot-nlp")
@click.argument(
    "snapshot_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Rebuild the NLP cache even if one already exists.",
)
@debug_option
def db_snapshot_nlp(
    snapshot_dir: str, overwrite: bool, debug: bool,
):
    """
    Build a pre-compiled NLP automaton inside an existing Parquet snapshot.

    Reads parquet files from <snapshot_dir>, builds the AliasDictionary
    + Aho-Corasick automaton, serializes it to <snapshot_dir>/nlp/. The
    automaton is then loaded instantly by the embedded:// IGEM-Server,
    skipping the ~70s rebuild from Parquet on each container start.

    Idempotent: refuses to overwrite an existing cache without --overwrite.
    """
    from pathlib import Path

    from igem_backend.modules.db.snapshot_nlp import build_nlp_cache
    from igem_backend.utils.logger import Logger

    log = Logger(log_level="DEBUG" if debug else "INFO")
    metadata = build_nlp_cache(
        snapshot_dir=Path(snapshot_dir),
        overwrite=overwrite,
        logger=log,
    )

    click.echo("")
    click.echo(f"Aliases       : {metadata['alias_count']:,}")
    click.echo(f"Unique norms  : {metadata['norm_count']:,}")
    click.echo(f"Cache size    : {metadata['size_bytes']:,} bytes")
    click.echo(f"Build time    : {metadata['build_seconds']}s")
    click.echo(f"Output        : {snapshot_dir}/nlp/")


@db_group.command("export")
@db_uri_option
@click.option(
    "--output", "-o",
    required=True,
    type=click.Path(file_okay=False, dir_okay=True),
    help="Output directory for the snapshot (created if missing).",
)
@click.option(
    "--tables",
    type=click.STRING,
    default=None,
    help=(
        "Comma-separated list of tables to export "
        "(default: all tables in the DB)."
    ),
)
@click.option(
    "--exclude",
    type=click.STRING,
    default=None,
    help="Comma-separated list of tables to skip.",
)
@click.option(
    "--chunksize",
    type=click.INT,
    default=50_000,
    show_default=True,
    help="Rows per Parquet write batch.",
)
@click.option(
    "--compression",
    type=click.Choice(["zstd", "snappy", "gzip", "none"]),
    default="zstd",
    show_default=True,
    help="Parquet compression codec.",
)
@click.option(
    "--version", "snapshot_version",
    type=click.STRING,
    default=None,
    help="Snapshot label (default: today's date in ISO format).",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Allow writing to an already-non-empty directory.",
)
@debug_option
@click.pass_context
def db_export(
    ctx: click.Context,
    db_uri: str | None,
    output: str,
    tables: str | None,
    exclude: str | None,
    chunksize: int,
    compression: str,
    snapshot_version: str | None,
    overwrite: bool,
    debug: bool,
):
    """
    Export the database as a versioned Parquet snapshot directory.

    The output directory will contain one .parquet file per table plus
    a manifest.json with versions, hashes, and row counts. The snapshot
    is read-only and consumable by IGEM-Server in read-only mode or by
    HPC scripts via DuckDB.
    """
    from igem_backend.ge import GE

    uri = require_db_uri(ctx, db_uri)
    ge = GE(db_uri=uri, debug_mode=debug)
    _refuse_if_read_only(ge, "db export")

    table_list = (
        [t.strip() for t in tables.split(",") if t.strip()] if tables else None
    )
    exclude_set = (
        {t.strip() for t in exclude.split(",") if t.strip()}
        if exclude else None
    )
    comp = None if compression == "none" else compression

    manifest = ge.db.export_snapshot(
        output_dir=output,
        tables=table_list,
        exclude=exclude_set,
        chunksize=chunksize,
        compression=comp,
        snapshot_version=snapshot_version,
        overwrite=overwrite,
    )

    click.echo("")
    click.echo(f"Snapshot version : {manifest['snapshot_version']}")
    click.echo(f"Schema version   : {manifest['schema_version']}")
    click.echo(f"Duration         : {manifest['duration_seconds']}s")
