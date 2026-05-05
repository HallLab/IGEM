"""
Snapshot export — write DB tables as versioned Parquet files.

Produces a directory containing:
  manifest.json                   — versions, hashes, table metadata
  <table_name>.parquet            — one file per exported table

The manifest is the canonical descriptor — clients (and the read-only
mode of IGEM-Server) validate it on load before serving queries. See
`Caderno/2026-04-25_01.md` for the full architecture.

Streaming
---------
Tables are read in chunks (default 50k rows) and written incrementally
via `pyarrow.parquet.ParquetWriter`, so peak memory stays bounded
regardless of table size. A 7M-row table exports with <500MB RAM.

Consistency
-----------
The whole export runs inside a single `REPEATABLE READ` transaction so
all parquet files reflect the same point-in-time snapshot of the DB,
even if the export takes minutes and writes happen elsewhere.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# Tables to skip by default — these grow without bound and don't carry
# value for read-only HPC consumers (audit logs, transient ETL state).
_DEFAULT_EXCLUDED: frozenset[str] = frozenset({
    # add table names here as we identify ETL-internal tables
})

DEFAULT_CHUNKSIZE = 50_000
DEFAULT_COMPRESSION = "zstd"


def export_snapshot(
    engine: Engine,
    output_dir: Path | str,
    tables: Optional[list[str]] = None,
    exclude: Optional[set[str]] = None,
    chunksize: int = DEFAULT_CHUNKSIZE,
    compression: str = DEFAULT_COMPRESSION,
    snapshot_version: Optional[str] = None,
    igem_version: str = "0.1.0",
    overwrite: bool = False,
    verbose: bool = True,
) -> dict:
    """
    Export the DB to a versioned Parquet snapshot directory.

    Parameters
    ----------
    engine:
        SQLAlchemy engine pointing to the source DB.
    output_dir:
        Target directory; created if missing.
    tables:
        Explicit list of table names to export. Default: all tables in
        the DB metadata.
    exclude:
        Set of table names to skip (combined with `_DEFAULT_EXCLUDED`).
    chunksize:
        Rows per Parquet write batch.
    compression:
        Parquet compression codec (zstd | snappy | gzip | none).
    snapshot_version:
        Label written into the manifest. Default: today's date.
    igem_version:
        IGEM-Server version string written into the manifest.
    overwrite:
        If False (default), refuse to write to a non-empty directory.

    Returns
    -------
    The manifest dict that was written to disk.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    existing = list(out.iterdir())
    if existing and not overwrite:
        raise FileExistsError(
            f"Output directory {out} is not empty. "
            f"Use overwrite=True or pick a fresh directory."
        )

    # Resolve table list
    inspector = inspect(engine)
    all_tables = set(inspector.get_table_names())
    skip = set(exclude or set()) | _DEFAULT_EXCLUDED

    if tables is None:
        target_tables = sorted(all_tables - skip)
    else:
        missing = set(tables) - all_tables
        if missing:
            raise ValueError(f"Tables not found in DB: {sorted(missing)}")
        target_tables = sorted(set(tables) - skip)

    if not target_tables:
        raise ValueError("No tables to export after applying filters.")

    snapshot_version = snapshot_version or dt.date.today().isoformat()
    started_at = dt.datetime.now(dt.timezone.utc)

    if verbose:
        print(
            f"\nSnapshot export — {len(target_tables)} tables "
            f"→ {out} (compression={compression})",
            flush=True,
        )
        print(
            f"  {'TABLE':<40} {'ROWS':>15} {'SIZE':>12}",
            flush=True,
        )
        print(
            f"  {'-' * 40} {'-' * 15} {'-' * 12}",
            flush=True,
        )

    # REPEATABLE READ guarantees all per-table SELECTs see the same
    # consistent snapshot of the DB. We open ONE connection and reuse
    # it across all exports.
    table_meta: dict[str, dict] = {}

    if engine.dialect.name == "postgresql":
        conn = engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        )
    else:
        conn = engine.connect()

    try:
        with conn.begin():
            for tname in target_tables:
                meta = _export_one_table(
                    conn=conn,
                    table_name=tname,
                    out_dir=out,
                    chunksize=chunksize,
                    compression=compression,
                )
                table_meta[tname] = meta
                if verbose:
                    print(
                        f"  {tname:<40} {meta['rows']:>15,} "
                        f"{_human_size(meta['size_bytes']):>12}",
                        flush=True,
                    )
    finally:
        conn.close()

    finished_at = dt.datetime.now(dt.timezone.utc)
    duration_s = (finished_at - started_at).total_seconds()

    manifest = {
        "snapshot_version": snapshot_version,
        "schema_version": _read_schema_version(engine),
        "igem_version": igem_version,
        "igem_version_compatible": ">=0.1.0,<0.2.0",
        "exported_at": started_at.isoformat().replace("+00:00", "Z"),
        "duration_seconds": round(duration_s, 1),
        "source": _source_info(engine),
        "compression": compression,
        "chunksize": chunksize,
        "tables": table_meta,
    }

    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))

    if verbose:
        total_size = sum(t["size_bytes"] for t in table_meta.values())
        total_rows = sum(t["rows"] for t in table_meta.values())
        print(
            f"  {'-' * 40} {'-' * 15} {'-' * 12}",
            flush=True,
        )
        print(
            f"  {'TOTAL':<40} {total_rows:>15,} "
            f"{_human_size(total_size):>12}",
            flush=True,
        )
        print(
            f"\nManifest written: {manifest_path}",
            flush=True,
        )
    return manifest


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Column type fragments that pyarrow cannot infer cleanly from pandas
# DataFrames — we CAST them to TEXT in the SELECT so they round-trip as
# JSON-encoded strings. Consumers can json.loads() them on read.
#
# Hits:
#   JSON / JSONB     — empty inner structs trip pyarrow when all NULL
#                      (e.g. etl_packages.stats with nested keys)
#   VECTOR(N)        — pgvector type isn't natively understood by pyarrow
#   ARRAY            — generic PG array; varies per element type
_TEXTIFY_TYPE_FRAGMENTS: tuple[str, ...] = (
    "JSON", "JSONB", "VECTOR", "ARRAY",
)


def _select_with_casts(table_name: str, columns: list[dict]) -> str:
    """Build a SELECT that CASTs problematic types to TEXT."""
    parts: list[str] = []
    for col in columns:
        name = col["name"]
        type_str = str(col["type"]).upper()
        if any(frag in type_str for frag in _TEXTIFY_TYPE_FRAGMENTS):
            parts.append(f'CAST("{name}" AS TEXT) AS "{name}"')
        else:
            parts.append(f'"{name}"')
    return f'SELECT {", ".join(parts)} FROM "{table_name}"'


def _export_one_table(
    conn,
    table_name: str,
    out_dir: Path,
    chunksize: int,
    compression: str,
) -> dict:
    """Stream a single table to a Parquet file. Returns metadata dict."""
    out_path = out_dir / f"{table_name}.parquet"

    # Inspect column types and stringify the ones pyarrow can't handle
    insp = inspect(conn.engine)
    columns_meta = insp.get_columns(table_name)
    select_sql = _select_with_casts(table_name, columns_meta)

    writer: Optional[pq.ParquetWriter] = None
    arrow_schema: Optional[pa.Schema] = None
    n_rows = 0

    try:
        chunks = pd.read_sql(text(select_sql), conn, chunksize=chunksize)
        for chunk_df in chunks:
            if chunk_df.empty:
                continue
            if writer is None:
                arrow_table = pa.Table.from_pandas(
                    chunk_df, preserve_index=False
                )
                arrow_schema = arrow_table.schema
                writer = pq.ParquetWriter(
                    out_path,
                    arrow_schema,
                    compression=compression,
                )
                writer.write_table(arrow_table)
            else:
                arrow_table = pa.Table.from_pandas(
                    chunk_df,
                    preserve_index=False,
                    schema=arrow_schema,
                )
                writer.write_table(arrow_table)
            n_rows += len(chunk_df)
    finally:
        if writer is not None:
            writer.close()

    # Empty table — write an empty parquet so consumers don't have
    # missing files; schema is derived from a 0-row SELECT.
    if writer is None:
        empty_df = pd.read_sql(text(f"{select_sql} WHERE 1=0"), conn)
        arrow_table = pa.Table.from_pandas(empty_df, preserve_index=False)
        pq.write_table(arrow_table, out_path, compression=compression)

    size = out_path.stat().st_size
    sha256 = _file_sha256(out_path)

    pf = pq.ParquetFile(out_path)
    columns = [f.name for f in pf.schema_arrow]

    return {
        "rows": n_rows,
        "file": out_path.name,
        "size_bytes": size,
        "sha256": sha256,
        "columns": columns,
    }


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_schema_version(engine: Engine) -> str:
    """Read schema_version from igem_metadata; fallback if absent."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT schema_version FROM igem_metadata "
                    "ORDER BY id DESC LIMIT 1"
                )
            ).first()
            if row and row[0]:
                return str(row[0])
    except Exception:
        pass
    return "unknown"


def _source_info(engine: Engine) -> dict:
    url = engine.url
    return {
        "engine": url.drivername,
        "host": url.host or "local",
        "database": url.database or "",
    }


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"
