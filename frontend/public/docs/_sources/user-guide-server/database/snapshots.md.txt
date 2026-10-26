# Snapshots

A snapshot is a **self-contained, immutable, read-only image** of a
SQL database at a point in time, serialised as Parquet files plus a
JSON manifest. Snapshots are the deployment unit for HPC nodes,
embedded analyses, and any context where running a live PostgreSQL
server is not desirable.

The same `igem-server` package consumes both modes transparently:
point `--db-uri` at a snapshot directory (or at the explicit
`parquet:///` URI) and ORM queries continue to work, backed by a
DuckDB engine that exposes the Parquet files as views.

## What's in a snapshot

```
/path/to/snapshot/
├── manifest.json              # version metadata + per-table hashes
├── entity_types.parquet
├── entity_aliases.parquet
├── gene_masters.parquet
├── chemical_masters.parquet
├── ...                        # one .parquet per exported table
└── nlp/                       # optional pre-compiled NLP automaton
    ├── alias_dict.pkl
    ├── automaton.bin
    └── meta.json
```

`manifest.json` is the index. Schema:

```json
{
  "snapshot_version": "2026-05-10",
  "schema_version":   "0.1.0",
  "schema_revision":  "7a8b9c0d1e2f",
  "igem_version":     "0.2.0",
  "exported_at":      "2026-05-10T11:09:04Z",
  "duration_seconds": 42,
  "tables": {
    "gene_masters": {
      "file":   "gene_masters.parquet",
      "rows":   62894,
      "sha256": "abc123..."
    }
  }
}
```

The four version fields capture different concerns:

| Field | What it identifies | Bumps when |
|---|---|---|
| `igem_version` | The `igem-server` package that produced the export | Server is released |
| `schema_revision` | Alembic head at export time | A migration is added |
| `schema_version` | Logical (semver) schema label | Schema gains/loses meaningful columns |
| `snapshot_version` | The snapshot itself | Every export (typically dated) |

Consumers can pin against `snapshot_version` for reproducibility,
or use `schema_revision` to know which Alembic head the snapshot
was generated from.

## Exporting

```bash
igem-server --db-uri postgresql://... db export \
    --output /snapshots/2026-05-10/ \
    --compression zstd
```

See [Commands — `db export`](commands.md#db-export) for the full
option list. Notes on tuning:

- **Compression**: `zstd` is the default and gives the best ratio
  for the omics data IGEM ships; `snappy` is faster to write and
  read but produces files ~30% larger; `none` is only useful for
  debugging Parquet contents with external tools.
- **Chunksize**: 50,000 rows per write batch is a good default. Drop
  it for very wide tables that exhaust process memory; raise it for
  many narrow tables to reduce per-batch overhead.
- **Tables / exclude**: by default every ORM-registered table is
  exported. Use `--tables` to ship a domain subset (e.g. only
  genes + chemicals) or `--exclude` to drop heavy tables a consumer
  does not need (e.g. variants for a non-genomics workflow).

The export is read-only against the source DB — long-running exports
do not block writes, but a `BEGIN ... COMMIT` transaction is held on
the connection for the duration so very large exports are best run
when ingestion is idle.

## Consuming locally

Point `--db-uri` at the snapshot directory:

```bash
igem-server --db-uri /snapshots/2026-05-10/ db info
# Backend     : snapshot
# Read-only   : True
# Path        : /snapshots/2026-05-10
# Version     : 2026-05-10
# Schema      : 0.1.0
# Tables      : 42
# Exported at : 2026-05-10T11:09:04Z
```

Or use the explicit URI scheme:

```bash
igem-server --db-uri parquet:///snapshots/2026-05-10/ db info
```

Both are equivalent. The bare path is auto-detected as a snapshot
when the directory contains `manifest.json`; otherwise it falls
back to SQLite semantics.

Once connected, every reporting / NLP / query path that works
against a SQL backend works against the snapshot — the API surface
is identical. The only operations that fail are writes (ETL,
seeding, schema migration), which raise on the read-only flag.

## Consuming over HTTP

If you publish snapshots over HTTP (the public `geneexposure.org`
endpoint does this), users can pull them with `db
snapshot-download`:

```bash
igem-server db snapshot-download \
    --url https://geneexposure.org/downloads/latest/ \
    --output ./snapshot/ \
    --workers 4
```

The command fetches `manifest.json` first, then downloads each
listed file in parallel and verifies the sha256 hash. See
[Commands — `db snapshot-download`](commands.md#db-snapshot-download)
for the full option list.

## NLP cache

The NLP resolver (Aho-Corasick automaton) is built from the
`entity_aliases` table at server startup. For a snapshot with
millions of aliases this takes ~70 seconds — fine for a long-lived
server, painful for short-lived HPC jobs that re-create the process
every run.

The fix is to pre-compile the automaton **into** the snapshot:

```bash
igem-server db snapshot-nlp /path/to/snapshot/
```

This writes a `<snapshot>/nlp/` directory with the serialised
`AliasDictionary` and automaton. When IGEM-Server starts against a
snapshot that has an `nlp/` directory, it loads the cached
automaton instead of rebuilding — start-up drops from ~70s to ~2s.

The cache is opt-in: snapshots without `nlp/` work fine, just
slower to start. Pre-compiling is only worth it if the snapshot is
used in many short-lived processes (HPC job arrays, container
restarts) — for a long-running server the one-time build cost
amortises.

## HPC workflow

The intended HPC pattern is:

1. **One-off**: maintainer exports a snapshot from PROD and uploads
   to a shared filesystem or HTTPS endpoint.
2. **One-off per user**: HPC users download the snapshot once (with
   or without the NLP cache).
3. **Per job**: each compute job binds the snapshot directory into
   the IGEM container at `/snapshot` and starts the server in
   embedded mode (`embedded://`). No network access required.

See the HPC user guide for the operational details.

## Snapshot vs SQL — when to use which

| Situation | Mode |
|---|---|
| Production server, frequent writes (ETL ingestion, NLP runtime) | SQL (PostgreSQL) |
| Local dev, single-machine, schema iteration | SQL (SQLite) |
| HPC compute node with no inbound network, many short-lived jobs | Snapshot |
| Embedded analyses bundled with an `igem-server` container | Snapshot |
| Public distribution of a frozen reference dataset | Snapshot |
| Backup against catastrophic DB corruption | Both — see [Backup](backup.md) |

Snapshots are deployment-friendly but **never** a substitute for
the live SQL backend during ETL or any write workflow. They are a
projection of the DB at one moment, not a replacement for it.

## Limits

Snapshots have no `alembic_version` row — schema is frozen at
export. Every `igem-server db` command that touches schema state
refuses against a snapshot URI:

```bash
igem-server --db-uri /snap/ db upgrade
# Error: Cannot run 'db upgrade' against a snapshot (read-only).
# Configure --db-uri to point to a writable database
# (postgresql://… or sqlite:///…).
```

To "upgrade" a snapshot, regenerate it from a SQL backend that is
itself at the desired schema:

```bash
igem-server --db-uri postgresql://... db upgrade
igem-server --db-uri postgresql://... db export \
    --output /snapshots/$(date +%Y-%m-%d)/
```

The new snapshot carries the post-upgrade `schema_revision` in its
manifest, so consumers can pin against snapshots from after a
specific migration.
