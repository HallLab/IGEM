# Database

The server side of IGEM has two database modes — **SQL** (PostgreSQL
in production, SQLite for dev/CI) and **Snapshot** (DuckDB over
Parquet files, read-only). This page is the entry point; the
subchapters cover each operational area in depth.

## Two modes, same API

| Mode | URI examples | Capabilities | Schema management |
|---|---|---|---|
| **SQL** | `postgresql://user:pass@host/igem` <br> `sqlite:////path/igem.db` | Full read+write — ETL ingestion, NLP runtime, reports, all queries | [Alembic-managed](alembic.md) |
| **Snapshot** | `parquet:///path/snapshot/` <br> `/path/snapshot/` (auto-detected if `manifest.json` exists) | Read-only — query-only consumption from HPC nodes or distributed users | None — schema frozen at export time |

The SQL mode is what every write path needs. The snapshot mode is a
self-contained, immutable image of a SQL database at a point in time,
produced via `igem-server db export`. From the outside (ORM queries,
reports), the two modes behave identically; internally the SQL mode
runs against a real engine while the snapshot mode runs DuckDB views
over Parquet files.

## In this chapter

```{toctree}
:maxdepth: 1

alembic
commands
snapshots
access
models
backup
```

- **[Alembic](alembic.md)** — schema versioning, migration lifecycle, autogenerate workflow.
- **[Commands](commands.md)** — reference for every `igem-server db` subcommand.
- **[Snapshots](snapshots.md)** — exporting, the manifest, NLP cache, HPC consumption.
- **[Access](access.md)** — URI formats, environment variables, `.igem.toml`, pgvector.
- **[Models](models.md)** — ORM models, the entity model, sessions.
- **[Backup](backup.md)** — `pg_dump` strategies, restore drills, snapshot vs dump.
