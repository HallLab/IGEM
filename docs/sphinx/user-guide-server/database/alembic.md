# Alembic

Since `igem-server 0.2.0`, all schema changes are managed by
[Alembic](https://alembic.sqlalchemy.org/). This page covers the
**concepts** — what a versioned database looks like, how revisions
flow, and the day-to-day workflow for migrations. For the CLI
command reference, see [Commands](commands.md).

## Source of truth

Every running database is **versioned** — it carries a single
`alembic_version` row that records which revision its schema
corresponds to. The same revision is mirrored into
`IgemMetadata.schema_revision` so it is visible from any SQL client
without needing Alembic itself.

```
┌─────────────────┐
│ igem-server 0.x │
│  (running pkg)  │
└────────┬────────┘
         │
         │  reads
         ▼
┌─────────────────┐         ┌─────────────────────┐
│ alembic_version │ ◄────── │ Alembic migrations  │
│ (DB row)        │  apply  │ (versions/*.py)     │
└────────┬────────┘         └─────────────────────┘
         │
         │  mirrored to
         ▼
┌─────────────────────────┐
│ IgemMetadata            │
│   .schema_revision      │
└─────────────────────────┘
```

The `alembic_version` table is the **source of truth**;
`IgemMetadata.schema_revision` is a best-effort copy for human
visibility. If they ever disagree, trust `alembic_version`.

## Lifecycle states

A SQL database, at any moment, is in one of three states:

| State | `db status` reports | Meaning | Next action |
|---|---|---|---|
| **Not versioned** | `Versioned? False` | No `alembic_version` row — typically a pre-Alembic database or a SQL dump that was restored without the version row | `db stamp-head` once to baseline it |
| **Behind head** | `Versioned? True`, `current ≠ head` | Has a version row pointing at an older revision; one or more migrations are pending | `db upgrade` to apply them |
| **At head** | `Versioned? True`, `current = head`, `→ up-to-date` | Schema matches the running package | Nothing — the DB is current |

Fresh databases created via `db create` from 0.2.0+ are stamped
automatically and start in the **At head** state.

## Choosing the right path

```
                  ┌─────────────────────┐
                  │   New SQL database  │
                  │  (postgresql://…)   │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ igem-server db      │ ← creates schema + seeds
                  │   create            │   + auto-stamps to head
                  └──────────┬──────────┘
                             │
                             ▼
              ┌──────────────────────────────────┐
              │         db status                │
              │  →  up-to-date                   │
              └──────────────────────────────────┘

                   Existing DB without version row
                  ┌─────────────────────┐
                  │ pre-Alembic DB or   │
                  │   restored dump     │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ igem-server db      │ ← one-time baseline
                  │   stamp-head        │   no DDL touched
                  └──────────┬──────────┘
                             │
                             ▼
              ┌──────────────────────────────────┐
              │         db status                │
              │  →  up-to-date                   │
              └──────────────────────────────────┘

                       Pending migrations
                  ┌─────────────────────┐
                  │  db status reports  │
                  │  current ≠ head     │
                  └──────────┬──────────┘
                             │
                  ┌──────────┴──────────┐
                  │ optional: db        │
                  │   migrate-dry-run   │ ← review SQL first
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ igem-server db      │
                  │   upgrade           │
                  └─────────────────────┘
```

## Adding a migration

If you have modified an ORM model in
`igem_backend.modules.db.models` and want to ship the schema change,
generate a migration via Alembic's autogenerate against a dev DB
that is itself at head:

```bash
cd backend
IGEM_DB_URI=postgresql://dev:dev@localhost/igem \
    poetry run alembic revision --autogenerate \
        -m "describe_the_change"
```

Alembic compares the live DB to your ORM metadata and writes a new
file under `src/igem_backend/alembic/versions/<hash>_<msg>.py`.

**Autogenerate is heuristic, not authoritative.** Always inspect the
generated file before committing:

- Look for unexpected `op.drop_table` / `op.drop_column` calls.
- Confirm `server_default` values are what you intended.
- For PostgreSQL-only DDL (partitions, custom indexes, `CREATE
  EXTENSION`), use `op.execute("...")` blocks gated on the dialect:
  `if op.get_bind().dialect.name == "postgresql": op.execute(...)`.

Once happy:

```bash
# Preview SQL
igem-server db migrate-dry-run

# Apply against dev DB
igem-server db upgrade

# Confirm
igem-server db status   # → up-to-date

# Commit the new revision file alongside the model change
git add src/igem_backend/alembic/versions/<hash>_*.py \
        src/igem_backend/modules/db/models/<model>.py
```

## Filtered objects

Two classes of objects are deliberately excluded from autogenerate
via the `include_object` filter in `alembic/env.py`:

1. **`entity_aliases.embedding`** — a `pgvector` `Vector(768)` column
   that Alembic's type comparator does not recognise. Schema changes
   to this column are rare and managed via hand-written migrations.

2. **Partition children** of variant tables (anything matching
   `variant_*_chr_*`). Partition DDL is too dialect-specific for
   autogenerate; it is written explicitly in the migrations that
   introduce partitioning.

If you add new objects of either flavour, extend the filter so they
do not appear as spurious diffs.

## Transactional safety

Migrations run inside a transaction. On PostgreSQL — where DDL is
transactional — a partial failure rolls back automatically and the
schema returns to the previous revision. SQLite is more limited (no
transactional `ALTER TABLE`), but is only used in dev/test contexts
where a re-create from scratch is cheap.

For destructive changes in production (`drop column`, `drop table`),
always take a `pg_dump` snapshot before the deploy regardless — see
[Backup](backup.md).
