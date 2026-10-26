# Commands

Reference for the `igem-server db` command group. All commands take
a `--db-uri` option or read from `IGEM_DB_URI` / `DATABASE_URL` /
`.igem.toml` — see [Access](access.md) for the resolution order.

## Quick table

| Command | Purpose | Read/Write | Modes accepted |
|---|---|---|---|
| `db create` | Create schema + seed initial data + stamp at head | write | SQL only |
| `db upgrade` | Apply pending migrations + re-seed idempotently | write | SQL only |
| `db status` | Report current vs head Alembic revision | read | SQL only |
| `db stamp-head` | Baseline an unversioned DB (insert `alembic_version` row, no DDL) | write | SQL only |
| `db migrate-dry-run` | Print SQL of pending migrations without executing | read | SQL only |
| `db info` | Show connection info, backend, schema version | read | SQL + Snapshot |
| `db export` | Export the database as a Parquet snapshot | read (source) + write (output) | SQL → Snapshot |
| `db snapshot-download` | Download a Parquet snapshot from an HTTP endpoint | write (output) | n/a |
| `db snapshot-nlp` | Build the pre-compiled NLP automaton inside a snapshot | write (snapshot dir) | n/a |

Schema-management commands refuse to run against a snapshot URI —
schema is a SQL-mode concept.

## `db create`

Creates a new SQL database: ensures the database exists at the URI
level (for PostgreSQL, runs `CREATE DATABASE` against the admin
connection if needed), bootstraps the schema via SQLAlchemy
metadata, seeds the canonical reference data, and stamps the new DB
at the current Alembic head.

```bash
igem-server --db-uri postgresql://dev:dev@localhost/igem db create
```

Options:
- `--overwrite` — skip the "database already exists" check and run
  the bootstrap flow anyway. Tables are created via
  `CREATE TABLE ... IF NOT EXISTS` (idempotent for new tables) and
  seeds re-run idempotently.

Output ends with `Stamping schema revision...` followed by `Stamp
complete.` — the new DB is at head from creation.

## `db upgrade`

Applies pending Alembic migrations and re-runs the idempotent seed
inserts. This is the command your deploy pipeline calls.

```bash
igem-server db upgrade
```

Behavior by DB state (see [Alembic — Lifecycle states](alembic.md#lifecycle-states)):
- **At head** → no DDL runs, seeds re-applied idempotently
- **Behind head** → migrations applied in order, then seeds
- **Not versioned** → refuses with a clear error pointing to `db stamp-head`

Migrations run inside a transaction (in PostgreSQL — DDL is
transactional), so a partial failure rolls back automatically.

## `db status`

Reports current vs head revision. Read-only; safe anywhere a SQL
connection is available.

```bash
igem-server db status
# Alembic status
#   DB URI         : postgresql://...
#   Repo head      : 7a8b9c0d1e2f
#   DB revision    : 7a8b9c0d1e2f
#   Versioned?     : True
#   → up-to-date.
```

Use it as a sanity check before/after a deploy, or to confirm a dev
DB is at the same revision as production before generating a new
migration.

## `db stamp-head`

Records the current head revision in `alembic_version` **without
running any DDL**. Use to baseline a database whose schema already
matches the running package but lacks a version row.

```bash
igem-server db stamp-head
```

Options:
- `--force` — clear the existing `alembic_version` row and re-stamp.
  Useful only when the existing row points at an unknown / corrupted
  revision. **Have a backup ready.**

Idempotent: if the DB is already at head, the command short-circuits
with `Already at head; nothing to do.`

## `db migrate-dry-run`

Prints the SQL that `db upgrade` would emit, without executing it.

```bash
igem-server db migrate-dry-run
# Dry-run (SQL only — no execution)
#   From: 7a8b9c0d1e2f
#   To  : head
# CREATE TABLE ...
# ALTER TABLE ... ADD COLUMN ...
```

Options:
- `--target <rev>` — render SQL targeting a specific revision (e.g.
  for rollback review). Default is `head`.

Uses Alembic's offline mode, which generates SQL from the start of
the migration graph; treat the output as a *preview of what
migrations would run*, not a literal statement-by-statement plan
against the live DB.

Common pattern in deploy review:

```bash
igem-server db migrate-dry-run > pending.sql
git diff <prev-deploy-sql> pending.sql   # if you keep them under version control
```

## `db info`

Shows connection-level information about the active backend.
Read-only; works against both SQL and snapshot URIs.

```bash
igem-server db info
# Backend     : sql
# Read-only   : False
# URI         : postgresql:///igem?host=/var/run/postgresql
# Engine      : postgresql
# Host        : <unknown>
# Database    : igem
# Connected   : True
```

For a snapshot URI, the output also reports the snapshot version,
schema version, table count, and export date pulled from the
manifest.

## `db export`

Exports the current SQL database as a versioned Parquet snapshot
directory. See [Snapshots](snapshots.md) for the full workflow.

```bash
igem-server db export \
    --output /snapshots/$(date +%Y-%m-%d)/ \
    --compression zstd
```

Key options:
- `--output <dir>` (required) — output directory; created if missing.
- `--tables a,b,c` — restrict to a subset of tables.
- `--exclude a,b` — explicitly skip tables.
- `--chunksize 50000` — rows per Parquet write batch.
- `--compression zstd|snappy|gzip|none` (default `zstd`).
- `--version <label>` — snapshot label (default: today's date).
- `--overwrite` — write into a non-empty directory.

Output is a directory containing one `.parquet` per table plus a
`manifest.json` with per-table sha256 / row count and global
metadata (`schema_version`, `schema_revision`, `snapshot_version`,
`exported_at`).

## `db snapshot-download`

Downloads a snapshot directory from an HTTP endpoint that serves a
`manifest.json`. Files are fetched in parallel and verified against
the manifest's sha256 hashes.

```bash
igem-server db snapshot-download \
    --url https://geneexposure.org/downloads/latest/ \
    --output /local/snapshot/ \
    --workers 4
```

Key options:
- `--url <base>` — base URL serving the snapshot directory and its
  `manifest.json` at the root. Default:
  `https://geneexposure.org/downloads/latest/`.
- `--output <dir>` (required) — local target directory.
- `--include-nlp` — also download the pre-compiled NLP automaton
  cache (~3.5 GB; off by default).
- `--workers N` — concurrent downloads (default 4).
- `--overwrite` — re-download files even if they exist locally with
  matching sha256.

After completion, point IGEM-Server at the output directory with
`--db-uri <output>` to consume the snapshot.

## `db snapshot-nlp`

Builds the pre-compiled NLP automaton inside an existing snapshot
directory. Reads the parquet files, builds the `AliasDictionary` +
Aho-Corasick automaton, and serialises it to `<snapshot>/nlp/`. The
automaton is then loaded instantly by the embedded IGEM-Server on
container start, skipping the ~70s rebuild.

```bash
igem-server db snapshot-nlp /path/to/snapshot/
```

Options:
- `--overwrite` — rebuild even if `<snapshot>/nlp/` already exists.

Idempotent without `--overwrite`: refuses if a cache is already
present.
