# Access

🚧 *In progress.*

Will cover:

- URI formats: `postgresql://…`, `sqlite:///…`, `parquet:///…`, bare
  path auto-detection.
- Resolution order: `--db-uri` flag → `DATABASE_URL` env → `IGEM_DB_URI`
  env → `.igem.toml` `[database].uri`.
- Tuning env vars: `IGEM_DB_CONNECT_TIMEOUT`,
  `IGEM_DB_APPLICATION_NAME`, `IGEM_DB_KEEPALIVES_*`,
  `IGEM_DB_POOL_RECYCLE`.
- `pgvector` extension requirements and how `db create` provisions it.
- SQLite pragmas applied automatically (WAL, cache size, etc.).
