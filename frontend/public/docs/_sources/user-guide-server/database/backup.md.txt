# Backup

🚧 *In progress.*

Will cover:

- `pg_dump` strategies: full vs schema-only vs data-only, custom vs
  plain SQL format, compression, parallel dump with `-j`.
- Restore drill: `pg_restore` against an empty target DB; verifying
  the restore via `db status` + `db info`.
- Cron / systemd timer setup for routine dumps in production.
- Off-site replication of dumps and snapshots.
- When a [snapshot](snapshots.md) is sufficient as a backup and
  when only a `pg_dump` will do (snapshots are read-only and
  schema-frozen — fine for content recovery, not for resuming
  writes).
- Tested restore policy: a backup you have never restored is not a
  backup.
