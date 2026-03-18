# SQL Migrations

Dwellio migrations are ordered SQL files in `app/db/migrations/`.

Rules:
- keep files atomic and ordered
- do not reorder existing migration versions
- add new migrations with the next numeric prefix
- keep `sql/dwellio_full_schema.sql` as a reference artifact, not execution history

Stage 0 baseline:
- `0001_extensions.sql` enables `postgis`, `pgcrypto`, `citext`, and `pg_trgm`
- migration execution is provided by `infra/scripts/run_migrations.py`

