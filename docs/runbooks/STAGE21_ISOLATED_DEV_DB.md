# Stage 21 Isolated Dev DB Workflow

Stage 21 work must use the isolated local database only.

Required environment:

```bash
export DWELLIO_DATABASE_URL='postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev'
export DWELLIO_ENV='stage21_dev'
export DWELLIO_ADMIN_API_TOKEN='dev-admin-token'
export DWELLIO_RAW_ARCHIVE_ROOT='/tmp/dwellio-stage21/raw'
```

Preflight before any Stage 21 command:

```bash
echo $DWELLIO_DATABASE_URL
pg_isready -h localhost -p 55442
```

Expected database URL:

```bash
DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev
```

Required warning:
- Never run Stage 21 work against `postgresql://postgres:postgres@localhost:54322/postgres`.
- `54322` is the protected shared/local baseline DB.
- If `echo $DWELLIO_DATABASE_URL` shows `54322`, stop immediately and switch back to the isolated Stage 21 DB URL.

Bootstrap reuse flow:

```bash
docker volume create dwellio_stage21_db_55442_data

docker run -d \
  --name dwellio_stage21_db_55442 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55442:5432 \
  -v dwellio_stage21_db_55442_data:/var/lib/postgresql/data \
  public.ecr.aws/supabase/postgres:17.6.1.095

docker exec -i dwellio_stage21_db_55442 \
  psql -h 127.0.0.1 -U supabase_admin -d postgres -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stage21_admin') THEN
    CREATE ROLE stage21_admin WITH LOGIN SUPERUSER PASSWORD 'stage21_admin';
  END IF;
END
$$;
DROP DATABASE IF EXISTS stage21_dev;
CREATE DATABASE stage21_dev TEMPLATE template0;
SQL
```

Restore and verification:

```bash
pg_restore \
  --no-owner \
  --no-privileges \
  --exit-on-error \
  --dbname='postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev' \
  /Users/nblevins/Desktop/Dwellio/artifacts/stage21_db_seed/<seed-file>.dump

python3 -m infra.scripts.run_migrations \
  --database-url 'postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev'

DWELLIO_DATABASE_URL='postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev' python3 - <<'PY'
from app.core.config import get_settings
from app.db.connection import get_connection, is_postgis_enabled
settings = get_settings()
print(settings.database_url)
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("select current_database(), current_user")
        print(cur.fetchone())
print({'postgis_enabled': is_postgis_enabled()})
PY
```

Durable backups for this isolated DB live under:

```bash
/Users/nblevins/Desktop/Dwellio/artifacts/stage21_db_seed/
```
