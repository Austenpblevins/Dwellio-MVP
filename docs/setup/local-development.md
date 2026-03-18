# Local Development (Stage 0)

This guide boots the Stage 0 foundation:
- Python backend API skeleton
- Python ETL/job runner skeleton
- ordered SQL migration tooling
- county adapter config scaffolding

## 1) Prerequisites

- Python 3.11+
- PostgreSQL 15+ with PostGIS available
- optional: Supabase local stack
- Node.js 18+ (only if using `apps/web`)

## 2) Install Python dependencies

```bash
python3 -m pip install -e ".[dev]"
```

## 3) Configure environment

```bash
cp .env.example .env
```

Set at minimum:
- `DWELLIO_DATABASE_URL`

## 4) Apply migrations

```bash
make migrate
```

Optional:

```bash
make list-migrations
python3 -m infra.scripts.run_migrations --dry-run
```

## 5) Run API

```bash
make run-api
```

Health endpoint:
- `GET /healthz`

Public contract scaffolding:
- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

## 6) Run jobs

```bash
python3 -m app.jobs.cli job_fetch_sources --county-id harris --tax-year 2026
```

## 7) Validation commands

```bash
make lint
make test
make typecheck
```

