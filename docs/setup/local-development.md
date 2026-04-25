# Local Development

This guide boots the current repo-aligned Dwellio MVP implementation:
- Python backend API
- Python ETL and job runner
- ordered SQL migrations
- fixture-backed Harris and Fort Bend ingestion
- public search / parcel / quote flows
- internal admin readiness, case, and packet review pages

## 1) Prerequisites

- Python 3.11+
- PostgreSQL 15+ with PostGIS available
- Node.js 18+ if using `apps/web`
- optional: Supabase local stack if you want a fuller Postgres/PostGIS workflow

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

Useful defaults for local work:
- `DWELLIO_DATABASE_URL=postgresql://postgres:postgres@localhost:54322/postgres`
- `DWELLIO_ADMIN_API_TOKEN=dev-admin-token`
- `DWELLIO_RAW_ARCHIVE_ROOT=.dwellio/raw`

## 4) Apply migrations

```bash
make migrate
```

Optional:

```bash
make list-migrations
python3 -m infra.scripts.run_migrations --dry-run
```

## 5) Verify local health

```bash
python3 -m infra.scripts.run_migrations
curl http://localhost:8000/healthz
```

The API health route returns:
- `status`
- `database`
- `postgis_enabled`

## 6) Run API

```bash
make run-api
```

Canonical public routes:
- `GET /search?address={query}`
- `GET /search/autocomplete?query={query}`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

Canonical internal/admin routes:
- `GET /admin/readiness/{county_id}`
- `GET /admin/search/inspect`
- `GET /admin/ops/import-batches`
- `POST /admin/ops/manual-import/register`
- `GET /admin/leads`
- `GET /admin/leads/{lead_id}`
- `GET /admin/cases`
- `GET /admin/packets`

Admin access:
- send `x-dwellio-admin-token: dev-admin-token`
- or set the `dwellio_admin_token` cookie

## 7) Run the web app

```bash
cd apps/web
npm install
export NEXT_PUBLIC_DWELLIO_API_BASE_URL='http://127.0.0.1:8000'
npm run dev
```

Supported web env vars:
- `NEXT_PUBLIC_DWELLIO_API_BASE_URL`
- `DWELLIO_API_BASE_URL`

Runtime behavior:
- if neither env var is set, the web app falls back to `http://127.0.0.1:8000`
- if the backend is unreachable, the public pages render an explicit API/configuration error instead of fabricating quote data

Important public pages:
- `/`
- `/search?address=...`
- `/parcel/{county_id}/{tax_year}/{account_number}`

Important internal pages:
- `/admin/ops`
- `/admin/leads`
- `/admin/leads/{leadId}`
- `/admin/cases`
- `/admin/cases/{caseId}`
- `/admin/packets`
- `/admin/packets/{packetId}`

## 8) Load fixture-backed county data

Harris property roll:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

Fort Bend property roll:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id fort_bend --tax-year 2026 --dataset-type property_roll
```

Inspect the latest batch:

```bash
python3 -m app.jobs.cli job_inspect_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

Rollback the latest published parcel-year dataset when needed:

```bash
python3 -m app.jobs.cli job_rollback_publish --county-id harris --tax-year 2026 --dataset-type property_roll
```

## 9) Refresh derived/search/quote support

```bash
python3 -m app.jobs.cli job_features --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_comp_candidates --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_score_models --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_score_savings --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_refresh_quote_cache --county-id harris --tax-year 2026
```

Read-model assumptions:
- public search reads `v_search_read_model`
- public parcel summary reads `parcel_summary_view`
- public quote reads `v_quote_read_model`
- quote and parcel routes can explicitly fall back to the nearest prior available year

For readiness reporting and historical backfill, a fuller prior year such as `2025` may be more complete than `2026`:

```bash
python3 -m infra.scripts.report_data_readiness --county-id harris --tax-years 2025 2024 2023 2022 2026
python3 -m infra.scripts.register_manual_import --county-id harris --tax-year 2025 --dataset-type property_roll --source-file /absolute/path/to/harris_property_roll_2025.json
python3 -m app.jobs.cli job_load_staging --county-id harris --tax-year 2025 --dataset-type property_roll --import-batch-id <import_batch_id>
python3 -m app.jobs.cli job_normalize --county-id harris --tax-year 2025 --dataset-type property_roll --import-batch-id <import_batch_id>
```

## 10) Smoke-test the current implementation

Public flow:

```bash
curl 'http://localhost:8000/search?address=101%20Main'
curl 'http://localhost:8000/parcel/harris/2026/1001001001001'
curl 'http://localhost:8000/quote/harris/2026/1001001001001'
curl 'http://localhost:8000/quote/harris/2026/1001001001001/explanation'
curl -X POST 'http://localhost:8000/lead' \
  -H 'content-type: application/json' \
  -d '{"county_id":"harris","tax_year":2026,"account_number":"1001001001001","email":"alex@example.com","anonymous_session_id":"anon-smoke-1","funnel_stage":"quote_gate","utm_source":"smoke","consent_to_contact":true}'
```

Admin flow:

```bash
curl -H 'x-dwellio-admin-token: dev-admin-token' 'http://localhost:8000/admin/cases?county_id=harris&tax_year=2026'
curl -H 'x-dwellio-admin-token: dev-admin-token' 'http://localhost:8000/admin/packets?county_id=harris&tax_year=2026'
```

Lead flow note:
- `POST /lead` now persists the canonical lead row plus a `lead_events` attribution payload.
- The route accepts quote-ready, unsupported, and missing-quote contexts without changing the public route shape.
- `context_status` reports whether the request matched `quote_ready`, `missing_quote_ready_row`, `unsupported_property_type`, or `unsupported_county`.

## 11) Validation commands

```bash
make lint
make test
make typecheck
cd apps/web && npm run lint
cd apps/web && npm run build
```

Targeted regression commands:

```bash
python3 -m pytest tests/integration/test_public_parcel_flows.py tests/integration/test_stage15_workflow_contracts.py tests/integration/test_stage16_lead_funnel_release_hardening.py
python3 -m pytest tests/unit/test_stage11_migration_contract.py tests/unit/test_stage13_migration_contract.py tests/unit/test_stage14_migration_contract.py
```

## 12) Public-safe vs restricted data boundary

- Restricted MLS and listing data stays out of public APIs.
- Public search/parcel/quote payloads expose only quote-safe derived/read-model fields.
- Owner names are masked on public surfaces when the owner looks like an individual.
- Internal admin routes can inspect operational and case workflow details, but they remain token-protected.
