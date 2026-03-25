# Operations, Smoke Tests, and Recovery

This runbook describes the practical local operator workflow for the currently implemented Dwellio branch.

## 1. Environment baseline

Set these first:

```bash
export DWELLIO_DATABASE_URL='postgresql://postgres:postgres@localhost:54322/postgres'
export DWELLIO_ADMIN_API_TOKEN='dev-admin-token'
```

Recommended local checks:

```bash
python3 -m infra.scripts.run_migrations
curl http://localhost:8000/healthz
```

## 2. Core operator workflow

Migration sync:

```bash
python3 -m infra.scripts.run_migrations --list
python3 -m infra.scripts.run_migrations
```

Fixture-backed ingestion:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
python3 -m app.jobs.cli job_run_ingestion --county-id fort_bend --tax-year 2026 --dataset-type property_roll
```

Inspect the latest import batch:

```bash
python3 -m app.jobs.cli job_inspect_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

Refresh quote-safe derived support:

```bash
python3 -m app.jobs.cli job_features --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_comp_candidates --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_score_models --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_score_savings --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_refresh_quote_cache --county-id harris --tax-year 2026
```

## 3. Read-model assumptions

Public search:
- reads `v_search_read_model`

Public parcel summary:
- reads `parcel_summary_view`

Public quote:
- reads `v_quote_read_model`

Refresh expectations:
- the public request path does not perform heavy comp generation
- quote-safe rows depend on prior ingestion and scoring jobs
- parcel and quote routes may serve the nearest prior available year, but must disclose that through fallback metadata

## 4. Public/admin boundary

Public endpoints:
- `GET /search?address={query}`
- `GET /search/autocomplete?query={query}`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

Internal admin endpoints:
- readiness and ingestion ops routes under `/admin/ops`
- internal case routes under `/admin/cases`
- internal packet routes under `/admin/packets`

Security rules:
- admin endpoints require `x-dwellio-admin-token`
- restricted MLS/listing artifacts must not appear in public payloads
- packet and case workflow data is internal-only

## 5. Smoke-test instructions

Public smoke:

```bash
curl 'http://localhost:8000/search?address=101%20Main'
curl 'http://localhost:8000/parcel/harris/2026/1001001001001'
curl 'http://localhost:8000/quote/harris/2026/1001001001001'
curl 'http://localhost:8000/quote/harris/2026/1001001001001/explanation'
```

Admin smoke:

```bash
curl -H 'x-dwellio-admin-token: dev-admin-token' 'http://localhost:8000/admin/cases?county_id=harris&tax_year=2026'
curl -H 'x-dwellio-admin-token: dev-admin-token' 'http://localhost:8000/admin/packets?county_id=harris&tax_year=2026'
```

Automated smoke-oriented pytest commands:

```bash
python3 -m pytest tests/integration/test_public_parcel_flows.py tests/integration/test_stage15_workflow_contracts.py
python3 -m pytest tests/unit/test_case_admin_api.py tests/unit/test_stage11_migration_contract.py tests/unit/test_stage13_migration_contract.py tests/unit/test_stage14_migration_contract.py
cd apps/web && npm run lint
cd apps/web && npm run build
```

Lead flow note:
- `POST /lead` keeps the canonical route, but the default implementation is still a scaffold and currently returns `501` unless a concrete lead workflow is wired in.

## 6. Rollback and recovery

Dataset rollback path:

```bash
python3 -m app.jobs.cli job_rollback_publish --county-id harris --tax-year 2026 --dataset-type property_roll
```

Admin rollback path:
- `POST /admin/ops/import-batches/{import_batch_id}/rollback`

Recovery rules:
- use rollback only for publish-state ingestion corrections
- do not edit ordered migrations after they have been applied
- rebuild search and quote-safe surfaces after corrected data is republished

Post-rollback recovery:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
python3 -m app.jobs.cli job_refresh_quote_cache --county-id harris --tax-year 2026
```

## 7. Practical limitations

- Harris and Fort Bend ingestion remains fixture-backed in the current repo.
- Public lead creation is not fully wired beyond the canonical route contract.
- Full packet generation, PDF assembly, and filing automation remain deferred.
- Real-data quote readiness still depends on local dataset completeness for a given county-year.
