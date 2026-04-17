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
curl -X POST 'http://localhost:8000/lead' \
  -H 'content-type: application/json' \
  -d '{"county_id":"harris","tax_year":2026,"account_number":"1001001001001","email":"alex@example.com","anonymous_session_id":"anon-ops-1","funnel_stage":"quote_gate","utm_source":"ops_smoke","consent_to_contact":true}'
```

Admin smoke:

```bash
curl -H 'x-dwellio-admin-token: dev-admin-token' 'http://localhost:8000/admin/cases?county_id=harris&tax_year=2026'
curl -H 'x-dwellio-admin-token: dev-admin-token' 'http://localhost:8000/admin/packets?county_id=harris&tax_year=2026'
python3 -m infra.scripts.report_readiness_metrics --county-id harris --tax-years 2026 2025
python3 -m infra.scripts.verify_ingestion_to_searchable --county-id harris --tax-year 2025
```

Automated smoke-oriented pytest commands:

```bash
python3 -m pytest tests/integration/test_public_parcel_flows.py tests/integration/test_stage15_workflow_contracts.py tests/integration/test_stage16_lead_funnel_release_hardening.py
python3 -m pytest tests/unit/test_case_admin_api.py tests/unit/test_stage11_migration_contract.py tests/unit/test_stage13_migration_contract.py tests/unit/test_stage14_migration_contract.py
cd apps/web && npm run lint
cd apps/web && npm run build
```

Lead flow note:
- `POST /lead` keeps the canonical route and now returns an accepted lead contract instead of a scaffold `501`.
- Quote-ready and unsupported contexts are preserved through `context_status` while keeping parcel-year identity and attribution payloads.
- Unsupported county/property and missing quote-ready rows should remain graceful lead-capture cases, not public API shape changes.

## 6. Rollback and recovery

Dataset rollback path:

```bash
python3 -m app.jobs.cli job_rollback_publish --county-id harris --tax-year 2026 --dataset-type property_roll --import-batch-id <import_batch_id>
```

Admin rollback path:
- `POST /admin/ops/import-batches/{import_batch_id}/rollback`
- `POST /admin/ops/import-batches/{import_batch_id}/retry-maintenance`

Recovery rules:
- use rollback only for publish-state ingestion corrections
- do not edit ordered migrations after they have been applied
- rebuild search and quote-safe surfaces after corrected data is republished
- if a bulk property-roll publish reaches canonical publish and then fails during post-commit maintenance, treat the batch as canonically published and inspect admin import-batch detail for failed `step_runs` before rerunning anything
- if canonical publish already succeeded, prefer the maintenance retry action before rerunning the whole property-roll pipeline
- review publish-control warnings such as `PUBLISH_WARNING_EXEMPTION_DROP` before promoting a replayed property-roll batch; warnings are intentionally nonblocking but should be treated as operator review gates
- review publish-control warnings such as `PUBLISH_WARNING_TAX_RATE_DROP` before promoting a tax-rates replay that converts previously rate-bearing units into unit-only rows; warnings are intentionally nonblocking but should be treated as operator review gates

Post-rollback recovery:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
python3 -m app.jobs.cli job_refresh_quote_cache --county-id harris --tax-year 2026
```

## 7. Practical limitations

- Harris and Fort Bend now support PR1 real-source local-file workflows, but operator smoke checks still depend on the local county-year files and database state present on the machine running them.
- Public lead creation is not fully wired beyond the canonical route contract.
- Full packet generation, PDF assembly, and filing automation remain deferred.
- Real-data quote readiness still depends on local dataset completeness for a given county-year.
