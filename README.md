# Dwellio

Dwellio is a Python-first Texas property tax protest platform for Harris and Fort Bend County single-family residential parcels.

This repository includes:
- source-of-truth architecture and product docs
- ordered SQL migrations and derived/read-model SQL views
- Python FastAPI backend for public and internal endpoints
- Python ETL and job runners
- Harris and Fort Bend county adapters with local fixture support
- optional Next.js frontend in `apps/web`

## Canonical architecture reminders

- Backend/API authority: Python
- ETL authority: Python
- Frontend may use React/Next.js
- Public APIs must read precomputed read models
- Restricted MLS/listing data must remain out of public APIs
- `defensible_value = min(market_model_output, unequal_appraisal_output)`
- layered flow stays `raw -> staging -> canonical -> derived/read models`

## Quick start

1. Install dependencies:
   ```bash
   python3 -m pip install -e ".[dev]"
   ```
2. Configure environment:
   ```bash
   cp .env.example .env
   ```
3. Apply migrations:
   ```bash
   make migrate
   ```
4. Run API:
   ```bash
   make run-api
   ```
5. Optional web app:
   ```bash
   cd apps/web
   npm install
   npm run dev
   ```

Detailed instructions:
- `docs/setup/local-development.md`
- `docs/runbooks/OPS_AND_RECOVERY.md`
- `docs/final_implementation_summary.md`

## Current implemented surfaces

Public routes:
- `GET /healthz`
- `GET /search?address={query}`
- `GET /search/autocomplete?query={query}`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

Internal admin routes:
- `GET /admin/search/inspect`
- `GET /admin/readiness/{county_id}`
- `GET /admin/ops/import-batches`
- `POST /admin/ops/manual-import/register`
- `POST /admin/ops/import-batches/{import_batch_id}/publish`
- `POST /admin/ops/import-batches/{import_batch_id}/rollback`
- `GET /admin/cases`
- `POST /admin/cases`
- `GET /admin/cases/{protest_case_id}`
- `POST /admin/cases/{protest_case_id}/notes`
- `POST /admin/cases/{protest_case_id}/status`
- `GET /admin/packets`
- `POST /admin/packets`
- `GET /admin/packets/{evidence_packet_id}`

## Current implementation boundaries

- Public search is backed by `v_search_read_model`.
- Public quote is backed by `v_quote_read_model`.
- Public parcel summary is backed by `parcel_summary_view`.
- Tax-year fallback for public parcel and quote flows is explicit through `requested_tax_year`, `served_tax_year`, and related metadata.
- Internal case and packet review is implemented behind admin-token-protected routes.
- Lead capture route shape exists, but the default backend workflow is still a scaffold unless a concrete lead service is wired in.
- Full filing automation, automated packet generation, and public exposure of evidence structures remain deferred.

## Useful commands

```bash
make list-migrations
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
python3 -m app.jobs.cli job_score_models --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_score_savings --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_refresh_quote_cache --county-id harris --tax-year 2026
python3 -m pytest tests/integration/test_public_parcel_flows.py tests/integration/test_stage15_workflow_contracts.py
```

## Repo structure (core)

```text
app/
  api/
  core/
  county_adapters/
  db/
    migrations/
    views/
  jobs/
  models/
  services/
  utils/
apps/
  web/
config/
  counties/
docs/
  source_of_truth/
  runbooks/
  architecture/
  setup/
infra/
  scripts/
  supabase/
tests/
  unit/
  integration/
  fixtures/
sql/
```
