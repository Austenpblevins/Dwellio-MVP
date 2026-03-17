# Codex Task Execution Checklist

This is the practical build checklist for Dwellio.

## Environment setup
Install and verify:
- VS Code
- Git
- GitHub Desktop
- Node.js
- Python
- Supabase CLI
- Codex extension or Codex app

## Repo setup
Use this repo structure as canonical:

```text
Dwellio/
  app/
    api/
    services/
    jobs/
    models/
    county_adapters/
    db/
      migrations/
      views/
    utils/
  docs/
    codex/
    source_of_truth/
    architecture/
    runbooks/
  tests/
  infra/
  sql/
```

## Safe build order
Never skip this order:

1. schema and migrations
2. models
3. services
4. ETL jobs
5. read models
6. APIs
7. tests
8. docs sync

## Database phase
- apply ordered migrations
- do not execute the convenience full schema file as a migration
- use `sql/dwellio_full_schema.sql` only as a reference or bootstrap aid

## Model phase
Generate or reconcile Pydantic models from the current schema.

## ETL phase
Implement or reconcile:
- `job_fetch_sources.py`
- `job_load_staging.py`
- `job_normalize.py`
- `job_geocode_repair.py`
- `job_sales_ingestion.py`
- `job_features.py`
- `job_comp_candidates.py`
- `job_score_models.py`
- `job_score_savings.py`
- `job_refresh_quote_cache.py`
- `job_packet_refresh.py`

## Read-model phase
Ensure:
- `v_search_read_model`
- `v_quote_read_model`

remain the authoritative public read models.

## API phase
Canonical public endpoints:

- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

Do not substitute `/quote/refresh` as a public endpoint unless it is intentionally added as an internal/admin route.

## Frontend note
React / Next.js may be used as a frontend layer.
Backend/API authority remains Python unless the source-of-truth docs are explicitly changed.

## Commit discipline
Commit after each stable milestone:
- migrations reconciled
- models reconciled
- services reconciled
- jobs reconciled
- APIs reconciled
- tests added
- docs synced
