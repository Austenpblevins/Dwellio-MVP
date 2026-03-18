# Dwellio

Dwellio is a Python-first property tax protest platform scaffold for Texas counties.

This repository includes:
- source-of-truth architecture and product docs
- ordered SQL migrations and read-model SQL views
- Python API skeleton (FastAPI)
- Python ETL/job skeleton
- county adapter and county config scaffolding
- optional Next.js frontend in `apps/web`

## Stage 0 foundation status

Stage 0 focuses on production-grade scaffolding only:
- runtime and env configuration
- DB connectivity and migration tooling
- API route contracts without feature logic
- ETL/job execution shell
- linting, formatting, type-checking, and tests

No quote/valuation business logic is implemented in Stage 0.

## Canonical architecture reminders

- Backend/API authority: Python
- ETL authority: Python
- Frontend may use React/Next.js
- Public APIs must read precomputed read models
- Restricted MLS/listing data must remain out of public APIs
- `defensible_value = min(market_model_output, unequal_appraisal_output)`

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

Detailed instructions:
- `docs/setup/local-development.md`

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
