# Codex System Context

This file gives Codex persistent architectural context for the Dwellio repository.

## System purpose
Dwellio is a Texas property tax protest platform that must:
1. resolve an address to a parcel
2. estimate a defensible protest value
3. estimate potential tax savings
4. determine whether protest is worthwhile
5. capture leads
6. support protest case workflows
7. learn from outcomes over time

## Architecture layers
Use a strict layered flow:

raw -> staging -> canonical -> derived -> read models -> API -> frontend

Do not collapse these layers.

## Core business rule
For each parcel-year:

`defensible_value = min(market_value_model, unequal_appraisal_model)`

## Canonical stack decision
- Database: PostgreSQL / Supabase
- Backend/API: Python
- ETL/jobs: Python
- Frontend: React / Next.js may be used as frontend only

If any imported planning document suggests a TypeScript or Next.js backend, treat that as non-canonical unless it is explicitly adopted in `docs/source_of_truth/*`.

## Data identity
The system is parcel-year centric.
Primary business identity is:

`parcel_id + tax_year`

## Public quote flow
Public quote requests must:
1. resolve parcel
2. read precomputed values from read models
3. return quote-safe outputs

Heavy comp analysis must not run live in the request path.

## Canonical read models
Public APIs rely on:
- `v_search_read_model`
- `v_quote_read_model`

## Core tables
- `parcels`
- `parcel_sales`
- `parcel_assessments`
- `neighborhood_stats`
- `comp_candidates`
- `valuation_runs`
- `parcel_savings_estimates`
- `decision_tree_results`
- `quote_explanations`
- `protest_recommendations`

## Build order
Codex should prefer this order:
1. schema and migrations
2. models
3. services
4. ETL jobs
5. read models
6. APIs
7. tests
8. docs sync
