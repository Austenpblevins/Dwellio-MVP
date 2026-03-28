# Testing, Observability, and Security

This document records the current implementation-oriented testing, observability, and security posture for Dwellio.

## Current automated coverage

Migration contract coverage:
- Stage 1, 3, 6, 7, 8, 9, 10, 11, 12, 13, and 14 migration contract tests
- replay-safety checks for late-stage migrations such as `0040` and `0041`

Public-flow coverage:
- public search route contract and payload safety
- public parcel summary route contract and caveat handling
- public quote and explanation route contracts
- deterministic tax-year fallback behavior for parcel and quote read paths
- public owner masking and leakage prevention

Internal-flow coverage:
- admin readiness routes
- admin search inspection route
- internal case list/detail and mutation routes
- internal packet list/detail and mutation routes
- admin auth enforcement on new internal endpoints

Fixture-backed coverage:
- Harris and Fort Bend county adapter parsing and normalization
- GIS spatial assignment fixture coverage
- Stage 15 workflow payload fixtures for lead/case/packet contract checks

## Smoke-test expectations

Critical flows to smoke locally:
- `GET /healthz`
- `GET /search?address=...`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `GET /admin/cases` with admin token
- `GET /admin/packets` with admin token
- one internal case create flow
- one internal packet create flow

If a flow depends on sparse local data, document that limitation explicitly rather than inventing a passing result.

## Observability surfaces

Current observability and inspectability paths:
- `job_runs` for job lifecycle tracking
- `import_batches`, `raw_files`, `validation_results`, and `lineage_records` for ingestion/debug review
- admin readiness dashboard for county-year status
- readiness KPI reporting for freshness, validation regressions, and recent failed jobs
- ingestion-to-searchable smoke verification for county-year traceability
- admin search inspect route for internal ranking diagnostics
- case and packet admin pages for internal workflow review

Structured logging exists across jobs and service entry points, but a centralized metrics/error platform is still an operational follow-on step.

## Security boundaries

Public-safe boundary:
- public search reads `v_search_read_model`
- public parcel reads `parcel_summary_view`
- public quote reads `v_quote_read_model`
- public APIs do not expose internal debug ranking fields, raw comps, or restricted listing artifacts

Restricted data rules:
- raw MLS/listing history/agent remarks remain restricted
- restricted source data may influence derived metrics and model outputs
- raw restricted records must not be serialized in public responses

Admin boundary:
- admin routes use the `require_admin_access` dependency
- local default token is `dev-admin-token` unless overridden by `DWELLIO_ADMIN_API_TOKEN`
- admin surfaces may expose internal readiness, case, and packet workflow details, but remain separate from public endpoints
