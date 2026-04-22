# Public Route Smoke-Test Matrix

This is the Stage 3 smoke-test matrix for the canonical public route family.

Ticket covered:

- `S3-T4` Define route smoke-test matrix

Use this matrix to answer one practical question:

> What is the minimum route-level verification we expect before saying the public route family still behaves safely?

This matrix does not replace deeper integration, frontend, or launch-readiness testing.
It defines the minimum smoke or contract-test expectation per public route in the current repo shape.

## Matrix

| Route | Minimum automated expectation | Minimum local smoke expectation | Current test evidence |
|---|---|---|---|
| `GET /healthz` | Route returns `status = ok` when dependency checks succeed and `status = degraded` instead of crashing when dependency checks fail. | `curl http://localhost:8000/healthz` | `tests/unit/test_health_api.py` |
| `GET /search` | Returns `200`, public-safe result payload, and no admin/debug search fields. | `curl 'http://localhost:8000/search?address=101%20Main'` | `tests/unit/test_search_routes.py`, `tests/integration/test_public_parcel_flows.py` |
| `GET /search/autocomplete` | Returns `200`, public-safe suggestion payload, and no admin/debug search fields. | `curl 'http://localhost:8000/search/autocomplete?query=101'` | `tests/unit/test_search_routes.py` |
| `GET /parcel/{county_id}/{tax_year}/{account_number}` | Returns `200` for a known parcel, preserves public-safe masking, excludes internal/debug parcel fields, and verifies fallback metadata when applicable. | `curl 'http://localhost:8000/parcel/harris/2026/1001001001001'` | `tests/unit/test_search_api.py`, `tests/unit/test_search_services.py`, `tests/integration/test_public_parcel_flows.py` |
| `GET /quote/{county_id}/{tax_year}/{account_number}` | Returns `200` for a known quote row, preserves fallback metadata, and returns `404` when no requested-year or fallback-year row exists. | `curl 'http://localhost:8000/quote/harris/2026/1001001001001'` | `tests/unit/test_quote_api.py`, `tests/integration/test_public_parcel_flows.py` |
| `GET /quote/{county_id}/{tax_year}/{account_number}/explanation` | Returns `200` for a known explanation row and `404` when no explanation-capable quote row exists. | `curl 'http://localhost:8000/quote/harris/2026/1001001001001/explanation'` | `tests/unit/test_quote_api.py`, `tests/integration/test_public_parcel_flows.py` |
| `GET /quote/instant/{county_id}/{tax_year}/{account_number}` | Returns `200` with public-safe instant-quote payload when supported, preserves fallback metadata, and maps intentional unsupported capability to `501` without leaking internal diagnostics. | `curl 'http://localhost:8000/quote/instant/harris/2026/1001001001001'` | `tests/unit/test_quote_api.py`, `tests/integration/test_public_parcel_flows.py` |
| `POST /lead` | Returns `200` accepted lead contract, preserves supported/unsupported context states, and keeps parcel-year attribution/fallback context in the response/event contract. | `curl -X POST 'http://localhost:8000/lead' -H 'content-type: application/json' -d '{"county_id":"harris","tax_year":2026,"account_number":"1001001001001","email":"alex@example.com"}'` | `tests/unit/test_lead_capture.py`, `tests/integration/test_stage15_workflow_contracts.py`, `tests/integration/test_stage16_lead_funnel_release_hardening.py`, `tests/integration/test_public_parcel_flows.py` |

## Minimum assertions by route

### Health

- Service should not crash the route when dependency inspection fails.
- The route may report `degraded`, but the contract should still be parseable and explicit.

### Search and autocomplete

- No admin-only scoring details in public payloads.
- Results should remain public-safe even when match confidence is low.

### Parcel

- Owner masking must stay public-safe.
- Parcel tax breakdown must stay free of assignment-debug fields.
- Fallback metadata must be explicit whenever a prior year is served.

### Refined quote and explanation

- Missing quote rows must stay `404`, not silent empty success.
- Public responses must not expose raw comps, reviewer data, or admin traces.

### Instant quote

- Unsupported capability may be `501`, but that state must remain explicit and bounded.
- Internal tax-basis diagnostics remain admin/internal only.

### Lead

- Unsupported county/property and missing quote-ready states remain graceful accepted lead states.
- These business states must not be conflated with system failure.

## Pytest bundle

The minimum automated public-route smoke bundle is:

```bash
DWELLIO_DATABASE_URL='postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev' \
python3 -m pytest \
  tests/unit/test_health_api.py \
  tests/unit/test_search_routes.py \
  tests/unit/test_search_services.py \
  tests/unit/test_quote_api.py \
  tests/unit/test_lead_capture.py \
  tests/integration/test_public_parcel_flows.py \
  tests/integration/test_stage15_workflow_contracts.py \
  tests/integration/test_stage16_lead_funnel_release_hardening.py
```

Use this as the Stage 3 public-route smoke floor, not as the full release test plan.
