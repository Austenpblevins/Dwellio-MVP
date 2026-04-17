# Admin County-Year Readiness

This internal-only readiness surface helps operators answer a simple question:

- Is a given county-year actually ready for search, parcel summary, and later quote-oriented work?

It is intentionally separate from public parcel and quote surfaces.

## What it shows

For each county-year, the admin readiness API and page distinguish:

- tax year seeded in `tax_years`
- dataset availability expectations from county adapter config
- county capability expectations from county adapter capability matrix
- source data acquired as raw files
- canonical publish progress from import batches
- operational freshness and validation regression signals
- recent failed job counts and alertable blockers
- derived/read-model readiness for:
  - `parcel_summary_view`
  - `search_documents`
  - `parcel_features`
  - `comp_candidate_pools`
  - `v_quote_read_model`

## Internal route

- `GET /admin/readiness/{county_id}?tax_years=2026&tax_years=2025`

This route is internal/admin-facing by contract. It exposes blockers and readiness details that should not be included in public parcel or quote responses.

## Admin web page

- `/admin/readiness?county=harris&years=2026,2025,2024`

The page uses the internal readiness API and is intended for operator review.
It can also surface the county capability matrix so operator expectations stay aligned with what each county source truly supports today.

If the API is not reachable, set:

- `NEXT_PUBLIC_DWELLIO_API_BASE_URL`
- or `DWELLIO_API_BASE_URL`

## How to interpret status

- `tax_year_missing`: the year is not seeded yet
- `awaiting_source_data`: no raw files registered yet
- `source_acquired`: raw files exist, but canonical publish is incomplete
- `canonical_partial`: at least part of the canonical layer is published
- `derived_ready`: parcel summary and search support are available
- `quote_ready`: quote read model rows exist

Dataset-level blocker examples:

- `manual_backfill_required`
- `source_not_acquired`
- `staging_validation_failed`
- `canonical_publish_pending`
- `stale_source_activity`
- `recent_job_failures`
- `validation_regression`

Operational KPI examples:

- `quality_score` and `quality_status`
- `freshness_status`, `freshness_age_days`, and `freshness_sla_days`
- `recent_failed_job_count`
- `validation_regression_count`
- `alerts`

Derived/downstream blocker examples:

- `parcel_summary_not_ready`
- `search_read_model_not_ready`
- `feature_layer_not_ready`
- `comp_layer_not_ready`
- `quote_read_model_not_ready`

## Recommended operator workflow

1. Prefer a fuller prior year such as `2025` when `2026` is still sparse.
2. Review dataset-level blockers first.
3. Use the county capability matrix in the county adapter config to sanity-check whether a missing signal is truly expected, limited, or unsupported before treating it as a code bug.
4. Confirm `parcel_summary_view` and `search_documents` are ready before deeper QA.
5. Treat feature/comp/quote readiness as downstream signals, not guarantees that public quote behavior is complete.
6. Use the operational KPI section to decide whether the year is alertable even when canonical publish technically succeeded.

## Operator CLI helpers

Machine-readable readiness KPI report:

```bash
python3 -m infra.scripts.report_readiness_metrics --county-id harris --tax-years 2025 2024 2023
```

Ingestion-to-searchable smoke verification:

```bash
python3 -m infra.scripts.verify_ingestion_to_searchable --county-id harris --tax-year 2025
```

## Intentional limitations

- This is readiness transparency, not an access-control system.
- Public trend display is intentionally deferred here.
- The page does not expose restricted comps, internal evidence, or MLS-style detail.
