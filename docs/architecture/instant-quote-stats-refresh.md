# Instant Quote Stats Refresh

Refresh job:
- `python3 -m app.jobs.cli job_refresh_instant_quote --county-id <county> --tax-year <year>`

Inputs:
- county-year-scoped canonical parcel-year inputs:
  - `parcel_year_snapshots`
  - `parcel_addresses`
  - `property_characteristics`
  - `parcel_improvements`
  - `parcel_lands`
  - `parcel_assessments`
  - `parcel_exemptions`
  - `parcel_taxing_units`
  - `effective_tax_rates`
  - `current_owner_rollups`
  - `parcel_geometries`

Outputs:
- `instant_quote_subject_cache`
- `instant_quote_neighborhood_stats`
- `instant_quote_segment_stats`
- `instant_quote_refresh_runs`

Refresh order:
1. rebuild `instant_quote_subject_cache` from county-year-scoped canonical parcel-year inputs
2. rebuild `instant_quote_neighborhood_stats` from supportable cache rows
3. rebuild `instant_quote_segment_stats` from supportable cache rows
4. run `job_validate_instant_quote` to attach a validation report to the latest county-year refresh run

Population rules:
- residential single-family only
- positive living area
- positive assessment basis
- neighborhood code required
- positive effective tax rate required

Trim method:
- trim assessed-PSF observations outside the deterministic `p05` to `p95` band
- persist both raw parcel count and trimmed parcel count

Minimum support:
- neighborhood support target: `20`
- segment support target: `8`

Indexes:
- subject serving cache scope index by `county_id + tax_year + parcel_id`
- neighborhood lookup by `county_id + tax_year + neighborhood_code + property_type_code`
- segment lookup by `county_id + tax_year + neighborhood_code + property_type_code + size_bucket + age_bucket`

Operational checks:
- compare the refresh source row count to `subject_cache_row_count` in `instant_quote_refresh_runs`
- treat non-zero `cache_view_row_delta` as a serving-layer mismatch warning
- use `validated_at` plus `validation_report.supported_public_quote_exists` as the latest county-year validation gate
