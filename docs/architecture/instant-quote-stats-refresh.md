# Instant Quote Stats Refresh

Refresh job:
- `python3 -m app.jobs.cli job_refresh_instant_quote --county-id <county> --tax-year <year>`

Inputs:
- `instant_quote_subject_view`
- upstream `parcel_summary_view`
- upstream `parcel_effective_tax_rate_view`
- upstream `parcel_assessments`

Outputs:
- `instant_quote_neighborhood_stats`
- `instant_quote_segment_stats`

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
- neighborhood lookup by `county_id + tax_year + neighborhood_code + property_type_code`
- segment lookup by `county_id + tax_year + neighborhood_code + property_type_code + size_bucket + age_bucket`
