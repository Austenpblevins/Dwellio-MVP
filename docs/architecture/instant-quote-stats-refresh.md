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
1. select a county-year tax-rate basis during refresh:
   - prefer the requested quote year only when it clears the full usability rule for the current-year quoteable cohort
   - otherwise fall back automatically to the nearest prior year that clears the same usability checks
   - persist the selected basis year, fallback flag, reason, basis-status classification, basis-status reason, coverage metrics, and continuity diagnostics in `instant_quote_subject_cache` and `instant_quote_refresh_runs`
2. rebuild `instant_quote_subject_cache` from county-year-scoped canonical parcel-year inputs using the selected tax-rate basis year
3. rebuild `instant_quote_neighborhood_stats` from supportable cache rows
4. rebuild `instant_quote_segment_stats` from supportable cache rows
5. run `job_validate_instant_quote` to attach a validation report to the latest county-year refresh run

Population rules:
- residential single-family only
- positive living area
- positive assessment basis
- neighborhood code required
- positive effective tax rate required

Tax-rate basis selection:
- quote-year identity stays on the requested parcel tax year
- only the effective tax rate may temporarily come from the nearest prior usable year
- no annual code change is required when the platform rolls from `2026` to `2027` and beyond
- once current-year effective tax-rate coverage becomes usable at refresh time, the refresh automatically switches back to the requested year
- basis-year usability is separate from adoption status:
  - usability answers whether refresh can safely use a basis operationally
  - adoption status answers whether the selected basis is prior-year adopted, same-year unofficial/proposed, or same-year final adopted
- if the selected basis year is prior to the quote year, refresh marks `prior_year_adopted_rates`
- if the selected basis year matches the quote year and no explicit internal final-adoption record exists, refresh marks `current_year_unofficial_or_proposed_rates`
- refresh marks `current_year_final_adopted_rates` only when `instant_quote_tax_rate_adoption_statuses` contains an explicit same-year final-adopted assertion for that county-year

Usability rule:
- keep the canonical supportable-subject floor at `20`
- require requested-year effective-tax-rate coverage of at least `85%` across the current-year quoteable cohort
- require requested-year county+school assignment completeness of at least `90%` across that same cohort
- require fallback basis parcel continuity by `parcel_id` of at least `85%`
- emit a continuity warning when fallback continuity is below `90%`, even if the fallback basis is still usable

Quoteable cohort for tax-rate basis evaluation:
- current-year `sfr` rows only
- positive living area
- positive assessment basis
- non-empty neighborhood code
- this keeps the selector aligned to the same instant-quote population that would otherwise be blocked by missing effective tax rates

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
- inspect `tax_rate_basis_year`, `tax_rate_basis_fallback_applied`, `tax_rate_basis_reason`, `tax_rate_basis_status`, `tax_rate_basis_status_reason`, supportable counts, coverage ratios, and continuity metrics in `instant_quote_refresh_runs` to see whether a county-year refresh used prior-year adopted rates or same-year unofficial/final rates and whether the fallback quality is healthy
- use `validated_at` plus `validation_report.supported_public_quote_exists` as the latest county-year validation gate
