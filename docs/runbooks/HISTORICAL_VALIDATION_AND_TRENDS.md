# Historical Validation And Parcel-Year Trends

Use this runbook when `2026` is sparse and you need a repeatable QA path against fuller prior years such as `2025`.

## What already exists vs what is still scaffolded

Implemented in the repo today:

- parcel-year canonical data backbone
- `parcel_summary_view`
- `neighborhood_stats`
- valuation, savings, and decision-tree support tables
- `v_quote_read_model`
- readiness reporting and historical backfill registration
- parcel-year and neighborhood YOY trend views added in this branch

Still scaffolded or only partially implemented:

- `job_features`
- `job_comp_candidates`
- `job_score_models`
- `job_score_savings`
- `job_refresh_quote_cache`
- public quote API wiring

That means historical validation should currently focus on:

- parcel-year trend derivation
- neighborhood trend support
- valuation/savings/decision service behavior
- persisted table/read-model readiness

Do not assume this branch completes the full Stage 12 comp pipeline.

## 1. Prefer fuller prior years for QA

Start by comparing readiness across years:

```bash
python3 -m infra.scripts.report_data_readiness --county-id harris --tax-years 2025 2024 2023 2022 2026
python3 -m infra.scripts.report_historical_validation --county-id harris --tax-years 2025 2024 2023 2022 2026 --current-tax-year 2026
```

Prefer a year like `2025` when it has:

- canonical-published county datasets
- `parcel_summary_view` rows
- neighborhood stats coverage
- parcel-year trend coverage

## 2. YOY and trend fields

This branch adds:

- `parcel_year_trend_view`
- `neighborhood_year_trend_view`

These derived views use prior-year rows only. They do not forecast future values.

Example parcel-year trend outputs:

- `appraised_value_change`
- `appraised_value_change_pct`
- `assessed_value_change`
- `assessed_value_change_pct`
- `notice_value_change`
- `notice_value_change_pct`
- `effective_tax_rate_change`
- `effective_tax_rate_change_pct`
- `exemption_value_change`
- `estimated_annual_tax_change`
- `homestead_changed_flag`
- `freeze_changed_flag`

Example neighborhood trend outputs:

- `median_sale_psf_change`
- `median_sale_psf_change_pct`
- `median_sale_price_change`
- `median_sale_price_change_pct`
- `price_std_dev_change`
- `weak_sample_support_flag`

## 3. Historical validation workflow

1. Backfill or confirm a prior year such as `2025`.
2. Use readiness reporting to confirm raw, canonical, and derived coverage.
3. Use the historical validation report to rank candidate validation years.
4. Run fixture-backed tests for:
   - parcel-year feature engineering and YOY fields
   - neighborhood trend calculations
   - valuation, savings, and decision-tree service behavior
5. Treat comp validation as not ready until persisted comp pools/candidates exist for the selected year.

## 4. Limitations

- This branch does not implement the full feature-generation or comp-generation jobs.
- Quote API endpoints are still not wired to `v_quote_read_model`.
- Historical validation for comp selection remains blocked until comp candidates are actually generated and persisted.
