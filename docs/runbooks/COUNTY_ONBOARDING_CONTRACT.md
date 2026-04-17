# County Onboarding Contract

Use this internal workflow to decide whether a county-year is ready to move from source prep into repeatable onboarding validation.

The goal is not to guess from one dashboard or one script. The goal is to use one contract that combines:

- county capability truth
- validation-year selection
- dataset prep requirements
- canonical publish state
- searchable/read-model readiness
- quote supportability readiness

## Primary CLI helper

```bash
python3 -m infra.scripts.report_county_onboarding --county-id harris --tax-years 2026 2025 2024 --current-tax-year 2026
```

## Internal admin API

```bash
GET /admin/onboarding/{county_id}?tax_years=2026&tax_years=2025&tax_years=2024&current_tax_year=2026
```

This route is internal/admin-facing by contract and returns the same onboarding phase model used by the CLI helper.

The report is machine-readable and is designed to answer:

1. Which prior year is the best repeatable validation baseline?
2. Which onboarding phases are already done?
3. Which phases are still blocking?
4. Are current gaps caused by source limits, missing prep work, or incomplete derived/read-model validation?
5. What is the next recommended operator action for each pending or blocking phase?
6. What is the overall onboarding posture: `ready`, `partial`, or `blocked`?
7. How does the current year compare to the recommended validation baseline?

## Intended operator flow

1. Review the county capability entries first.
2. Select the recommended validation year from the contract output instead of defaulting to the newest year.
3. If `dataset_prep_contract` is pending, run manual prep and manifest generation before trying onboarding validation.
4. If `canonical_publish_validation` is pending, finish bounded backfill/publish for the validation year before continuing.
5. If `searchable_validation` is pending, do not treat search/read-model gaps as quote bugs yet.
6. If `quote_supportability_validation` is pending, compare the gap against the county capability matrix before calling it a regression.
7. Prefer the `recommended_actions` list when present; it gives the safest next command or review step for each unresolved phase.
8. Use `onboarding_summary` first for triage, then drill into the phase list and recommended actions.
9. Use `baseline_comparison` to see whether the current year is still lagging the validation baseline on publish/search/quote signals.

## Important limitations

- This is an internal planning and validation contract, not a public feature flag.
- It does not replace the existing readiness dashboard; it organizes the relevant signals into onboarding phases.
- It does not execute prep, publish, or validation jobs by itself.
