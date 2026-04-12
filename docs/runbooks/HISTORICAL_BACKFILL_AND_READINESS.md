# Historical Backfill And Readiness

Use this runbook when current-year county data is sparse and you need a prior validation year such as `2025`.

## Recommended QA years

Prefer a prior year with fuller county coverage:

- `2025`
- `2024`
- `2023`
- `2022`

`2026` can still be used, but it should not be the only meaningful validation year.

## 1. Bootstrap tax years

Apply migrations so `tax_years` includes prior years:

```bash
python3 -m infra.scripts.run_migrations
```

## 2. Inspect readiness by county and year

```bash
python3 -m infra.scripts.report_data_readiness --county-id harris --tax-years 2025 2024 2023 2022 2026
python3 -m infra.scripts.report_data_readiness --county-id fort_bend --tax-years 2025 2024 2023 2022 2026
python3 -m infra.scripts.report_historical_validation --county-id harris --tax-years 2025 2024 2023 2022 2026 --current-tax-year 2026
python3 -m infra.scripts.report_historical_validation --county-id fort_bend --tax-years 2025 2024 2023 2022 2026 --current-tax-year 2026
```

Interpretation:

- `tax_year_known = true`: the tax year exists in `tax_years`
- `availability_status = fixture_ready`: `job_run_ingestion` can fetch from a local fixture
- `availability_status = manual_upload_required`: register a real county file first, then reuse the normal staging and normalize jobs
- `staged = true`: staging rows exist through the latest import batch state
- `canonical_published = true`: the latest import batch reached canonical publish
- derived flags show whether summary/search/feature/comp/quote data is actually present for that county-year
- instant-quote readiness can temporarily report a prior `instant_quote_tax_rate_basis_year` for a current quote year when current-year tax rates are not yet usable at refresh time
- `instant_quote_tax_rate_basis_fallback_applied = true` means the current quote year is still being served with the nearest prior usable adopted tax-rate basis
- `instant_quote_tax_rate_basis_status` is the internal/admin classification for the basis actually used:
  - `prior_year_adopted_rates`
  - `current_year_unofficial_or_proposed_rates`
  - `current_year_final_adopted_rates`
- `instant_quote_tax_rate_basis_year` and `instant_quote_tax_rate_basis_status` are different:
  - basis year tells you which year's effective tax rate was used
  - basis status tells you whether that basis is prior-year adopted or same-year unofficial/final
- same-year usable rates stay `current_year_unofficial_or_proposed_rates` unless internal county-year adoption metadata explicitly marks them final adopted
- requested-year tax-rate basis usability is now conservative:
  - it still needs the `20` supportable-subject floor
  - it also needs strong effective-tax-rate coverage and tax-assignment completeness on the current-year instant-quote cohort
  - tax-assignment completeness uses the materially present tax-unit types for the county-year basis, so counties that canonically publish only `county` parcel-taxing-unit rows are not blocked just because `school` rows are absent from the canonical layer
- fallback quality is now measurable through parcel continuity metrics between the requested quote year and the selected basis year
- historical validation ranking helps choose a fuller QA year such as `2025` instead of defaulting to sparse `2026`

## 3. Backfill a historical or current year from real county files

If the county download is still in raw export shape, convert it into the adapter-ready contract first with the reusable manual prep script:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id both \
  --tax-year 2025 \
  --dataset-type both \
  --raw-root ~/county-data/2025/raw \
  --ready-root ~/county-data/2025/ready
```

For the canonical year-scoped raw-file layout, expected filenames, override usage, and manifest inspection, use:

- `docs/runbooks/MANUAL_COUNTY_FILE_PREP.md`

That command writes:

- `~/county-data/2025/ready/harris_property_roll_2025.json`
- `~/county-data/2025/ready/harris_tax_rates_2025.json`
- `~/county-data/2025/ready/fort_bend_property_roll_2025.csv`
- `~/county-data/2025/ready/fort_bend_tax_rates_2025.csv`

Verification:

- The prep command runs the existing Harris and Fort Bend adapter parsers and validators against generated ready files unless you pass `--skip-verify`.
- It also writes one manifest per dataset containing raw-file paths, checksums, output paths, row counts, and validation status.
- The Fort Bend property-roll prep preserves county export values and residential-segment enrichment, emits `exemptions_json` from `ExemptionExport.txt`, and intentionally leaves `hs_amt` and `ov65_amt` blank when authoritative numeric exemption amounts are unavailable.
- The Harris property-roll prep preserves assessed, appraised, market, and prior-year values from `real_acct.txt`, uses the first owner row from `owners.txt`, and leaves unsupported fixture-only fields like bath counts and story counts unset rather than guessing.
- Both property-roll prep paths drop raw records that do not include the adapter-required situs/site address, city, zip, or market value fields instead of emitting known-invalid ready rows.

Register the downloaded file into the standard raw/import-batch lifecycle:

```bash
python3 -m infra.scripts.register_manual_import \
  --county-id harris \
  --tax-year 2025 \
  --dataset-type property_roll \
  --source-file /absolute/path/to/harris_property_roll_2025.json
```

Then continue with the existing ingestion jobs:

```bash
python3 -m app.jobs.cli job_load_staging --county-id harris --tax-year 2025 --dataset-type property_roll --import-batch-id <import_batch_id>
python3 -m app.jobs.cli job_normalize --county-id harris --tax-year 2025 --dataset-type property_roll --import-batch-id <import_batch_id>
python3 -m app.jobs.cli job_inspect_ingestion --county-id harris --tax-year 2025 --dataset-type property_roll --import-batch-id <import_batch_id>
```

Repeat for:

- `tax_rates`
- `deeds`

Do the same for `fort_bend` with the matching county files.

### PR2 bounded backfill orchestration

When the adapter-ready files already exist under a ready-file root, use the bounded backfill runner instead of registering each dataset by hand:

```bash
python3 -m infra.scripts.run_historical_backfill \
  --counties harris fort_bend \
  --tax-years 2025 2024 2023 2022 \
  --dataset-types property_roll tax_rates \
  --ready-root ~/county-data/2025/ready
```

Notes:

- The runner reuses the existing `register_manual_import`, `job_load_staging`, and `job_normalize` path.
- Duplicate ready files are reused idempotently. If the same checksum already published successfully, the runner reports the existing batch and skips re-ingest.
- Publish still blocks when staging validation or publish-control checks fail.
- `property_roll` rollback manifests now include newly inserted accounts so first-time historical publishes can be rolled back safely.

## 4. Fixture-backed year

For `2026`, the existing fixture-backed workflow still works:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

## 5. Operator guidance

- Choose `2025` for QA when it is more complete than `2026`.
- Do not assume a current year is backfill-ready just because it exists in `tax_years`.
- Use readiness reporting to confirm raw, canonical, and derived status by county-year before using a year for valuation or comp QA.
- For instant quote, no annual code change is required when current-year tax rates are late. Refresh will automatically use the requested year once its tax-rate basis becomes usable.
- When current-year rates should be treated as final adopted for internal admin truth, insert or update the matching row in `instant_quote_tax_rate_adoption_statuses` before rerunning `job_refresh_instant_quote`. Without that explicit metadata, same-year rates remain classified as `current_year_unofficial_or_proposed_rates`.
- When fallback is active, review continuity metrics before trusting the refresh for real current-year operations:
  - `instant_quote_tax_rate_basis_continuity_parcel_match_ratio`
  - `instant_quote_tax_rate_basis_warning_codes`

## 6. Reconcile legacy Stage 17 migration state

Some environments may already have seen the old standalone adoption-status `0050` before the integrated Stage 17 chain settled on:

- `0050 = stage17_tax_rate_basis_hardening`
- `0051 = stage17_tax_rate_adoption_status_admin_truth`

Inspect the environment first:

```bash
python3 -m infra.scripts.reconcile_stage17_tax_rate_migrations
```

Important environment shapes:

- `integrated_expected`: the environment already matches the integrated chain
- `pending_normal_migrations`: run the normal migration chain
- `legacy_old_0050_adoption_collision`: the environment likely recorded the old standalone adoption-status `0050`, so the hardening SQL can be skipped unless repaired
- `artifact_drift_repairable`: `schema_migrations` says Stage 17 ran, but one or more expected Stage 17 artifacts are missing

If the script reports a safe repairable shape, apply the targeted repair explicitly:

```bash
python3 -m infra.scripts.reconcile_stage17_tax_rate_migrations --apply-repair
```

Repair behavior:

- it does not run automatically during normal migrations
- it reapplies only the idempotent Stage 17 SQL needed for hardening/adoption artifacts
- it restamps `schema_migrations` so the integrated chain is inspectable again
- it is intended for known Stage 17 drift, not arbitrary manual schema surgery

If the script reports `manual_review_required`, stop and inspect the reported `schema_migrations` rows and missing artifacts before changing anything.

## 7. Update tax-rate adoption status

Use the internal operator job when a county-year should be explicitly marked as:

- `prior_year_adopted_rates`
- `current_year_unofficial_or_proposed_rates`
- `current_year_final_adopted_rates`

Example commands:

```bash
python3 -m app.jobs.cli job_set_tax_rate_adoption_status \
  --county-id harris \
  --tax-year 2026 \
  --tax-rate-basis-status current_year_unofficial_or_proposed_rates \
  --tax-rate-basis-status-reason "Current-year rates are present but not yet final adopted." \
  --tax-rate-basis-status-note "Use prior-year adopted posture during appeal-season estimate refresh."

python3 -m app.jobs.cli job_set_tax_rate_adoption_status \
  --county-id harris \
  --tax-year 2026 \
  --tax-rate-basis-status current_year_final_adopted_rates \
  --tax-rate-basis-status-reason "Board-adopted rates confirmed internally." \
  --tax-rate-basis-status-source governing_body_adoption_record \
  --tax-rate-basis-status-note "Minutes posted and checked by ops."
```

Operational meaning:

- `prior_year_adopted_rates`: internal/admin status for a county-year that should still be treated as prior-year adopted basis truth
- `current_year_unofficial_or_proposed_rates`: same-year rates exist but should not yet be treated as final adopted
- `current_year_final_adopted_rates`: same-year rates can be treated internally as final adopted

Final-adoption guardrails:

- `current_year_final_adopted_rates` now requires all of:
  - `--tax-rate-basis-status-reason`
  - `--tax-rate-basis-status-source`
  - `--tax-rate-basis-status-note`
- accepted final-adoption evidence sources are:
  - `official_county_publication`
  - `governing_body_adoption_record`
  - `internal_verified_source_record`
- `operator_asserted` remains acceptable for non-final statuses, but it is not accepted for `current_year_final_adopted_rates`
- if a legacy row or manual DB edit marks same-year rates final without enough audit metadata, refresh surfaces internal warning codes such as:
  - `current_year_final_adoption_metadata_incomplete`
  - `current_year_final_adoption_source_unverified`

After every status update, rerun:

```bash
python3 -m app.jobs.cli job_refresh_instant_quote --county-id harris --tax-year 2026
python3 -m app.jobs.cli job_validate_instant_quote --county-id harris --tax-year 2026
```

Then inspect:

- `instant_quote_refresh_runs.tax_rate_basis_year`
- `instant_quote_refresh_runs.tax_rate_basis_status`
- `instant_quote_refresh_runs.tax_rate_basis_status_reason`
- `instant_quote_refresh_runs.tax_rate_basis_warning_codes`
- readiness/admin output fields derived from the latest refresh run
- `instant_quote_tax_rate_adoption_statuses` for the stored reason, source, and note

## 8. Stage 19 SFR denominator QA

Use this check after changing county class-code mapping or quote-subject scope:

```sql
SELECT
  pys.account_number,
  COALESCE(pc.property_class_code, p.property_class_code) AS property_class_code,
  pc.property_type_code,
  p.situs_address,
  p.situs_city,
  pa.improvement_value,
  pi.living_area_sf
FROM parcel_year_snapshots pys
JOIN parcels p ON p.parcel_id = pys.parcel_id
LEFT JOIN property_characteristics pc
  ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
LEFT JOIN parcel_assessments pa
  ON pa.parcel_id = pys.parcel_id
 AND pa.tax_year = pys.tax_year
LEFT JOIN parcel_improvements pi
  ON pi.parcel_id = pys.parcel_id
 AND pi.tax_year = pys.tax_year
WHERE pys.county_id = '<county_id>'
  AND pys.tax_year = 2026
  AND pys.is_current IS TRUE
  AND pc.property_characteristic_id IS NOT NULL
  AND pc.property_type_code IS NULL
ORDER BY md5(pys.account_number), pys.account_number
LIMIT 200;
```

Classify the deterministic sample against county class-description evidence before expanding the SFR cohort:

- `correctly_excluded_non_sfr`: class-description evidence is mobile home, auxiliary building, multifamily, condo/apartment style, vacant, ag/rural, commercial/industrial, mineral/utility/BPP/inventory, exempt, or special/nonstandard.
- `likely_true_sfr_false_negative`: class-description evidence is single-family quoteable but the row is excluded.
- `ambiguous_needs_manual_review`: source descriptions are mixed or insufficient. Do not include the class until local raw evidence or official CAD spot checks resolve it.

Readiness and admin reporting must keep both denominator-quality KPIs visible:

- `support_rate_all_sfr_flagged`, `support_count_all_sfr_flagged`, `total_count_all_sfr_flagged`
- `support_rate_strict_sfr_eligible`, `support_count_strict_sfr_eligible`, `total_count_strict_sfr_eligible`

When an interrupted manual import leaves a staged draft residue:

1. Confirm the batch is not published: `status = 'staged'`, `publish_state = 'draft'`, and `publish_version IS NULL`.
2. Confirm it has no canonical current snapshots: `SELECT COUNT(*) FROM parcel_year_snapshots WHERE import_batch_id = '<id>'`.
3. Confirm a prior published batch exists for the same county, tax year, and dataset before changing status.
4. Close the draft by marking only that batch `status = 'rolled_back'`, `publish_state = 'rolled_back'`, and a `status_reason` that states no canonical publish occurred.
5. Do not delete raw files or staging rows; keep them as audit evidence.
6. Re-run instant-quote refresh, validation, and readiness reporting for the affected county-year.

## 9. Stage 19 Weekly Quote Quality Monitoring

Run this after the weekly Harris/Fort Bend quote refresh and validation jobs:

Freshness gate: skip refresh if the latest completed validation is less than `24` hours old; otherwise run refresh and validation for Harris and Fort Bend before running the monitor.

```bash
python3 infra/scripts/report_quote_quality_monitor.py \
  --county-ids harris,fort_bend \
  --tax-year 2026
```

Scheduler path:

No CI scheduler file is currently committed in this repository, so use the cron/systemd-compatible wrapper as the scheduler entrypoint until a team CI scheduler is added.

```bash
python3 infra/scripts/run_weekly_quote_quality_monitor.py \
  --county-ids harris,fort_bend \
  --tax-year 2026 \
  --output-dir artifacts/quote_quality_monitor/latest \
  --tmp-output-dir /tmp/stage19_weekly_quote_quality_monitor_artifacts \
  --alert-webhook-env-var DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL
```

Example weekly cron entry:

```cron
0 13 * * 1 cd /Users/nblevins/Desktop/Dwellio && python3 infra/scripts/run_weekly_quote_quality_monitor.py --county-ids harris,fort_bend --tax-year 2026 --output-dir artifacts/quote_quality_monitor/latest --tmp-output-dir /tmp/stage19_weekly_quote_quality_monitor_artifacts --alert-webhook-env-var DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL
```

Durable outputs:

- `artifacts/quote_quality_monitor/latest/stage19_weekly_quote_quality_monitor.json`
- `artifacts/quote_quality_monitor/latest/stage19_weekly_quote_quality_monitor.md`
- `artifacts/quote_quality_monitor/latest/stage19_refresh_watchlist_zero_savings.csv`
- `artifacts/quote_quality_monitor/latest/stage19_refresh_watchlist_top_outliers.csv`
- `artifacts/quote_quality_monitor/latest/stage19_refresh_watchlist_summary.md`
- `artifacts/quote_quality_monitor/latest/run_state.json`
- `artifacts/quote_quality_monitor/latest/alert_payload.json`
- `artifacts/quote_quality_monitor/latest/manifest.json`

Optional transient mirror outputs:

- `/tmp/stage19_weekly_quote_quality_monitor_artifacts/stage19_weekly_quote_quality_monitor.json`
- `/tmp/stage19_weekly_quote_quality_monitor_artifacts/stage19_weekly_quote_quality_monitor.md`
- `/tmp/stage19_weekly_quote_quality_monitor_artifacts/stage19_refresh_watchlist_zero_savings.csv`
- `/tmp/stage19_weekly_quote_quality_monitor_artifacts/stage19_refresh_watchlist_top_outliers.csv`
- `/tmp/stage19_weekly_quote_quality_monitor_artifacts/stage19_refresh_watchlist_summary.md`
- `/tmp/stage19_weekly_quote_quality_monitor_artifacts/run_state.json`

Alerting:

- The wrapper writes `alert_payload.json` every run.
- Alertable conditions are `quote_quality_monitor_job_failed`, `quote_quality_denominator_shift_alert`, `quote_quality_validation_denominator_shift_alert`, `quote_quality_excluded_class_leakage`, `validation_stale`, and `validation_missing`.
- Webhook delivery is enabled when `DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL` is configured in the runtime environment.
- Use `--force-alert` for an end-to-end notifier smoke test without waiting for organic alert conditions:

```bash
DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL=<configured_url> \
python3 infra/scripts/run_weekly_quote_quality_monitor.py \
  --county-ids harris,fort_bend \
  --tax-year 2026 \
  --output-dir artifacts/quote_quality_monitor/latest \
  --tmp-output-dir /tmp/stage19_weekly_quote_quality_monitor_artifacts \
  --force-alert
```

- Always review `alert_payload.json` and `run_state.json` after each scheduled run to confirm alert decisions and delivery status.

Interpretation:

- Denominator shift: review any `threshold_exceeded` status for `total_count_all_sfr_flagged` or `total_count_strict_sfr_eligible`. The default threshold is `5%`, configurable with `DWELLIO_INSTANT_QUOTE_DENOMINATOR_SHIFT_ALERT_THRESHOLD`.
- Validation freshness: review any `validation_stale` or `validation_missing` warning before using the weekly metrics. The default monitor threshold is `24` hours and can be adjusted with `--freshness-threshold-hours`.
- Excluded-class leakage: `strong_signal_excluded` should stay near zero. Any material leakage means a non-SFR class may be suppressing true SFR quote subjects or a source-class mapping changed.
- `$0` share: compare the current monitored zero-savings share against prior weekly reports. A sudden rise is a product/valuation-review signal first, not a tax-rate change by default.
- Extreme-savings watchlist: review the top-outlier CSV and escalate any row that breaches the public-safe savings threshold, has implausible effective-tax-rate metadata, or appears to be a non-SFR class in the quoteable SFR cohort.

Manual review rubric:

- `likely_legitimate_no_reduction`: supported quote, plausible tax rate, no material reduction signal, no completeness blocker.
- `likely_data_quality_issue`: unexpected blocker, stale source metadata, implausible class/type, or denominator/leakage alert nearby.
- `likely_valuation_model_outlier`: high savings driven by valuation assumptions rather than tax-rate or source metadata.
- `escalate`: public-safe savings threshold breach, implausible tax component metadata, or non-SFR leakage into the strict SFR cohort.

Optional monthly non-prod divergence drill:

Purpose: verify the all-SFR and strict-SFR denominator paths remain independently monitored when cohorts are intentionally mixed.

Frequency: monthly, non-prod only.

Procedure:

1. Run only outside production; do not mutate canonical production parcel or quote rows.
2. Use a fixture or temporary query-layer simulation with mixed cohorts, for example all-SFR `75/100` and strict-SFR `75/80`.
3. Confirm `support_rate_all_sfr_flagged != support_rate_strict_sfr_eligible`.
4. Pass when the drill reports `diverged = true`; fail if both KPI variants use the same denominator or rate.
5. Cleanup is no-op for fixture-driven runs. If a temporary non-prod table/view is used, drop only that temporary object after the drill.
6. Record the drill date, environment, and pass/fail result in the weekly monitor notes; do not persist fixture rows into production datasets.
