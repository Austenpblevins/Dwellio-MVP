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
- The Fort Bend property-roll prep preserves county export values and residential-segment enrichment, but leaves `hs_amt` and `ov65_amt` blank because the specified raw exports confirm exemption presence, not authoritative numeric exemption amounts.
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
