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
- historical validation ranking helps choose a fuller QA year such as `2025` instead of defaulting to sparse `2026`

## 3. Backfill a historical year from real county files

If the county download is still in raw export shape, convert it into the existing adapter-ready contract first:

```bash
python3 -m infra.scripts.convert_2025_real_sources
```

Required raw inputs for the current PR1 real-source promotion flow:

- `~/county-data/2025/raw/2025 Harris_Real_acct_owner/real_acct.txt`
- `~/county-data/2025/raw/2025 Harris_Real_acct_owner/owners.txt`
- `~/county-data/2025/raw/2025 Harris_Real_building_land/building_res.txt`
- `~/county-data/2025/raw/2025 Harris_Real_building_land/land.txt`
- `~/county-data/2025/raw/2025 Harris Roll Source_Real_jur_exempt/jur_tax_dist_exempt_value_rate.txt`
- `~/county-data/2025/raw/2025 Fort Bend_Certified Export-EXTRACTED/2025_07_17_1800_PropertyExport.txt`
- `~/county-data/2025/raw/2025 Fort Bend_Certified Export-EXTRACTED/2025_07_17_1800_OwnerExport.txt`
- `~/county-data/2025/raw/2025 Fort Bend_Certified Export-EXTRACTED/2025_07_17_1800_ExemptionExport.txt`
- `~/county-data/2025/raw/WebsiteResidentialSegs-7-22.csv`
- `~/county-data/2025/raw/2025 Fort Bend Tax Rate Source.csv`

That command writes:

- `~/county-data/2025/ready/harris_property_roll_2025.json`
- `~/county-data/2025/ready/harris_tax_rates_2025.json`
- `~/county-data/2025/ready/fort_bend_property_roll_2025.csv`
- `~/county-data/2025/ready/fort_bend_tax_rates_2025.csv`

Verification:

- The conversion command runs the existing Harris and Fort Bend adapter parsers and validators against those generated ready files unless you pass `--skip-verify`.
- The Fort Bend converter preserves the county export values and residential segment enrichment, but leaves `hs_amt` and `ov65_amt` blank because the specified raw exports include exemption presence codes, not authoritative numeric exemption amounts.
- The Harris converter preserves assessed, appraised, market, and prior-year values from `real_acct.txt`, uses the first owner row from `owners.txt`, and leaves unsupported fixture-only fields like bath counts and story counts unset rather than guessing.
- Both property-roll converters drop raw records that do not include the adapter-required situs/site address, city, zip, or market value fields instead of emitting known-invalid ready rows.

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
