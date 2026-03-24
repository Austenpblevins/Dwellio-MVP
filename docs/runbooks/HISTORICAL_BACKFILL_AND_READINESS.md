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
- historical validation ranking helps choose a fuller QA year such as `2025` instead of defaulting to sparse `2026`

## 3. Backfill a historical year from real county files

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

## 4. Fixture-backed year

For `2026`, the existing fixture-backed workflow still works:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

## 5. Operator guidance

- Choose `2025` for QA when it is more complete than `2026`.
- Do not assume a current year is backfill-ready just because it exists in `tax_years`.
- Use readiness reporting to confirm raw, canonical, and derived status by county-year before using a year for valuation or comp QA.
