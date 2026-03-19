# Harris Adapter

This document describes the Stage 4 Harris County adapter implementation.

## Scope

`HarrisCountyAdapter` is the first end-to-end county adapter built on the approved Stage 1 schema and Stage 2-3 ingestion framework.

Current supported dataset:

- `property_roll`

Current tax year coverage:

- `2026`

Current acquisition mode:

- fixture-backed JSON via `config/counties/harris/datasets.yaml`

## Runtime flow

The Harris workflow stays inside the approved layer order:

1. fetch source payload
2. archive raw file
3. register `import_batches` and `raw_files`
4. parse into `stg_county_property_raw`
5. validate staging rows into `validation_results`
6. normalize into canonical parcel-year tables
7. publish version metadata
8. inspect counts and failures

## Code layout

- adapter: [adapter.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/harris/adapter.py)
- acquisition hooks: [fetch.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/harris/fetch.py)
- staging parser: [parse.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/harris/parse.py)
- normalization helpers: [normalize.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/harris/normalize.py)
- validation rules: [validation.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/harris/validation.py)
- config registry: [datasets.yaml](/Users/nblevins/Desktop/Dwellio/config/counties/harris/datasets.yaml)
- field mappings: [field_mappings.yaml](/Users/nblevins/Desktop/Dwellio/config/counties/harris/field_mappings.yaml)

## Local commands

Full Harris run:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

Inspect the latest Harris batch:

```bash
python3 -m app.jobs.cli job_inspect_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll
```

Dry-run the full lifecycle:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id harris --tax-year 2026 --dataset-type property_roll --dry-run
```

Verifier:

```bash
python3 -m infra.scripts.verify_stage4_harris --database-url postgresql://postgres:postgres@localhost:55436/postgres
```

## Inspection surface

`IngestionLifecycleService.inspect_import_batch()` now makes the Harris workflow easier to debug by returning:

- batch status and publish version
- raw file count
- job run count
- staging row count
- lineage row count
- validation result count
- validation error count
- canonical parcel-year counts
- failed validation records with persisted `details_json`

## Known limitations

- Harris is still fixture-backed in Stage 4.
- Live HCAD download automation is intentionally deferred.
- Only `property_roll` is implemented in the Harris adapter today.
