# Fort Bend Adapter

This document describes the Stage 5 Fort Bend County adapter implementation.

## Scope

`FortBendCountyAdapter` is the second end-to-end county adapter built on the approved Stage 1 schema and Stage 2-4 ingestion framework.

Current supported dataset:

- `property_roll`

Current tax year coverage:

- `2026`

Current acquisition mode:

- fixture-backed CSV via [datasets.yaml](/Users/nblevins/Desktop/Dwellio/config/counties/fort_bend/datasets.yaml)

## Runtime flow

The Fort Bend workflow stays inside the approved layer order:

1. fetch source payload
2. archive raw file
3. register `import_batches` and `raw_files`
4. parse CSV rows into `stg_county_property_raw`
5. validate staging rows into `validation_results`
6. normalize into canonical parcel-year tables
7. publish version metadata
8. inspect counts and failures

## Code layout

- adapter: [adapter.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/fort_bend/adapter.py)
- acquisition hooks: [fetch.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/fort_bend/fetch.py)
- staging parser: [parse.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/fort_bend/parse.py)
- normalization helpers: [normalize.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/fort_bend/normalize.py)
- validation rules: [validation.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/fort_bend/validation.py)
- dataset config: [datasets.yaml](/Users/nblevins/Desktop/Dwellio/config/counties/fort_bend/datasets.yaml)
- field mappings: [field_mappings.yaml](/Users/nblevins/Desktop/Dwellio/config/counties/fort_bend/field_mappings.yaml)
- fixture payload: [property_roll_2026.csv](/Users/nblevins/Desktop/Dwellio/app/county_adapters/fort_bend/fixtures/property_roll_2026.csv)

## Source differences handled in adapter logic

Fort Bend differs from Harris in a few source-facing ways, and those differences stay outside the canonical layer:

- acquisition uses CSV fixture payloads instead of JSON
- source field names are Fort Bend-specific and live only in county config
- `hs_amt` and `ov65_amt` are expanded by the parser into the canonical `exemptions` list before normalization
- `pool_ind` is converted from `Y/N` into boolean values during parsing

## Local commands

Full Fort Bend run:

```bash
python3 -m app.jobs.cli job_run_ingestion --county-id fort_bend --tax-year 2026 --dataset-type property_roll
```

Inspect the latest Fort Bend batch:

```bash
python3 -m app.jobs.cli job_inspect_ingestion --county-id fort_bend --tax-year 2026 --dataset-type property_roll
```

Verifier:

```bash
python3 -m infra.scripts.verify_stage5_fort_bend --database-url postgresql://postgres:postgres@localhost:55439/postgres
```

## Known limitations

- Fort Bend is still fixture-backed in Stage 5.
- Live FBCAD download automation is intentionally deferred.
- Only `property_roll` is implemented in the Fort Bend adapter today.
