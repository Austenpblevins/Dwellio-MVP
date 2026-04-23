# Fort Bend Adapter

This document describes the Fort Bend County adapter and the canonical Fort Bend area contract used by the reproducible raw-to-final pipeline.

## Scope

`FortBendCountyAdapter` is the second end-to-end county adapter built on the approved Stage 1 schema and Stage 2-4 ingestion framework.

Current supported dataset:

- `property_roll`

Current tax year coverage:

- `2026`

Current acquisition mode:

- adapter-ready CSV via [datasets.yaml](/Users/nblevins/Desktop/Dwellio/config/counties/fort_bend/datasets.yaml)
- canonical prep sourced from the original Fort Bend raw exports plus the authoritative property summary export

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

## Canonical area contract

Fort Bend does not expose quote-safe living area directly in `WebsiteResidentialSegs.csv`.

The permanent county contract is:

- `living_area_sf`: authoritative living area from `PropertyDataExport*.txt` `SquareFootage`
- `gross_component_area_sf`: gross component total summed from `WebsiteResidentialSegs.csv`
- `living_area_source`: source label showing which living-area path populated `living_area_sf`

Why this exists:

- the legacy Fort Bend path treated total component area as quote-facing living area
- that overstated true living area by folding in non-living segments such as attached garage and porch
- the quote system now keeps gross component area for auditability, but uses canonical living area for PSF math

## Source differences handled in adapter logic

Fort Bend differs from Harris in a few source-facing ways, and those differences stay outside the canonical layer:

- acquisition uses adapter-ready CSV payloads instead of JSON
- source field names are Fort Bend-specific and live only in county config
- `exemptions_json` (when present) plus legacy `hs_amt`/`ov65_amt` fields are expanded by the parser into the canonical `exemptions` list before normalization
- `pool_ind` is converted from `Y/N` into boolean values during parsing
- legacy `bldg_sqft` input is accepted only as a backward-compatibility alias and is normalized into `living_area_sf`

## Canonical rebuild workflow

The supported Fort Bend rebuild flow is:

1. run [prepare_manual_county_files.py](/Users/nblevins/Desktop/Dwellio/infra/scripts/prepare_manual_county_files.py) from the original Fort Bend raw exports
2. include the official `PropertyDataExport*.txt` files so `living_area_sf` is derived canonically from the start
3. register or historical-backfill the rebuilt Fort Bend ready files
4. rebuild downstream quote-support layers from the corrected source data

The legacy correction script should be treated as a backstop for historical repair, not the primary workflow.

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

- Fort Bend ingestion still expects adapter-ready CSV output from the prep workflow.
- Live FBCAD download automation is intentionally deferred.
- Only `property_roll` is implemented in the Fort Bend adapter today.
