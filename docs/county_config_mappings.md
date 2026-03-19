# County Config And Field Dictionary

This document describes the Stage 3 county configuration layer that sits on top of the approved Stage 1 schema and Stage 2 ingestion framework.

## Purpose

Stage 3 moves county-specific dataset metadata and source-to-canonical mappings out of shared Python logic and into reviewable config files.

The goals are:
- keep county field names out of shared ingestion services
- make dataset readiness and dependencies explicit
- keep Harris and Fort Bend on the same config shape
- provide a canonical field dictionary that mapping files can target

## File layout

County metadata bootstrap files remain at:
- `config/counties/harris.yaml`
- `config/counties/fort_bend.yaml`

Stage 3 adds dataset and mapping files at:
- `config/counties/harris/datasets.yaml`
- `config/counties/harris/field_mappings.yaml`
- `config/counties/fort_bend/datasets.yaml`
- `config/counties/fort_bend/field_mappings.yaml`

## Dataset config shape

Each `datasets.yaml` file defines dataset-level behavior such as:
- `source_system_code`
- `file_format`
- `staging_table`
- `access_method`
- `cadence`
- `dependencies`
- `ingestion_ready`
- `null_handling`
- `transformation_notes`

Shared ingestion code reads these values through [config_loader.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/common/config_loader.py).

## Field mapping shape

Each `field_mappings.yaml` file defines canonical section targets for a dataset.

Current supported section modes are:
- `object`: one canonical object
- `singleton_list`: one generated list item
- `template_list`: multiple generated list items with fixed defaults
- `source_list`: one canonical list item per source list item

Each mapping entry includes:
- `target_field`
- `canonical_field_code`
- `source_field` or `source_fields`
- `transform`
- `null_handling`
- optional defaults and transform options

## Canonical field dictionary

Stage 3 adds `canonical_field_dictionary` as the machine-readable catalog of canonical field targets.

Why it exists:
- reviewable catalog of canonical field codes
- stable target list for county mapping files
- future support for loading config-backed mappings into `county_field_mappings`

The current seed covers the `property_roll` dataset sections used by the Harris fixture-backed adapter.

## How adapters consume mappings

1. The county adapter loads [CountyAdapterConfig](/Users/nblevins/Desktop/Dwellio/app/county_adapters/common/config_loader.py).
2. `list_available_datasets()` reads dataset metadata from `dataset_configs`.
3. `parse_raw_to_staging()` reads the configured `staging_table`.
4. `normalize_staging_to_canonical()` calls [field_mapping.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/common/field_mapping.py) to build canonical section payloads from config.
5. Validation reads required source fields from config instead of hardcoding county field names in shared logic.

## Current status

- Harris `property_roll` is config-driven and fixture-backed.
- Fort Bend uses the same file structure but remains scaffold-only.
- Real Harris/Fort Bend county acquisition and parser details are intentionally deferred to the county-specific implementation stages.
