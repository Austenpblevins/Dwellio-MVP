# Source Registry

This document records how Dwellio represents county source definitions without hardcoding source URLs or acquisition details into shared ingestion logic.

## Authority

The runtime source registry is configuration-backed.
For the current implementation, registry entries come from:

- `config/counties/<county_id>.yaml`
- `config/counties/<county_id>/datasets.yaml`
- `source_systems` seed rows in ordered SQL migrations

The shared loader in [config_loader.py](/Users/nblevins/Desktop/Dwellio/app/county_adapters/common/config_loader.py) reads the county config shape, and [source_registry.py](/Users/nblevins/Desktop/Dwellio/app/ingestion/source_registry.py) exposes normalized registry entries to adapters and jobs.

## Registry fields

Each dataset registry entry carries the Stage 4 fields required by the platform spec:

- `source_system_code`
- `source_name`
- `source_type`
- `county_coverage`
- `entity_coverage`
- `official_url`
- `fallback_source`
- `access_method`
- `file_format`
- `cadence`
- `reliability_tier`
- `auth_required`
- `manual_fallback_supported`
- `legal_notes`
- `parser_module_name`
- `adapter_name`
- `active_flag`
- `supported_years`

## Harris Stage 4 entry

Harris currently exposes one active registry entry:

- county: `harris`
- dataset: `property_roll`
- source system: `HCAD_BULK`
- access method: `fixture_json`
- file format: `json`
- cadence: `annual`
- reliability tier: `development_fixture`

This is intentionally fixture-backed for the first end-to-end county implementation. It proves the acquisition and normalization contract without claiming live HCAD automation yet.

## Fort Bend Stage 5 entry

Fort Bend now also exposes one active registry entry:

- county: `fort_bend`
- dataset: `property_roll`
- source system: `FBCAD_EXPORT`
- access method: `fixture_csv`
- file format: `csv`
- cadence: `annual`
- reliability tier: `development_fixture`

This proves the shared source registry and ingestion framework can support multiple counties without adding county-specific hacks to shared services or canonical tables.
