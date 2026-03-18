# Canonical Schema Notes

This file documents the Stage 1 schema reconciliation for Dwellio.

## Naming reconciliation

Some Stage 1 prompts use broader conceptual names than the current ordered migration chain.
Per `docs/runbooks/CANONICAL_PRECEDENCE.md`, the executable repo schema remains canonical, so we preserve the established table families where they already exist instead of creating duplicate parallel systems.

Concept mapping:

- `source_files` maps to `raw_files` plus enriched source-file metadata on `import_batches`
- `ingestion_jobs` maps to `job_runs`
- `validation_results` is now explicit in `validation_results`; legacy parser failures still live in `ingest_errors`
- `lineage_records` is now explicit in `lineage_records`
- `parcel_identifiers` maps to `parcel_aliases` plus natural identifiers on `parcels`
- `parcel_year_snapshots` is now explicit in `parcel_year_snapshots`
- `improvements` coexists with `parcel_improvements`: detail rows live in `improvements`, aggregate annual compatibility stays in `parcel_improvements`
- `land_segments` coexists with `parcel_lands`: detail rows live in `land_segments`, aggregate annual compatibility stays in `parcel_lands`
- `parcel_tax_unit_assignments` maps to `parcel_taxing_units`
- `search_documents` is a search-support table; the public search contract remains `v_search_read_model`

## Stage 1 additions

Stage 1 now includes:

- provenance and QA support via `validation_results` and `lineage_records`
- annual parcel anchoring via `parcel_year_snapshots`
- normalized annual property detail via `property_characteristics`, `improvements`, `land_segments`, and `value_components`
- tax-unit component storage via `taxing_unit_types`, `taxing_units`, `tax_rates`, and `parcel_taxing_units`
- GIS support via `parcel_geometries` and `taxing_unit_boundaries`
- ownership/deed support via `deed_records`, `deed_parties`, `parcel_owner_periods`, and `current_owner_rollups`
- search support via `search_documents`
- evidence scaffolding via `evidence_packets`
- manual correction hooks via `manual_overrides` and `manual_override_events`

## Verification

Use:

```bash
python -m infra.scripts.verify_stage1_schema
```

The verifier applies pending migrations, checks required tables/views/indexes/seeds, and runs a rollback-only foreign-key smoke test.
