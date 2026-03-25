# Canonical Schema Notes

This file documents the current schema reconciliation state across the implemented migration chain.

## Naming reconciliation

Some early prompts use broader conceptual names than the current ordered migration chain.
Per `docs/runbooks/CANONICAL_PRECEDENCE.md`, the executable repo schema remains canonical, so Dwellio preserves the established table families where they already exist instead of creating duplicate parallel systems.

Concept mapping:

- `source_files` maps to `raw_files` plus enriched source-file metadata on `import_batches`
- `ingestion_jobs` maps to `job_runs`
- `validation_results` is explicit in `validation_results`; parser failures still also surface through `ingest_errors`
- `lineage_records` is explicit in `lineage_records`
- `parcel_identifiers` maps to `parcel_aliases` plus natural identifiers on `parcels`
- `parcel_year_snapshots` is explicit in `parcel_year_snapshots`
- `improvements` coexists with `parcel_improvements`: detail rows live in `improvements`, aggregate annual compatibility stays in `parcel_improvements`
- `land_segments` coexists with `parcel_lands`: detail rows live in `land_segments`, aggregate annual compatibility stays in `parcel_lands`
- `parcel_tax_unit_assignments` maps to `parcel_taxing_units`
- `search_documents` is a search-support table; the canonical public search contract remains `v_search_read_model`
- prompt references to `evidence_packet_sections` and `evidence_packet_comps` map to the canonical Stage 14 tables `evidence_packet_items`, `evidence_comp_sets`, and `evidence_comp_set_items`

## Implemented canonical additions

- provenance and QA support via `validation_results` and `lineage_records`
- annual parcel anchoring via `parcel_year_snapshots`
- normalized annual property detail via `property_characteristics`, `improvements`, `land_segments`, and `value_components`
- tax-unit component storage via `taxing_unit_types`, `taxing_units`, `tax_rates`, and `parcel_taxing_units`
- GIS support via `parcel_geometries` and `taxing_unit_boundaries`
- ownership/deed support via `deed_records`, `deed_parties`, `parcel_owner_periods`, and `current_owner_rollups`
- search support via `search_documents`
- parcel summary support via `parcel_summary_view`
- public search and quote read models via `v_search_read_model` and `v_quote_read_model`
- lead and client workflow support via `leads`, `lead_events`, `clients`, and `representation_agreements`
- protest operations via `protest_cases`, `case_notes`, `case_outcomes`, `client_parcels`, `case_assignments`, `hearings`, and `case_status_history`
- evidence support via `evidence_packets`, `evidence_packet_items`, `evidence_comp_sets`, and `evidence_comp_set_items`
- manual correction hooks via `manual_overrides` and `manual_override_events`

## Public/internal boundary

- Public routes must stay on derived/read-model surfaces:
  - `GET /search?address={query}` -> `v_search_read_model`
  - `GET /parcel/{county_id}/{tax_year}/{account_number}` -> `parcel_summary_view`
  - `GET /quote/{county_id}/{tax_year}/{account_number}` -> `v_quote_read_model`
- Internal case and packet workflows remain on operational tables and admin routes.
- Restricted MLS/listing artifacts remain outside public payloads.

## Verification

```bash
python3 -m infra.scripts.run_migrations
python3 -m pytest tests/unit/test_stage11_migration_contract.py tests/unit/test_stage13_migration_contract.py tests/unit/test_stage14_migration_contract.py
```

For schema introspection on a local database:

```bash
psql "$DWELLIO_DATABASE_URL" -Atqc "SELECT max(version) FROM schema_migrations;"
psql "$DWELLIO_DATABASE_URL" -Atqc "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 10;"
```
