# Tax Data Pull Spec to Repo Mapping

This file reconciles the detailed Tax Data Pull implementation documents with the canonical Dwellio repository.

## Summary

The Tax Data Pull spec is compatible with Dwellio, but several concepts in that spec are more detailed or use different names than the current repo. When names differ, the repo schema and `docs/source_of_truth/*` are authoritative.

## Canonical backend decision

Use the following stack split:

- Backend/API: Python
- ETL/jobs: Python
- Database: PostgreSQL / Supabase
- Frontend: React / Next.js only if used as frontend

This resolves any ambiguity from imported docs that mention a TypeScript or Next.js backend.

## Table / concept mapping

| Tax Data Pull concept | Canonical repo table / mechanism |
|---|---|
| `source_files` | `import_batches`, `raw_files` |
| `ingestion_jobs` | `job_runs` |
| `validation_results` | `ingest_errors` plus future validation outputs |
| `lineage_records` | `import_batches`, `raw_files`, `source_record_hash`, `job_runs.metadata_json` |
| `parcel_identifiers` | `parcel_aliases`, `account_number`, `cad_property_id`, `geo_account_number`, `quick_ref_id` |
| `parcel_year_snapshots` | `parcel_assessments` plus parcel-year warehouse tables |
| `search_documents` | `v_search_read_model` |
| `current_owner_rollups` | `parcels.owner_name` plus `parcel_ownership_history` and future owner rollups |
| deed-based sale normalization | `sales_raw` -> `parcel_sales` in `job_sales_ingestion.py` |
| tax units / rates | `taxing_units`, `parcel_taxing_units`, `effective_tax_rates`, `tax_rate_histories` |

## Required implementation rules

### Do not create duplicate parcel canonicals
Use:
- `parcels`
- `parcel_addresses`
- `parcel_assessments`
- `parcel_exemptions`
- `parcel_aliases`

Do not create a second parcel identity system in parallel.

### Do not create a second search system
The repo already defines `v_search_read_model`. If future performance work requires a materialized search structure, document it as an optimization rather than a second canonical architecture.

### Keep deed ingestion inside the existing sales pipeline
Tax-data-pull deed logic should feed:
- `sales_raw`
- `parcel_sales`

through `job_sales_ingestion.py`, not through a separate quote-path or duplicate tax-path engine.

### Keep tax-detail expansion additive
Tax-unit, exemption, ownership, and GIS enrichment are valid extensions so long as they map back into the current repo’s schema families and read-model strategy.

## Safe next implementation priority
1. finish canonical repo migrations and views
2. wire ETL jobs to existing schema
3. merge tax-data-pull concepts into current job/adapter design
4. avoid introducing duplicate canonical tables unless explicitly approved
