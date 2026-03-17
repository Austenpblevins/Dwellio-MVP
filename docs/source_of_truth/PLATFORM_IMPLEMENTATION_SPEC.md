# PLATFORM_IMPLEMENTATION_SPEC.md

## Purpose
This document defines the canonical platform and data-backbone implementation for Dwellio.

This is the controlling specification for:
- architecture
- source acquisition
- ingestion
- raw/staging/canonical/derived layers
- canonical schema
- county adapter contracts
- tax and ownership backbone
- lineage and validation
- admin and ops support

This document is not the main authority for quote formulas or protest recommendation logic unless those items are required to support backbone interfaces.
Those belong in `QUOTE_ENGINE_PRODUCT_SPEC.md`.

---

## Platform Goal
Build a scalable Texas property-tax data platform for Dwellio that supports:
- clean multi-county ingestion
- cross-county canonical normalization
- parcel search
- tax and ownership support
- downstream valuation and protest logic
- operational traceability
- admin inspection and recovery

For MVP, the platform must support Harris and Fort Bend residential parcel data well enough to power quote-ready parcel-year records.

---

## MVP Platform Scope
Must include:
- modular monolith repo scaffold
- source registry
- raw file storage lifecycle
- ingestion jobs
- county adapter framework
- staging tables
- canonical parcel/tax/owner/deed/GIS model
- validation framework
- dataset publish/versioning framework
- parcel search index support
- effective tax-rate support
- admin inspection support
- manual upload fallback
- documentation and runbooks

Must not require in MVP:
- statewide county support
- commercial data model extensions in execution logic
- advanced ML serving infrastructure
- legal filing automation across all districts

---

## Architecture Style
Use a modular monolith for MVP, designed for later service extraction.

Recommended stack:
- Frontend: Next.js
- Backend/API: TypeScript app layer
- ETL/integration: Python
- Database: PostgreSQL
- GIS: PostGIS
- Queue/jobs: pg-boss or equivalent
- Object storage: S3-compatible storage
- Auth: Supabase Auth or equivalent
- Observability: structured logging, metrics, and error tracking
- PDF generation: server-side HTML to PDF
- Address parsing: pluggable interface, libpostal-compatible if available
- Geocoding: pluggable provider interface

---

## Core Modules
1. source registry
2. raw file storage
3. ingestion job orchestration
4. county adapters
5. raw loaders
6. staging transformers
7. canonical normalization
8. tax computation support
9. search/indexing
10. ownership/deed reconciliation
11. protest/evidence support tables
12. admin QA and monitoring
13. API layer
14. frontend/admin pages

---

## Source Registry Requirement
Create a first-class source registry in both configuration and database form.

For each dataset define:
- source name
- source type
- county coverage
- entity coverage
- official URL if available
- fallback source
- access method
- file format
- cadence
- reliability tier
- auth/manual requirements
- legal/license notes
- parser module name
- adapter name
- active flag

The system must not hardcode file URLs in business logic.
Use source registry plus county adapter configuration.

---

## Data Acquisition Specification
For Harris and Fort Bend, the platform must support acquisition of:
1. appraisal roll / parcel accounts
2. building/improvement detail
3. land detail
4. exemptions
5. tax unit references if available
6. tax rate references
7. GIS boundaries if available
8. deed/ownership records or source hooks
9. manual upload fallback

Supported acquisition methods:
- direct file download
- scraper-assisted download when lawful and stable
- manual upload
- URL-based ingestion
- scheduled refresh
- ad hoc rerun by tax year
- dry-run rerun

Required raw-file lifecycle:
1. acquire raw file
2. checksum file
3. store immutable raw copy
4. detect file type
5. parse to staging
6. validate schema
7. normalize to canonical
8. run QA
9. publish dataset version
10. mark job success/failure

---

## County Adapter Contracts
Every county adapter must implement the same interface.

Required methods:
- `list_available_datasets(county, tax_year)`
- `acquire_dataset(dataset_type, tax_year)`
- `detect_file_format(file)`
- `parse_raw_to_staging(file)`
- `normalize_staging_to_canonical(job_id, tax_year)`
- `validate_dataset(job_id, tax_year)`
- `publish_dataset(job_id, tax_year)`
- `rollback_publish(job_id)`
- `get_adapter_metadata()`

Required metadata:
- county name
- appraisal district name
- supported years
- supported dataset types
- known limitations
- primary keys used
- special parsing notes
- manual fallback instructions

Initial adapters:
- `HarrisCountyAdapter`
- `FortBendCountyAdapter`

Do not create one-off county logic outside this interface.

---

## Data Layers
Implement four layers.

### Layer 1: Raw
Immutable record of acquired files and extracts.
Never updated except metadata flags.

### Layer 2: Staging
Source-shaped parsed tables.
County-specific structure is allowed here.
May be rebuilt by ingestion job.

### Layer 3: Canonical
Stable cross-county normalized tables.
This is the main source for downstream logic.

### Layer 4: Derived / Product
Search indexes, parcel summaries, tax estimate views, quote-ready views, protest-facing read models.

---

## Canonical Entity Model
Build around Texas-stable concepts, not county-specific field names.

Core entities:
- county
- appraisal_district
- source_system
- source_file
- ingestion_job
- import_batch
- parcel
- parcel_identifier
- parcel_address
- parcel_year_snapshot
- property_characteristics
- improvement
- land_segment
- exemption_record
- taxing_unit
- tax_rate
- parcel_tax_unit_assignment
- deed_record
- deed_party
- parcel_owner_period
- current_owner_rollup
- parcel_geometry
- school_district_boundary
- mud_boundary
- validation_result
- lineage_record
- evidence_packet
- protest_case
- valuation_run

Use UUID primary keys internally while also storing natural source keys.

---

## Canonical Schema Principles
All canonical tables should include where relevant:
- `id`
- `created_at`
- `updated_at`
- source reference or lineage fields
- `tax_year` / effective-year fields where relevant
- `active` or version flags where needed

Important rules:
- one stable parcel entity across years
- current account number is convenience only; history belongs in identifiers
- county-specific names stay in adapter config or staging
- canonical tables must remain cross-county
- append/version preferred over destructive overwrite

---

## Backbone Domains
The backbone must fully support these domains:
1. counties
2. appraisal districts
3. source systems
4. source files
5. ingestion jobs
6. import batches
7. raw county records
8. staging county records
9. canonical parcels
10. parcel yearly snapshots
11. property characteristics
12. improvements/buildings
13. land details
14. exemptions
15. taxing units
16. tax rates
17. parcel-to-tax-unit assignments
18. GIS geometries and boundaries
19. deeds and ownership transfers
20. owner parties
21. current owner rollups
22. search indexes
23. valuation support interfaces
24. protest/evidence support tables
25. audit and lineage
26. QA validation results
27. user/admin auth support
28. documents and uploads

---

## Search and Read Support
The platform must support:
- address search
- account number search
- normalized parcel summary read model
- quote-ready lookup by parcel-year

Search should not depend on staging tables.
Search should be powered from canonical and derived/search-support structures.

---

## Tax and Ownership Backbone Rules
Do not:
- store only a flat effective tax rate without components when components are available
- overwrite current owner directly from newest source without lineage
- assume every parcel has one school district already linked
- assume deed consideration is always present
- assume every exemption can be flattened into one number
- build county-specific hacks into canonical tables

Must support:
- component tax-rate storage by taxing unit and year
- parcel-to-tax-unit assignments with confidence or provenance
- ownership rollup with traceable history
- deed-source integration hooks or records
- exemption records by parcel-year and type

---

## Validation and QA Framework
The platform must support validation at multiple levels:
- file/schema validation
- staging row validation
- canonical publish validation
- tax assignment validation
- ownership reconciliation validation
- completeness dashboards
- ingestion failure inspection

Validation results must be persisted and inspectable.

---

## Publish / Versioning Framework
Do not normalize directly into the only live production view with no versioning.

Support:
- dataset publish states
- versioned publish identifiers
- dry runs
- rollback of publish
- job status and summaries
- lineage from derived rows back to source/import/job where practical

---

## Admin and Ops Requirements
Provide internal support for:
- running full county load
- rerunning failed jobs
- manual upload and manual file registration
- publishing a dataset
- rolling back a publish
- investigating validation failures
- rebuilding search indexes
- inspecting tax-assignment gaps
- inspecting source lineage

---

## Repo Structure
Suggested structure:

```text
/apps/web
/apps/admin
/packages/db
/packages/shared
/packages/search
/packages/tax
/services/etl
/services/jobs
/config/counties/harris
/config/counties/fort_bend
/sql/migrations
/docs
/scripts
```

This can be adapted as long as the separation of concerns remains intact.

---

## Documentation Requirements
Create and maintain:
- `docs/architecture_overview.md`
- `docs/canonical_schema.md`
- `docs/source_registry.md`
- `docs/ingestion_framework.md`
- `docs/harris_adapter.md`
- `docs/fort_bend_adapter.md`
- `docs/tax_unit_assignment.md`
- `docs/exemption_model.md`
- `docs/ownership_reconciliation.md`
- `docs/search_architecture.md`
- `docs/ops_runbook.md`

---

## Testing Requirements
### Unit tests
- address normalization helpers
- county adapter parsing
- field transforms
- tax rate math
- assignment precedence
- owner reconciliation scoring

### Integration tests
- end-to-end ingestion per county
- staging-to-canonical publish
- parcel search
- tax summary generation
- validation rule execution

---

## Migration Strategy
Use staged SQL migrations in this order:
1. foundation tables
2. ingestion ops tables
3. parcel core tables
4. tax tables
5. GIS tables
6. deed/owner tables
7. search tables
8. protest/evidence support tables
9. derived views
10. seed data

---

## Coding Style Rules
1. Prefer explicit over clever.
2. Prefer configuration-driven county mapping.
3. Prefer append/version over destructive update.
4. Every parser should fail loudly on unknown critical schema shifts.
5. Use typed interfaces and validation schemas.
6. Keep business logic out of raw/staging parsing.
7. Separate canonical normalization from product derivations.

---

## Data Backbone Acceptance Criteria
The backbone build is successful when:
1. Harris County can be ingested end-to-end for a target tax year into canonical tables.
2. Fort Bend can be ingested end-to-end for a target tax year into canonical tables.
3. parcel search works by address and account number.
4. each parcel summary can show:
   - annual market value
   - assessed value if available
   - exemptions summary
   - component tax rate breakdown
   - total effective tax rate
   - owner summary
5. parcel-to-tax-unit assignments exist with confidence scores or provenance.
6. tax rates are stored by taxing unit and year.
7. source files and transformations are traceable.
8. admin users can run and inspect ingestion jobs.
9. manual upload fallback works.
10. data completeness and validation dashboards exist.

---

## Platform Execution Order
Execute backbone work in this order:
1. scaffold repo and environment
2. enable database + PostGIS
3. implement foundation schema
4. implement ingestion ops schema
5. implement canonical parcel/tax/owner schema
6. seed core reference tables
7. implement county adapter framework
8. implement raw file registry and storage handling
9. implement Harris adapter
10. implement Fort Bend adapter
11. implement validation framework
12. implement publish/versioning framework
13. implement parcel search index
14. implement tax-unit assignment engine
15. implement effective tax-rate derivations
16. implement owner reconciliation
17. implement admin UI
18. implement public parcel summary/read support
19. implement protest/evidence support tables
20. write docs and runbooks
21. add tests and fixtures
22. optimize performance

---

## Final Platform Instruction
Use this file as the source of truth for platform/backbone implementation.
Do not let product convenience or quote-engine shortcuts distort the canonical data model.
Build the backbone first so the quote engine sits on stable, traceable parcel-year records.
