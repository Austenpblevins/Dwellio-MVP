# CANONICAL_CONTEXT.md

## Purpose
This file is the single authority map for all Dwellio build execution in Codex.

Codex must always treat this document as the highest-priority implementation context file.

This file exists because the current Dwellio planning set contains two long documents with intentional overlap:
- `DWELLIO MASTER IMPLEMENTATION SPEC.md`
- `DWELLIO_MASTER_SPEC.md`

Those documents should not be treated as equal master specs during implementation.
This file defines which document controls which topic so Codex does not blend conflicting abstractions or duplicate work.

---

## Product Objective
Dwellio is a Texas residential property tax protest platform.

For the MVP, the system must:
1. resolve a homeowner address to the correct county parcel
2. ingest and normalize county parcel and tax data for Harris and Fort Bend
3. compute a defensible protest value using market-value and unequal-appraisal logic
4. estimate likely tax savings
5. produce a protest recommendation
6. capture leads and create protest cases
7. support internal operations, data QA, and future packet generation

The key product number is **not** a generic market value.
The key number is the **lowest supportable protest value**.

For each parcel-year:
- compute market-value logic
- compute unequal-appraisal logic
- final defensible value = `min(market_value, unequal_appraisal_value)`

---

## MVP Boundary
Counties:
- Harris
- Fort Bend

Property type:
- single-family residential only

Included MVP capabilities:
- source registry
- county adapter framework
- raw/staging/canonical/derived data layers
- parcel search
- parcel normalization
- tax-unit and rate support
- exemption support
- ownership support
- quote engine
- savings estimate
- protest recommendation
- lead capture
- protest case creation
- admin inspection of jobs and data quality
- nightly or scheduled ETL
- precomputed quote/read model

Explicitly out of MVP execution logic:
- commercial property valuation logic
- statewide county rollout beyond Harris and Fort Bend
- full production ML training pipelines
- full filing automation across all districts
- attorney/legal workflow automation
- mobile app

The architecture must still prepare for future expansion.

---

## Authority Order
Codex must follow this precedence order:

1. `CANONICAL_CONTEXT.md`
2. `PLATFORM_IMPLEMENTATION_SPEC.md`
3. `QUOTE_ENGINE_PRODUCT_SPEC.md`
4. direct user instructions in the current prompt if they clearly override a lower-level implementation detail

If there is any conflict, follow the highest item in this list.

---

## Topic Authority Map

### PLATFORM_IMPLEMENTATION_SPEC.md controls:
- system architecture
- source registry
- data acquisition
- county adapter contracts
- raw/staging/canonical/derived layer design
- canonical entity model
- canonical schema definitions
- parcel, owner, exemption, tax-unit, deed, GIS, lineage, and validation architecture
- import batch and raw file handling
- publish/versioning framework
- manual upload fallback
- admin QA and operations framework
- migration order
- implementation order for the data backbone
- runbooks, observability, and validation dashboards

### QUOTE_ENGINE_PRODUCT_SPEC.md controls:
- product objective details related to quoting and protest decisions
- market-value logic
- unequal-appraisal logic
- comp selection rules
- defensible value rules
- savings engine rules
- protest recommendation logic
- confidence scoring
- ARB / win probability logic
- quote response fields
- lead flow and protest case product workflow
- public quote/search API behavior
- packet and valuation-related product requirements

---

## Conflict Rules
If there is any conflict on these topics, `PLATFORM_IMPLEMENTATION_SPEC.md` wins:
- schema naming for backbone entities
- ingestion design
- county adapter structure
- raw/staging/canonical/derived separation
- tax-unit assignment structure
- ownership lineage
- exemptions normalization
- GIS and boundary modeling
- source registry behavior
- validation and publish/versioning framework
- job orchestration and ops structure

If there is any conflict on these topics, `QUOTE_ENGINE_PRODUCT_SPEC.md` wins:
- comp scoring
- valuation formulas
- defensible value rules
- savings estimate formulas
- protest recommendation rules
- confidence thresholds
- quote outputs
- public quote logic
- ARB probability logic
- case-selection logic

If there is still ambiguity, prefer:
1. preserving platform correctness over speed
2. preserving canonical data architecture over product shortcuts
3. precomputed read models over heavy live calculations
4. append/version history over destructive overwrite

---

## Non-Negotiable Build Rules
1. Do not collapse raw, staging, canonical, and derived layers.
2. Do not expose restricted MLS or other restricted source data in public APIs.
3. Do not hardcode Harris or Fort Bend field mappings outside county adapter/config layers.
4. Do not build the public quote request path around heavy live comp analysis.
5. Do not tie frontend behavior directly to staging tables.
6. Do not overwrite valuation, owner, or ingestion history destructively.
7. Do not flatten tax logic into a single simplistic rate if component rates are available.
8. Do not implement commercial logic in the MVP.
9. Do not let product-specific shortcuts distort canonical data model design.
10. Prefer configuration-driven and testable implementation over one-off county hacks.

---

## Required Architectural Pattern
Dwellio MVP should be built as a modular monolith with clean separation of concerns.

Recommended stack:
- app/frontend: Next.js
- app/backend: Python-based app/API layer
- ETL/integration: Python
- database: PostgreSQL + PostGIS
- jobs/queue: pg-boss or similar
- object storage: S3-compatible
- auth: Supabase Auth or equivalent
- PDF generation: server-side HTML-to-PDF

This stack can be adjusted only if the replacement preserves the same architectural behavior.

---

## Required Data Flow
Every county dataset must move through this shape:

1. acquire raw files
2. archive immutable raw copies
3. checksum and register files/import batch/job
4. parse into staging tables
5. validate schema and field expectations
6. normalize into canonical cross-county tables
7. run QA and lineage tracking
8. publish a versioned dataset state
9. build derived/product tables and read models
10. refresh quote/search outputs

---

## Required Build Sequence
Codex should generally implement in this order:

### Phase 1 — foundation
- repo scaffold
- environment configuration
- database extensions
- base schema foundations
- source registry
- ingestion/job tables
- reference tables

### Phase 2 — canonical data backbone
- parcels and identifiers
- parcel addresses
- parcel year snapshots
- improvements and land
- exemptions
- taxing units and tax rates
- parcel-to-tax-unit assignments
- ownership/deed model
- geometry/boundaries
- validation framework
- publish/versioning framework

### Phase 3 — county ingestion
- county adapter framework
- Harris adapter
- Fort Bend adapter
- raw file registry and storage lifecycle
- staging loaders
- normalization flows
- manual upload fallback
- QA dashboards / admin inspection tools

### Phase 4 — search and product read layer
- address normalization and parcel search
- derived parcel summary/read model
- effective tax-rate derivations
- quote-ready read tables/views

### Phase 5 — quote engine and product logic
- feature layer
- comp candidate generation
- market-value model
- unequal-appraisal model
- defensible value logic
- savings engine
- protest recommendation engine
- explanation builder

### Phase 6 — workflow
- leads
- clients
- representation agreements
- protest cases
- hearing and outcome storage
- admin tooling

### Phase 7 — hardening
- tests
- fixtures
- docs
- runbooks
- performance improvements
- future-phase scaffolding

---

## Prompting Rules for Codex
When running Codex:
- Always attach `CANONICAL_CONTEXT.md`.
- Attach `PLATFORM_IMPLEMENTATION_SPEC.md` for platform, schema, ETL, county ingestion, search, admin, or ops tasks.
- Attach `QUOTE_ENGINE_PRODUCT_SPEC.md` for quote engine, valuation, comp, savings, recommendation, or case-selection tasks.
- For mixed tasks, attach all three, but Codex must still obey the authority hierarchy in this file.

---

## Definition of Success
The MVP build is successful when:
- Harris and Fort Bend can be ingested into canonical tables with traceable lineage
- parcel search works reliably by address and account number
- each parcel-year can produce a quote from precomputed data
- quote output includes defensible value, savings range, confidence, and protest recommendation
- lead capture and protest case creation work
- admins can inspect job status, validation failures, and source lineage
- the system is ready to expand without redesigning core canonical models

---

## Final Instruction to Codex
Use this file as the controlling authority map.
Do not merge duplicate sections from the other specs blindly.
Use the platform spec for backbone architecture.
Use the quote engine product spec for valuation and protest logic.
When uncertain, choose the option that reduces future rework and preserves canonical architecture.
