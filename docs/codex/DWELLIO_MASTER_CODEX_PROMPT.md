# DWELLIO MASTER CODEX PROMPT

This file consolidates the session-start prompt, prompt library, execution sequence, execution checklist, task prompts, and task runbook into one canonical Codex operating file for building Dwellio from early repository setup through final completion.

Use this as the single prompt reference when working in Codex.

---

## Purpose

This file is designed to do four things at once:

1. give Codex the architectural context it needs at the start of every session
2. point Codex to the correct source-of-truth documents inside the repo
3. provide the exact step-by-step prompt sequence from repo foundation through final implementation
4. preserve the practical execution rules, guardrails, audit prompts, and completion checks that were previously spread across multiple prompt files

This file is intentionally detailed. It is meant to reduce ambiguity and prevent Codex from inventing architecture, skipping stages, or building out of order.

---

## How to use this file

Use this document in three ways:

### 1. At the start of every Codex session
Paste the **Session Start Master Prompt** from this document first.

### 2. For each implementation stage
Paste only the stage prompt for the exact stage you are currently working on.

### 3. At the end of each stage
Require Codex to return the completion checklist defined in this file before moving to the next stage.

Do not allow Codex to jump ahead just because it can scaffold future files. The order matters.

---

## Source of truth document set

Before any implementation work, Codex must read the following files in this repo.

### Primary source-of-truth files
- `docs/source_of_truth/AGENT_RULES.md`
- `docs/source_of_truth/CANONICAL_CONTEXT.md`
- `docs/source_of_truth/DWELLIO_BUILD_PLAN.md`
- `docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md`
- `docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md`
- `docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md`
- `docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md`

### Supporting Codex control files
- `docs/codex/codex-system-context.md`
- `docs/codex/codex-guardrails.md`

### Runbooks and architecture state files
- `docs/runbooks/CANONICAL_PRECEDENCE.md`
- `docs/runbooks/ARCHITECTURE_STATE.md`
- `docs/runbooks/ARCHITECTURE_MAP.md`
- `docs/runbooks/TAX_DATA_PULL_REPO_MAPPING.md`

### Architecture reference files
- `docs/architecture/api-contracts.md`
- `docs/architecture/comp-engine.md`
- `docs/architecture/domain-scoring-formulas.md`
- `docs/architecture/frontend-page-spec.md`
- `docs/architecture/implementation-spec.md`
- `docs/architecture/job-runner-architecture.md`
- `docs/architecture/neighborhood-and-market-stats.md`
- `docs/architecture/sales-reconstruction-engine.md`
- `docs/architecture/schema-reference.md`
- `docs/architecture/testing-observability-security.md`
- `docs/architecture/valuation-savings-recommendation-engine.md`

---

## Canonical precedence rule

Codex must use the following precedence order when documents appear to conflict:

1. `docs/source_of_truth/*`
2. actual repo schema, migration files, read models, and implementation already present in the repo
3. `docs/runbooks/*`
4. `docs/architecture/*`
5. imported planning notes or older prompt documents

If a prompt references a file that does not exist exactly under that name, Codex must not guess silently. It should use the closest matching source-of-truth document already present in the repo and explicitly state the mapping it is using.

Example:
- If a prompt references `DWELLIO_MASTER_SPEC.md` but that exact file is not present, use the combined authority of:
  - `CANONICAL_CONTEXT.md`
  - `PLATFORM_IMPLEMENTATION_SPEC.md`
  - `QUOTE_ENGINE_PRODUCT_SPEC.md`
  - `DWELLIO_BUILD_PLAN.md`
  - `DWELLIO_SCHEMA_REFERENCE.md`
  - `AGENT_RULES.md`
  - `DWELLIO_CODEX_CONTEXT.md`

---

## Canonical repo structure

Use the attached repo structure as canonical unless the source-of-truth docs explicitly authorize a change.

```text
Dwellio/
  app/
    api/
    services/
    jobs/
    models/
    county_adapters/
      common/
      harris/
      fort_bend/
    db/
      migrations/
      views/
    utils/
  docs/
    codex/
    source_of_truth/
    architecture/
    runbooks/
    imported_specs/
  tests/
    unit/
    integration/
    fixtures/
  infra/
    supabase/
    scripts/
  sql/
```

### Existing repo reference files already present
- `README.md`
- `FINAL_MANIFEST.md`
- `sql/dwellio_full_schema.sql`

### Practical notes
- The convenience full schema file in `sql/dwellio_full_schema.sql` is a reference/bootstrap artifact, not the authoritative migration history.
- Migration execution order must remain controlled through the migration folder.
- Do not create parallel schema families, duplicate read models, or multiple conflicting search systems.

---

## Core architecture rules

These rules apply in every stage.

1. Backend is Python-first.
2. ETL and jobs are Python-first.
3. Frontend may use React / Next.js, but backend authority remains Python unless the source-of-truth docs are explicitly changed.
4. The system is parcel-year centric.
5. Public APIs must read from precomputed read models.
6. MLS / private listing data must remain restricted and must not appear in public APIs.
7. `defensible_value = min(market_model_output, unequal_appraisal_output)`
8. All valuation runs must be persisted.
9. Do not silently change architecture.
10. Do not introduce duplicate architectures, schemas, or table families.
11. Do not collapse the layered flow:
    - raw -> staging -> canonical -> derived -> read models -> API -> frontend
12. Do not run heavy comp generation inside the public quote endpoint.
13. Preserve valuation history.
14. Preserve county adapter separation.
15. Prefer small safe changes over broad refactors.

---

## Session Start Master Prompt

Paste this at the start of every Codex session.

```text
You are working inside the Dwellio property tax protest platform repository.

Before doing any work, read the following files:

docs/codex/codex-system-context.md
docs/codex/codex-guardrails.md

docs/source_of_truth/AGENT_RULES.md
docs/source_of_truth/CANONICAL_CONTEXT.md
docs/source_of_truth/DWELLIO_BUILD_PLAN.md
docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md
docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md
docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md
docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md

docs/runbooks/CANONICAL_PRECEDENCE.md
docs/runbooks/ARCHITECTURE_STATE.md
docs/runbooks/ARCHITECTURE_MAP.md
docs/runbooks/TAX_DATA_PULL_REPO_MAPPING.md

docs/architecture/api-contracts.md
docs/architecture/comp-engine.md
docs/architecture/domain-scoring-formulas.md
docs/architecture/frontend-page-spec.md
docs/architecture/implementation-spec.md
docs/architecture/job-runner-architecture.md
docs/architecture/neighborhood-and-market-stats.md
docs/architecture/sales-reconstruction-engine.md
docs/architecture/schema-reference.md
docs/architecture/testing-observability-security.md
docs/architecture/valuation-savings-recommendation-engine.md

Treat docs/source_of_truth/* as highest precedence.
Treat actual repo schema and code as higher authority than summaries, imported planning docs, or older prompt files.

Core rules:
1. Dwellio is Python-first for backend and ETL.
2. Frontend may use React/Next.js, but backend authority remains Python unless source-of-truth docs explicitly change that decision.
3. Public APIs must use precomputed read models.
4. Restricted MLS/listing data must never appear in public APIs.
5. defensible_value = min(market_model_output, unequal_appraisal_output).
6. Preserve parcel-year centric design.
7. Preserve valuation history.
8. Do not silently create duplicate canonical tables, quote systems, or search systems.
9. Do not collapse raw -> staging -> canonical -> derived -> read models.
10. If you detect architecture conflicts, explain them before editing files.

Behavior rules:
- only modify files related to the requested scope
- prefer small, safe changes
- if a prompt references a missing file name, map it to the closest existing source-of-truth files and say so
- do not guess silently
- return files changed, commands to run, tests to run, and any blockers

When ready, wait for the next instruction describing the specific task.
```

---

## Master “All-in-One” Codex Prompt

Use this when you want Codex to understand the whole project and the exact build philosophy before you move stage by stage.

```text
You are working inside the Dwellio repository, which is a production-grade Texas property tax protest platform.

Your job is to build and reconcile the system in the correct order without inventing parallel architectures.

Before making any changes, read:
- docs/codex/codex-system-context.md
- docs/codex/codex-guardrails.md
- docs/runbooks/CANONICAL_PRECEDENCE.md
- docs/runbooks/ARCHITECTURE_STATE.md
- docs/runbooks/ARCHITECTURE_MAP.md
- docs/runbooks/TAX_DATA_PULL_REPO_MAPPING.md
- docs/source_of_truth/AGENT_RULES.md
- docs/source_of_truth/CANONICAL_CONTEXT.md
- docs/source_of_truth/DWELLIO_BUILD_PLAN.md
- docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md
- docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md
- docs/source_of_truth/PLATFORM_IMPLEMENTATION_SPEC.md
- docs/source_of_truth/QUOTE_ENGINE_PRODUCT_SPEC.md
- docs/architecture/*

Treat source-of-truth docs as highest authority.
Treat actual repo schema and implementation as higher authority than summaries or imported planning docs.

Hard rules:
1. Dwellio is Python-first for backend and ETL.
2. Frontend may use React/Next.js, but backend authority remains Python unless source-of-truth docs explicitly change that.
3. Preserve parcel-year centric design.
4. Public quote APIs must use precomputed read models.
5. Restricted MLS/listing data must never appear in public APIs.
6. defensible_value = min(market_value_model_output, unequal_appraisal_output).
7. Preserve valuation history.
8. Do not silently create duplicate canonical tables, search systems, or quote systems.
9. Do not collapse raw -> staging -> canonical -> derived -> read models.
10. Do not silently change migrations order or core schema relationships.
11. If there is a conflict, explain it before editing.

Operational rules:
- work stage by stage
- do not jump ahead
- keep all code production-oriented
- use SQL-first migrations
- preserve county adapter separation
- preserve provenance and lineage
- keep public-safe endpoints separate from internal/admin workflows
- do not expose restricted sales data publicly
- do not substitute convenience schema files for ordered migrations

At the end of each stage, always return:
1. files created or modified
2. migrations added or changed
3. commands to run
4. what to test
5. what passed
6. what remains blocked
7. the safest next step

Wait for me to specify the exact stage or task.
```

---

# STEP-BY-STEP MASTER PROMPT LIST

The stages below reflect the full process from early foundation to final completion. Use them in order.

---

## Stage 0 — Create the repo foundation and guardrails

### Objective
Set up the project structure, tooling, environment management, migration workflow, and core conventions before any feature work begins.

### Build in this stage
- monorepo or organized repo structure
- package manager setup
- TypeScript setup for frontend if used
- Python ETL environment
- PostgreSQL + PostGIS configuration
- migration framework
- env file structure
- linting / formatting
- test framework
- basic README
- docs folder
- config folder for county adapters

### Must be true before moving on
- project boots locally
- database connects
- migrations can run
- PostGIS is enabled
- app and ETL folders exist
- one command can start local dev

### Prompt for Codex
```text
Use the source-of-truth docs and this repository as the canonical architecture reference.

Stage 0 objective:
Create the repo foundation for Dwellio as a production-grade Texas property tax appeal platform.

Build:
- repo structure
- Next.js frontend structure only if needed for frontend
- Python backend/app API structure
- Python ETL service structure
- PostgreSQL connection setup
- PostGIS enablement
- migration tooling
- environment variable handling
- linting, formatting, and testing setup
- docs scaffolding
- config scaffolding for county adapters

Constraints:
- modular monolith
- Python-first backend and ETL
- scalable folder structure
- no feature logic yet
- no fake business logic
- keep all code production-oriented
- preserve the existing repo structure where already present

Deliver:
- repo tree
- all setup files
- starter scripts
- local run instructions
- migration baseline
```

---

## Stage 1 — Build the core canonical database schema

### Objective
Create the stable schema backbone first. No ingestion logic yet.

### Build in this stage
- counties
- appraisal_districts
- source_systems
- source_files
- ingestion_jobs
- validation_results
- lineage_records
- parcels
- parcel_identifiers
- parcel_addresses
- parcel_year_snapshots
- property_characteristics
- improvements
- land_segments
- value_components
- exemption_types
- parcel_exemptions
- taxing_units
- tax_rates
- parcel_tax_unit_assignments
- parcel_geometries
- taxing_unit_boundaries
- deed_records
- deed_parties
- parcel_owner_periods
- current_owner_rollups
- search_documents
- protest_cases
- evidence_packets
- valuation_runs
- case_outcomes
- override tables

### Must be true before moving on
- all schema migrations run cleanly
- indexes exist
- foreign keys work
- seed tables populate
- no redesign needed for Harris or Fort Bend

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 1 objective:
Implement the full canonical database schema and migrations for Dwellio.

Build:
- all core tables defined in the source-of-truth docs
- indexes
- foreign keys
- enum strategy where appropriate
- PostGIS geometry support
- override tables
- seed data for counties, appraisal districts, exemption types, and basic unit types

Requirements:
- SQL-first migrations
- keep schema normalized
- version-safe and tax-year aware
- do not skip lineage, validation, ingestion ops, or GIS tables
- do not rely on sql/dwellio_full_schema.sql as the migration history
- no app screens yet

Deliver:
- migration files in execution order
- seed scripts
- schema documentation comments where useful
- a short test script proving migration success
```

---

## Stage 2 — Build the ingestion framework and adapter architecture

### Objective
Create the reusable ingestion engine before building any county-specific importer.

### Build in this stage
- adapter interface
- ingestion job runner
- raw file acquisition abstraction
- raw file metadata tracking
- staging table pattern
- parse pipeline
- normalize pipeline
- validate pipeline
- publish pipeline
- rollback hooks
- dry-run support
- job status logging

### Must be true before moving on
- a dummy dataset can flow raw -> staging -> canonical
- jobs can succeed/fail cleanly
- validation errors can be stored
- publish versioning exists

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 2 objective:
Build the ingestion framework and county adapter architecture for Dwellio.

Build:
- county adapter contract/interface
- ingestion job orchestration
- raw file acquisition abstraction
- raw file storage metadata handling
- staging table loading pattern
- normalization pipeline pattern
- validation execution framework
- publish/versioning workflow
- rollback hooks
- dry-run mode

Requirements:
- county-specific logic must live in adapters
- framework must support Harris and Fort Bend without redesign
- jobs must be idempotent and auditable
- structured logs for each stage
- do not implement real Harris/Fort Bend parsing yet beyond placeholders

Deliver:
- ingestion framework code
- adapter base classes/interfaces
- sample dummy adapter
- job runner
- example end-to-end flow with fixture data
```

---

## Stage 3 — Create county configuration files and field dictionaries

### Objective
Define county behavior through config before parsing real county data.

### Build in this stage
- config/counties/harris/datasets.yaml
- config/counties/harris/field_mappings.yaml
- config/counties/fort_bend/datasets.yaml
- config/counties/fort_bend/field_mappings.yaml
- machine-readable canonical field dictionary
- dataset dependency rules
- null handling rules
- transformation hints

### Must be true before moving on
- Codex can point to config instead of hardcoding field names
- field mapping is reviewable
- dataset types are explicit

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 3 objective:
Create county configuration files and the canonical field dictionary.

Build:
- dataset config files for Harris and Fort Bend
- field mapping config files for Harris and Fort Bend
- canonical field dictionary storage and seed data
- dataset dependency definitions
- null handling definitions
- transformation notes structure

Requirements:
- no county field names hardcoded in core logic
- config must be easy to extend to future counties
- include comments or docs describing intended use

Deliver:
- YAML or JSON config files
- field dictionary schema + seed data
- documentation showing how adapters consume these mappings
```

---

## Stage 4 — Implement the Harris County adapter end-to-end

### Objective
Make one county work completely before touching Fort Bend.

### Build in this stage
- Harris dataset acquisition logic
- Harris staging parsers
- Harris normalization mapping
- Harris validation rules
- Harris publish routine
- Harris rerun workflow
- Harris fixture-based tests

### Must be true before moving on
- one Harris tax year loads successfully into canonical tables
- parcel rows, characteristics, exemptions, and source lineage exist
- validation results render correctly

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 4 objective:
Implement the Harris County adapter end-to-end.

Build:
- source registry entries for Harris
- Harris adapter module
- dataset acquisition hooks
- staging parsers
- canonical normalization transforms
- validation rules
- publish workflow
- rerun workflow
- fixture-based tests

Requirements:
- use adapter framework and config files
- no hardcoding into canonical layer
- preserve provenance
- retain tax-year versioning
- make it easy to inspect row counts and failed records

Deliver:
- working Harris adapter
- tests
- sample local run command
- documentation for Harris ingestion workflow
```

---

## Stage 5 — Implement the Fort Bend adapter end-to-end

### Objective
Prove the framework is truly multi-county, not Harris-specific.

### Build in this stage
- Fort Bend dataset acquisition logic
- Fort Bend staging parsers
- Fort Bend normalization mapping
- Fort Bend validation rules
- Fort Bend publish routine
- Fort Bend tests

### Must be true before moving on
- Fort Bend loads successfully without schema changes
- differences from Harris are handled via adapter/config
- framework survives second county cleanly

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 5 objective:
Implement the Fort Bend adapter end-to-end using the existing ingestion framework.

Build:
- source registry entries for Fort Bend
- Fort Bend adapter module
- staging parsers
- canonical normalization transforms
- validation rules
- publish workflow
- rerun workflow
- fixture-based tests

Requirements:
- do not redesign canonical schema
- do not add county-specific hacks into core tables
- solve differences through adapter/config layers
- prove the framework supports multiple counties correctly

Deliver:
- working Fort Bend adapter
- tests
- documentation for Fort Bend ingestion
- brief note on any source differences handled by adapter logic
```

---

## Stage 6 — Build the tax unit and tax rate subsystem

### Objective
Support actual parcel-level tax calculations with component rates.

### Build in this stage
- taxing unit reference management
- tax rate ingestion support
- rate versioning by year
- parcel-to-tax-unit assignment engine
- effective tax rate rollup view
- tax component breakdown view
- missing-assignment QA rules

### Must be true before moving on
- a parcel can show county/city/school/MUD component rates
- total effective rate is computed from components
- confidence scoring exists
- missing school district or MUD assignment is detectable

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 6 objective:
Build the tax unit, tax rate, and parcel-to-tax-unit assignment subsystem.

Build:
- taxing_units workflows
- tax_rates ingestion support
- parcel_tax_unit_assignments engine
- assignment precedence logic
- confidence scoring
- effective tax rate derived views
- component rate breakdown views
- QA checks for missing or conflicting assignments

Requirements:
- version everything by tax year
- support county, city, school district, MUD, and special district types
- allow GIS-based assignment and manual override later
- do not flatten rates into one opaque parcel field

Deliver:
- schema additions if needed
- assignment engine code
- derived views
- tests
- admin-readable outputs for debugging assignment coverage
```

---

## Stage 7 — Build GIS support and spatial assignment logic

### Objective
Handle school districts, MUDs, and special districts correctly.

### Build in this stage
- geometry ingestion helpers
- boundary ingestion
- centroid + polygon storage
- spatial join utilities
- parcel-to-boundary assignment support
- GIS QA checks

### Must be true before moving on
- spatial assignment can work when direct mappings do not
- school district / MUD assignment can be derived geographically
- invalid geometries are handled

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 7 objective:
Build GIS support and spatial tax-unit assignment utilities.

Build:
- PostGIS helper functions
- parcel geometry ingestion support
- taxing unit boundary ingestion support
- centroid and geometry storage
- spatial join assignment utilities
- GIS QA checks and geometry validation

Requirements:
- support parcel centroid and polygon workflows
- support school district and MUD boundary use cases
- keep GIS logic modular and auditable
- integrate with parcel_tax_unit_assignments framework

Deliver:
- GIS ingestion utilities
- spatial assignment functions
- tests with fixture geometries
- documentation for GIS-based assignment logic
```

---

## Stage 8 — Build the exemption normalization and annual rollup logic

### Objective
Represent Texas exemptions correctly enough to avoid future rework.

### Build in this stage
- exemption type seed set
- exemption normalization logic
- parcel exemption rollup view
- homestead / over65 / disabled / freeze flags
- annual exemption summaries
- QA checks for suspicious exemption patterns

### Must be true before moving on
- a parcel can carry multiple exemptions for one year
- exemption outputs are reviewable
- future tax computation logic can rely on the structure

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 8 objective:
Build exemption normalization and annual parcel exemption rollups.

Build:
- normalization logic for parcel exemptions
- exemption type master seed data
- parcel_exemption_rollup_view
- summary flags for homestead, over65, disabled, disabled veteran, and freeze
- QA checks for missing or conflicting exemption data

Requirements:
- support multiple exemptions per parcel per year
- preserve raw exemption codes
- do not flatten everything into a single amount only
- build for future tax computation support

Deliver:
- normalization code
- derived views
- tests
- documentation for exemption handling
```

---

## Stage 9 — Build deed ingestion hooks and ownership reconciliation

### Objective
Separate raw ownership evidence from current-owner rollup logic.

### Build in this stage
- deed record ingestion support
- deed party extraction
- deed-to-parcel linking support
- owner timeline generation
- current owner rollup derivation
- confidence scoring
- manual override support

### Must be true before moving on
- deed records can coexist with CAD owner names
- current-owner summary is derived, not blindly overwritten
- ownership lineage is preserved

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 9 objective:
Build deed ingestion support and ownership reconciliation logic.

Build:
- deed_records ingestion support
- deed_parties extraction
- deed-to-parcel linking utilities
- parcel_owner_periods generation
- current_owner_rollups derivation
- confidence scoring
- manual override hooks

Requirements:
- do not overwrite raw CAD owner values
- preserve source lineage
- support temporal conflict between deed timing and CAD snapshots
- make reconciliation auditable and explainable

Deliver:
- ownership reconciliation code
- derived owner views
- tests
- admin/debug tooling hooks
```

---

## Stage 10 — Build derived parcel summary views

### Objective
Create the internal data products the app will actually use.

### Build in this stage
- parcel_summary_view
- parcel_effective_tax_rate_view
- parcel_tax_estimate_summary
- parcel_owner_current_view
- parcel_data_completeness_view
- parcel_search_view

### Must be true before moving on
- core parcel screens can be powered from stable derived views
- app code no longer needs to touch raw canonical joins directly
- completeness scoring exists

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 10 objective:
Build the derived parcel summary views used by the application and admin tools.

Build:
- parcel_summary_view
- parcel_effective_tax_rate_view
- parcel_tax_estimate_summary
- parcel_owner_current_view
- parcel_search_view
- parcel_data_completeness_view

Requirements:
- derived views must use canonical tables only
- expose enough fields for public parcel summary and internal admin review
- include warning flags and completeness scoring where specified

Deliver:
- SQL/view definitions
- tests or validation queries
- documentation of each view’s purpose
```

---

## Stage 11 — Build search architecture and parcel lookup API

### Objective
Get the highest-value user interaction working: fast parcel search.

### Build in this stage
- address normalization
- search document generation
- trigram / fuzzy matching
- account-number exact match
- owner-name fallback
- autocomplete API
- parcel summary API

### Must be true before moving on
- search is fast and useful
- address lookups work against normalized parcel data
- parcel detail retrieval is stable

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 11 objective:
Build the parcel search subsystem and parcel lookup API.

Build:
- address normalization pipeline
- search_documents build/rebuild process
- exact account-number search
- normalized address search
- fuzzy/trigram search
- owner name fallback search
- autocomplete endpoint
- parcel summary endpoint

Requirements:
- optimized for interactive search latency
- use derived search structures rather than direct heavy joins
- return ranked results with confidence signals
- support public-facing parcel lookup needs

Deliver:
- search indexing code
- API endpoints
- tests
- sample response payloads
```

---

## Stage 12 — Build admin ingestion and QA dashboard

### Objective
Make the data backbone operable, not just technically present.

### Build in this stage
- ingestion job dashboard
- validation results page
- source file page
- manual upload flow
- publish controls
- rollback controls
- assignment issue views
- data completeness dashboard

### Must be true before moving on
- an operator can run the system without reading code
- failed jobs are inspectable
- manual fallback works

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 12 objective:
Build the admin UI for ingestion operations, QA, and manual fallbacks.

Build:
- ingestion jobs dashboard
- job detail page
- validation results page
- source files page
- manual upload page
- publish/rollback controls
- parcel data completeness dashboard
- tax assignment issue views

Requirements:
- admin-only access
- clear operational UX
- structured display of counts, warnings, and errors
- support manual workflows when county automation fails

Deliver:
- admin pages
- API endpoints if needed
- role protection
- basic styling sufficient for ops use
```

---

## Stage 13 — Build the public parcel summary MVP

### Objective
Expose the first user-facing value from the backbone.

### Build in this stage
- search page
- parcel result page
- assessed / market value summary
- exemptions summary
- tax-rate breakdown
- owner summary with restricted exposure
- data-quality caveat flags

### Must be true before moving on
- a homeowner can search and view parcel tax data cleanly
- public pages do not expose sensitive internal fields
- the data backbone proves user value

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 13 objective:
Build the public-facing parcel search and parcel summary MVP.

Build:
- public search page
- parcel result page
- parcel summary card
- exemption summary display
- component tax rate breakdown display
- total effective tax rate display
- owner summary with appropriate privacy restrictions
- data caveat/warning display where confidence is limited

Requirements:
- use derived views and public-safe endpoints
- fast and clean UX
- no protest quote logic yet beyond structural placeholders
- prioritize correctness and clarity

Deliver:
- pages
- endpoints
- basic responsive UI
```

---

## Stage 14 — Build protest support foundation

### Objective
Prepare the operational layer for later quote generation and filing workflows.

### Build in this stage
- protest_cases
- evidence_packets
- evidence packet sections
- evidence packet comps
- valuation_runs
- comparable_sales schema
- case_outcomes
- admin case pages

### Must be true before moving on
- protest workflows have storage and admin surfaces
- evidence generation can be layered in later without schema changes
- valuation support exists structurally

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 14 objective:
Build the protest support foundation for Dwellio.

Build:
- protest case CRUD foundation
- evidence packet data structures
- evidence_packet_sections
- evidence_packet_comps
- valuation_runs
- comparable_sales schema
- case_outcomes
- internal admin pages for reviewing cases and packets

Requirements:
- do not implement full legal automation yet
- make the data model complete enough to avoid future schema redesign
- keep residential-only focus in MVP logic

Deliver:
- schema/migrations if needed
- backend code
- basic admin pages
- tests
```

---

## Stage 15 — Build docs, runbooks, and test coverage

### Objective
Lock the platform into something maintainable.

### Build in this stage
- architecture docs
- county adapter docs
- ingestion framework docs
- ops runbook
- rollback instructions
- local dev instructions
- fixture datasets
- integration tests
- smoke tests

### Must be true before moving on
- another developer could run and maintain the system
- critical flows are testable
- operations are documented

### Prompt for Codex
```text
Use the source-of-truth docs as the source of truth.

Stage 15 objective:
Complete documentation, runbooks, fixtures, and test coverage for the Dwellio MVP backbone.

Build:
- architecture docs
- canonical schema docs
- ingestion framework docs
- Harris adapter docs
- Fort Bend adapter docs
- tax assignment docs
- exemption docs
- owner reconciliation docs
- search docs
- ops runbook
- fixture datasets
- integration tests
- smoke tests

Requirements:
- docs must match implemented code
- focus on maintainability and reproducibility
- include step-by-step local run instructions

Deliver:
- docs
- fixtures
- tests
- final implementation summary
```

---

## Cross-cutting audit and reconciliation prompts

Use these when you need to inspect the repo without blindly editing.

### Repository audit prompt
```text
Inspect the entire repository and summarize:

1. system architecture layers
2. current build status for:
   - database
   - models
   - services
   - ETL jobs
   - APIs
   - tests
3. stale or duplicate files
4. architecture inconsistencies
5. the safest next implementation step

Do not modify files yet.
```

### Database migration audit prompt
```text
Focus only on:
- app/db/migrations/
- app/db/views/
- sql/dwellio_full_schema.sql

Tasks:
- validate migration ordering
- validate foreign keys
- confirm indexes
- check read-model compatibility
- detect schema drift against docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md and related source-of-truth docs

Return:
1. mismatches
2. recommended fixes
3. affected files

Do not edit files yet.
```

### Model reconciliation prompt
```text
Compare:
- app/models/
- app/db/migrations/
- app/db/views/

Find:
- missing model fields
- stale fields
- mismatched field names
- API response models that do not match read models

Return recommended changes before editing.
```

### Domain service audit prompt
```text
Review:
- app/services/

Check alignment with:
- docs/architecture/domain-scoring-formulas.md
- docs/architecture/valuation-savings-recommendation-engine.md
- relevant source-of-truth docs

Verify the following services:
- address_resolver
- comp_scoring
- market_model
- equity_model
- decision_tree
- arb_probability
- savings_engine
- explanation_builder
- packet_generator

Return inconsistencies and safe improvement suggestions.
Do not modify code yet.
```

### ETL pipeline audit prompt
```text
Review:
- app/jobs/
- app/county_adapters/

Ensure the ETL pipeline produces the following tables correctly:
- parcel_sales
- neighborhood_stats
- comp_candidates
- valuation_runs
- parcel_savings_estimates
- decision_tree_results

Return:
- job dependencies
- missing steps
- schema mismatches
- exact read/write expectations
```

### API implementation / audit prompt
```text
Focus on:
- app/api/
- app/models/
- app/db/views/
- docs/architecture/api-contracts.md

Implement or validate endpoints:
- GET /search
- GET /quote/{county}/{year}/{account}
- GET /quote/{county}/{year}/{account}/explanation
- POST /lead

Rules:
- public endpoints read from read models
- do not expose restricted MLS data
- maintain parcel-year context

Return mismatches and recommended fixes before large edits.
```

### Testing prompt
```text
Create or reconcile unit tests for:
- address normalization
- comp scoring
- market model
- equity model
- decision tree
- savings engine

Create or reconcile integration tests for:
- search endpoint
- quote endpoint
- lead submission
```

### Documentation sync prompt
```text
Compare documentation with code.

Focus on:
- docs/architecture/
- docs/runbooks/
- docs/codex/

Find:
- outdated terminology
- outdated API routes
- repo structure mismatches
- docs that drifted from actual implementation

Do not change docs/source_of_truth unless explicitly instructed.
```

### Final repository audit
```text
Perform a full repository reconciliation audit.

Check:
- docs/
- app/
- sql/
- tests/

Return:
- architecture mismatches
- duplicate files
- final safe improvements
- remaining blockers
```

---

## Safe build order

Never skip this order unless a blocking dependency makes it impossible:

1. schema and migrations
2. models
3. services
4. ETL jobs
5. read models
6. APIs
7. tests
8. docs sync

This order exists to keep Dwellio stable, parcel-year centric, and auditable.

---

## Database phase execution rules

- apply ordered migrations
- do not execute the convenience full schema file as the migration history
- use `sql/dwellio_full_schema.sql` only as a reference or bootstrap aid
- preserve migration ordering
- keep migrations atomic
- do not redesign the schema casually once county adapters depend on it

---

## ETL phase execution rules

When implementing or reconciling ETL, make sure the following jobs or their equivalent responsibilities are covered:

- `job_fetch_sources.py`
- `job_load_staging.py`
- `job_normalize.py`
- `job_geocode_repair.py`
- `job_sales_ingestion.py`
- `job_features.py`
- `job_comp_candidates.py`
- `job_score_models.py`
- `job_score_savings.py`
- `job_refresh_quote_cache.py`
- `job_packet_refresh.py`

Preserve county adapter separation and preserve parcel-year centric data flow.

---

## Read-model phase execution rules

These are the authoritative public read models unless source-of-truth docs explicitly change them:

- `v_search_read_model`
- `v_quote_read_model`

Do not create a parallel public search or quote model without first explaining why.

---

## API phase execution rules

Canonical public endpoints:

- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

Do not substitute `/quote/refresh` as a public endpoint unless it is intentionally added as an internal/admin route.

---

## Frontend note

React / Next.js may be used as a frontend layer.
Backend/API authority remains Python unless the source-of-truth docs are explicitly changed.

---

## Commit discipline

Commit after each stable milestone, such as:
- migrations reconciled
- models reconciled
- services reconciled
- jobs reconciled
- APIs reconciled
- tests added
- docs synced

Do not bundle unrelated architectural changes into the same milestone.

---

## End-of-stage completion checklist

At the end of every stage, require Codex to return the following in a structured response:

1. files created
2. files modified
3. migrations added or changed
4. commands to run locally
5. what to test
6. what passed
7. what failed
8. known blockers
9. architecture concerns detected
10. safest next step

Do not move to the next stage until the previous stage is clean enough to support it.

---

## Important execution rule

Do not let Codex jump ahead to:
- quote engine finalization
- live valuation engine refinement
- sales comp automation beyond the stage currently authorized
- evidence packet automation beyond the stage currently authorized
- user signup funnels
- advanced commercial-property logic

until the earlier structural stages are sound.

Those later layers depend on the backbone.

---

## Practical operating workflow

1. Start every Codex session with the Session Start Master Prompt.
2. Run a repository audit if you are not sure where the repo currently stands.
3. Work one stage at a time.
4. After each stage, review:
   - files created
   - migrations added
   - commands to run
   - tests to run
   - blockers
5. Only then move to the next stage.
6. Use the cross-cutting audit prompts whenever you suspect drift.
7. Keep this file in the repo as the single consolidated prompt reference.

---

## Recommended file location and usage

This file should live in:

`docs/codex/DWELLIO_MASTER_CODEX_PROMPT.md`

It should be treated as the consolidated operator file for Codex work on Dwellio.
