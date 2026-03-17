# Dwellio Codex Execution Sequence

Use this as your run order. Each stage includes:

## Stage 0 — Create the repo foundation and guardrails
### Objective
Set up the project structure, tooling, environment management, migration workflow, and core conventions before any feature work begins.

### Build in this stage
- monorepo or organized repo structure
- package manager setup
- TypeScript setup
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
Use the master spec as the canonical architecture document.

Stage 0 objective:
Create the repo foundation for Dwellio as a production-grade Texas property tax appeal platform.

Build:
- repo structure
- Next.js app
- backend/app API structure
- Python ETL service
- PostgreSQL connection setup
- PostGIS enablement
- migration tooling
- environment variable handling
- linting, formatting, testing setup
- docs scaffolding
- config scaffolding for county adapters

Constraints:
- modular monolith
- TypeScript for app/backend
- Python for ETL
- scalable folder structure
- no feature logic yet
- no fake business logic
- keep all code production-oriented

Deliver:
- repo tree
- all setup files
- starter scripts
- local run instructions
- migration baseline

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
Use the master spec as the source of truth.

Stage 1 objective:
Implement the full canonical database schema and migrations for Dwellio.

Build:
- all core tables defined in the master spec
- indexes
- foreign keys
- enum strategy where appropriate
- PostGIS geometry support
- override tables
- seed data for counties, appraisal districts, exemption types, basic unit types

Requirements:
- SQL-first migrations
- keep schema normalized
- version-safe and tax-year aware
- do not skip lineage, validation, ingestion ops, or GIS tables
- no app screens yet

Deliver:
- migration files in execution order
- seed scripts
- schema documentation comments where useful
- a short test script proving migration success

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
- a dummy dataset can flow raw → staging → canonical
- jobs can succeed/fail cleanly
- validation errors can be stored
- publish versioning exists

### Prompt for Codex
Use the master spec as the source of truth.

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
- do not implement actual Harris/Fort Bend parsing yet beyond placeholders

Deliver:
- ingestion framework code
- adapter base classes/interfaces
- sample dummy adapter
- job runner
- example end-to-end flow with fixture data

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
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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

## Stage 8 — Build the exemption normalization and annual rollup logic
### Objective
Represent Texas exemptions correctly enough to avoid future rework.

### Build in this stage
- exemption type seed set
- exemption normalization logic
- parcel exemption rollup view
- homestead/over65/disabled/freeze flags
- annual exemption summaries
- QA checks for suspicious exemption patterns

### Must be true before moving on
- a parcel can carry multiple exemptions for one year
- exemption outputs are reviewable
- future tax computation logic can rely on the structure

### Prompt for Codex
Use the master spec as the source of truth.

Stage 8 objective:
Build exemption normalization and annual parcel exemption rollups.

Build:
- normalization logic for parcel exemptions
- exemption type master seed data
- parcel_exemption_rollup_view
- summary flags for homestead, over65, disabled, disabled veteran, freeze
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
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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

## Stage 11 — Build search architecture and parcel lookup API
### Objective
Get the highest-value user interaction working: fast parcel search.

### Build in this stage
- address normalization
- search document generation
- trigram/fuzzy matching
- account-number exact match
- owner-name fallback
- autocomplete API
- parcel summary API

### Must be true before moving on
- search is fast and useful
- address lookups work against normalized parcel data
- parcel detail retrieval is stable

### Prompt for Codex
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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

## Stage 13 — Build the public parcel summary MVP
### Objective
Expose the first user-facing value from the backbone.

### Build in this stage
- search page
- parcel result page
- assessed/market value summary
- exemptions summary
- tax-rate breakdown
- owner summary with restricted exposure
- data-quality caveat flags

### Must be true before moving on
- a homeowner can search and view parcel tax data cleanly
- public pages do not expose sensitive internal fields
- the data backbone proves user value

### Prompt for Codex
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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
Use the master spec as the source of truth.

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

## How to use this sequence correctly
Run Codex one stage at a time.

At the end of each stage, require Codex to give you:
- files created
- migrations added
- commands to run
- what to test
- what passed
- what remains blocked

Then move to the next stage only after the prior stage is clean.

## Important execution rule
Do not let Codex jump ahead to:
- quote engine
- valuation engine
- sales comp automation
- evidence packet automation
- user signup funnels

until Stages 0–13 are structurally sound.

Those later layers depend on this backbone.

## Best practical workflow
Keep the master spec in the repo as a permanent reference file, then paste only the relevant stage prompt into Codex for each step.
