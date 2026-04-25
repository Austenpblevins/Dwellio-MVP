# Dwellio MVP V1 - Repo-Aligned Production Launch Roadmap
**Decision-complete roadmap from the current repo state to a launchable MVP V1**

## 1. Purpose

This document replaces the earlier corrected roadmap with a stricter production-launch roadmap.

It answers one practical question:

> **What must Dwellio build next, in what order, from the current repo state, to become a production-ready Texas residential property tax protest business without breaking the architecture already in place?**

This roadmap is intentionally limited to planning.
It does not start implementation.
It does not change repo code, routes, migrations, jobs, or current build stages.

Execution tracking for Stages 2-15 now lives in:

- [MVP_V1_EXECUTION_BOARD.md](./MVP_V1_EXECUTION_BOARD.md)

This roadmap is aligned to the current repository realities:

- Python / FastAPI backend
- Postgres canonical storage and read models
- public-safe, read-model-centric public routes
- separate protected admin surfaces
- Harris and Fort Bend initial county scope
- single-family residential focus
- public quote-to-lead flow is ahead of agreements, billing, filing, and customer account operations

This is not a greenfield roadmap.
It is a launch-oriented roadmap built from what already exists.

---

## 2. Executive conclusion

### The repo is structurally strong

The current Dwellio repo is not off-track.
Its architecture is already stronger than a generic MVP plan in several important areas:

- county onboarding and readiness operations
- public/internal boundary discipline
- instant quote serving architecture
- telemetry and observability foundations
- packet/case internal foundation work
- admin readiness and publish/rollback controls

### The real problem is not architecture

The next challenge is not "how do we architect an MVP from scratch?"
The challenge is now:

> **How do we complete the missing business-critical layers - packet generation, agreements, billing, filing prep, submission/proof, customer access, and launch hardening - without weakening the strong architecture already in place?**

### This roadmap is now explicitly a production-MVP launch roadmap

This document is not just an engineering sequence.
It includes three kinds of stages:

- engineering stages
- business-operating stages
- launch-governance stages

That distinction matters because Dwellio cannot become launch-ready through engineering alone.

---

## 3. Fixed architectural decisions

These decisions should now be treated as locked unless Dwellio intentionally chooses to reopen them.

### 3.1 Backend and data platform

Dwellio is standardized around:

- FastAPI for the API layer
- Postgres for canonical storage and read models
- public-safe read models for customer-facing routes
- protected internal/admin routes for operations, case review, and packet support

### 3.2 Public route discipline

Public routes remain read-model-only and public-safe.
The canonical public route family is:

- `GET /healthz`
- `GET /search`
- `GET /search/autocomplete`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}`
- `POST /lead`

### 3.3 Internal/admin boundary

Internal workflows remain behind protected admin surfaces, including:

- county-year readiness
- county onboarding
- scalability review
- search inspection
- import-batch inspection
- validation/source-file review
- publish/rollback/retry-maintenance
- case review
- packet review

### 3.4 Current county and property boundary

MVP V1 remains bounded to:

- Harris County
- Fort Bend County
- single-family residential scope

No part of this roadmap assumes statewide rollout, commercial support, or multi-property-type expansion in V1.

### 3.5 Product maturity split

The repo currently implies four different maturity levels:

| Product area | Current maturity |
|---|---|
| Public search / parcel / quote | Implemented |
| Public quote-to-lead funnel | Partial but meaningful |
| Internal case / packet foundation | Partial |
| Agreements / billing / filing / customer account portal | Deferred |

### 3.6 Packet ordering rule

Packet foundation and packet generation are separate stages.
They must continue to be treated separately.
Dwellio already has packet support structures before packet artifact generation, and that ordering should be preserved.

---

## 4. Current repo baseline

This section is the plain-English baseline for the roadmap.
Use [architecture-state.md](../architecture-state.md) as the live implementation-status ledger when validating this roadmap.

### 4.1 Already implemented or meaningfully present

#### Public foundation

- public property search
- parcel summary route
- refined quote route and explanation route
- separate instant quote route and serving architecture
- lead capture persistence with attribution and parcel-year context
- parcel-page-based soft-gated quote-to-lead flow

#### County operations

- county-year readiness
- county onboarding contract surfaces
- import batch review
- source-file review
- completeness review
- tax-assignment review
- publish / rollback / retry-maintenance controls
- manual county file prep runbook
- historical validation and telemetry foundations
- scalability bottleneck review

#### Internal workflow foundation

- internal case CRUD foundation
- internal case notes and status history
- internal evidence packet support structures
- internal packet queues and detail views
- valuation linkage into internal review
- admin-token boundary for internal routes

### 4.2 Partial today

These exist, but are not yet full business-ready systems:

| Subsystem | Why it is only partial |
|---|---|
| Instant quote | Separate route and serving layer exist, but this does not replace the full refined quote path |
| Public web funnel | Search -> parcel -> quote-to-lead exists, but not full signup / agreements / payment onboarding |
| Harris county support | Real adapter/onboarding/manual-prep path exists, but still includes fixture/manual-prep realities |
| Fort Bend county support | Real support exists, but still includes manual-prep / local data realities |
| Case workflow | Good internal foundation exists, but not the full operator workbench |
| Evidence packet workflow | Internal packet structure exists, but not final packet/PDF generation |
| Observability/reporting | Strong repo-level telemetry and runbooks exist, but not full centralized production monitoring stack |

### 4.3 Deferred today

These are the most important true build gaps:

- service agreement generation
- e-sign completion flow
- billing / payments
- customer account and customer dashboard
- customer notification center
- final packet/PDF generation
- county protest submission automation or structured operator-assisted submission system
- confirmation-proof capture workflow
- full operator workbench roles and workflows
- full launch-grade legal/compliance operating workflow
- launch-grade RBAC, recovery, and release-hardening layers

---

## 5. What changed from the earlier plan

### Keep

- focus on Harris and Fort Bend first
- SFR-only MVP V1
- fast public quote experience
- editable internal protest-case workflow
- admin workflow and case status tracking
- long-term goal of business-ready automation

### Strip out or rewrite

- backend/search/job-stack alternatives
- generic `/api/v1/...` route inventory that does not match the repo
- language implying full onboarding already exists
- language implying filing automation already exists
- generic evidence table names that do not match the canonical schema
- any wording that blurs internal packet foundation with final packet generation

### Add explicitly

- launch operating model and compliance gate
- manual county file preparation
- county-year onboarding/readiness
- tax-rate adoption truth and basis handling
- publish / rollback control stages
- historical validation
- isolated DB / test environment guardrails
- scalability telemetry review
- represented-customer model before full agreement/e-sign implementation
- deadline and filing-governance work before submission automation
- controlled live pilot before broad launch
- distinction between:
  - public quote-to-lead
  - internal case / packet support
  - later agreements / billing / filing / customer-access work

### Finalized Stage 0 decision memo

This memo locks the planning decisions for Stage 0.
It defines the launch operating model that later stages must honor.
It does **not** authorize launch by itself.
Launch remains blocked until the remaining compliance and artifact dependencies are satisfied.

| Decision area | Approved direction | Planning note |
|---|---|---|
| Contracting party | `Dwellio Tax LLC` | Use the entity name consistently in agreements, billing, and customer-facing materials |
| Representation model | Dwellio represents homeowners as agent | MVP V1 is not analysis-only or DIY |
| Pre-launch compliance path | No agent-representation launch until registration is active | Planning assumption remains conservative even though the founder is a licensed appraiser |
| County and property scope | Harris County + Fort Bend County, SFR-only | Keep this boundary explicit in product copy, agreements, and ops |
| Filing posture | Operator-assisted portal filing, with automation where stable and recoverable | Manual fallback remains allowed |
| Customer promise | "Instant estimate + we handle the protest" | Avoid guaranteed-outcome or guaranteed-savings language |
| Agreement package | Service agreement, agent appointment, e-sign consent, privacy consent, payment authorization | All required before Dwellio files or acts as agent |
| Agent authority duration | Until revoked | Align operating assumptions with Form `50-162` handling |
| Billing model | No upfront fee for MVP | Keep V1 economically simple |
| Fee formula | Fee is based only on the protested year's actual tax-bill reduction after exemptions, caps, and ceilings are applied | No fee on theoretical savings or value reductions that do not reduce the actual tax bill |
| Collection timing | Invoice after the final county outcome is known and savings can be calculated | Do not invoice before actual savings are measurable |
| Customer identity standard | Email required, phone strongly recommended | Good enough for MVP intake, but phone should be encouraged before high-touch support or filing steps |
| Customer-doc model use | Disclosed default with customer consent in the agreement | Make this explicit, not implied |
| Document access | Role-based access | Do not default broad support access to all customer documents |
| Decision owner | Founder / product owner | Final planning decisions stay centralized for MVP speed |

#### Stage 0 policy rules

- No authorization, no filing.
- No active registration, no agent-representation launch.
- Form `50-162` must be in place before Dwellio acts as agent.
- No guaranteed outcome or guaranteed savings language in public-facing copy.
- Every case must have county, tax year, deadline, notice-date status, and representation status confirmed before filing.
- Automation is allowed only where operator review, recovery, and submission proof are available.

#### Remaining Stage 0 launch blockers

- active registration
- counsel-reviewed agreement package for `Dwellio Tax LLC`
- county-specific filing SOPs for Harris and Fort Bend
- final fee-language review so contract text matches the operational billing formula
- implementation of role-based access consistent with the Stage 0 policy

#### Official planning anchors

- [TDLR Property Tax Consultants](https://www.tdlr.texas.gov/ptc/)
- [TDLR Property Tax Consultant FAQ](https://www.tdlr.texas.gov/ptc/ptcfaq.htm)
- [Texas Occupations Code Chapter 1152](https://statutes.capitol.texas.gov/Docs/OC/htm/OC.1152.htm)
- [Form 50-162](https://comptroller.texas.gov/forms/50-162.pdf)
- [Texas Comptroller protest guidance](https://comptroller.texas.gov/taxes/property-tax/protests/index.php)
- [HCAD Online Services](https://hcad.org/hcad-online-services)
- [FBCAD Appeals](https://www.fbcad.org/appeals/)
- [FBCAD knowledge base](https://www.fbcad.org/knowledge-base-2/)

---

## 6. How to read this roadmap

### 6.1 Stage types

Each stage is one of three types:

- **Engineering**: software delivery or technical hardening
- **Business-operating**: legal, operational, customer, or economic workflows needed to run the business safely
- **Launch-governance**: release gating, auditability, pilot validation, or safety controls that decide whether launch is allowed

### 6.2 Dependency-based sequencing

This roadmap is ordered, but not fully serial.
Some stages can run in parallel once prerequisite decisions are locked.

For each stage, the roadmap now states:

- Owner type
- Depends on
- Can run in parallel with
- Launch-critical: yes/no

### 6.3 Roadmap rule

A stage being planned here does **not** mean the repo already implements it.
This document must never imply that work exists just because it is described.

---

## 7. Corrected staged roadmap

# Stage 0 - Launch operating model and compliance gate
**Status:** planning decisions defined; launch gate remains open pending active registration, legal artifacts, and county-specific operating readiness
**Goal:** lock the legal and operating model required to launch safely so later stages build against a fixed business posture instead of assumptions

## Why this stage exists

The repo can continue engineering work without this stage being coded, but the business cannot launch without it.
Earlier plans underweighted the legal and operating model that must exist before representation, billing, filing, and customer-facing promises become real.
The finalized Stage 0 decision memo above is now the planning baseline for this stage.

## Included

- contracting-party decision for `Dwellio Tax LLC`
- representation model and pre-launch registration gate
- county filing posture for Harris and Fort Bend
- portal automation permission policy
- authorization package definition
- contingency billing policy definition
- privacy, model-use consent, and role-based access rules
- document retention, audit, and filing-gate rules
- hard operating rules, including no authorization / no filing

## Excluded

- code implementation of agreements, billing systems, or auth systems
- county submission adapter implementation
- full SOP authoring for every later stage
- launch signoff before active registration and legal review are complete

## Deliverables

- final Stage 0 decision memo embedded in this roadmap
- representation/licensing decision record
- county filing posture for Harris and Fort Bend
- defined agreement package for MVP V1
- defined fee formula and invoicing timing
- privacy/model-use consent rule and role-based-access rule
- launch operating rule set for authorization and filing gates
- remaining Stage 0 blocker list

## Acceptance criteria

- `Dwellio Tax LLC` is the defined contracting party for MVP V1
- Dwellio's representation model is defined as agent representation, but launch remains blocked until registration is active
- county filing posture is defined as operator-assisted portal filing with limited recoverable automation
- the agreement package is defined before agreement/e-sign implementation begins
- the fee formula and invoicing timing are defined before billing implementation begins
- role-based access and model-use consent posture are defined before customer-document workflows expand
- no later roadmap stage assumes filing is permitted without authorization, deadline confirmation, and representation status

## Owner type

- business-operating
- legal/compliance
- product owner

## Depends on

- none

## Can run in parallel with

- Stage 1
- Stage 2
- Stage 3
- Stage 4
- Stage 5

## Launch-critical

- yes

## Business result

Dwellio gets a fixed launch operating posture for MVP V1, and the remaining Stage 0 work becomes explicit launch blockers instead of open planning ambiguity.

---

# Stage 1 - Repo baseline freeze and authority cleanup
**Status:** immediate governance checkpoint
**Goal:** stop roadmap drift and ensure all future planning is anchored to actual repo reality

## Why this stage exists

The repo is advanced enough that stale docs now create real delivery risk.
This stage is intentionally short and governance-focused, not a large product phase.

## Included

- authoritative implementation-status ledger
- authoritative roadmap document
- authoritative route inventory
- authoritative current-scope statement
- PR docs-impact rule
- demotion of stale milestone summaries from live status authority

## Excluded

- application feature work
- schema or route redesign
- broader launch hardening

## Deliverables

- one live implementation-status ledger
- one live repo-aligned roadmap
- one authoritative route inventory reference
- one current-scope statement for counties/property types/public vs internal maturity
- PR docs-impact rule for routes, jobs, migrations, and workflow changes

## Acceptance criteria

- every major subsystem is tagged as implemented, partial, or deferred in the status ledger
- public route inventory matches actual FastAPI routes
- admin route inventory matches actual admin surfaces
- packet foundation and packet/PDF generation are documented as separate stages
- public lead funnel is documented as not being full onboarding
- non-authoritative milestone summaries are clearly marked as historical

## Owner type

- engineering governance
- product/architecture owner

## Depends on

- none

## Can run in parallel with

- Stage 0
- Stage 2
- Stage 3

## Launch-critical

- yes

## Business result

The roadmap becomes safe to hand to Codex, contractors, future hires, or yourself without confusing repo reality.

---

# Stage 2 - County data operations hardening
**Status:** already strong, needs formal hardening
**Goal:** make Harris and Fort Bend county-year support dependable, auditable, and operator-friendly

## Why this stage exists

The repo already has meaningful county onboarding maturity.
This stage turns that from advanced engineering foundation into business-operable county support.

## Included

- manual county file prep workflow
- year-scoped raw/ready file handling
- source-file inspection
- import-batch inspection
- validation review
- completeness review
- tax-assignment review
- county-year onboarding contract
- readiness dashboard clarity
- publish / rollback / retry-maintenance controls
- historical validation support
- known-failure SOPs for malformed source files or schema drift

## Excluded

- statewide county rollout
- unattended multi-county onboarding
- commercial property support
- non-SFR support

## Deliverables

- operator runbook for Harris and Fort Bend year onboarding
- stable year-safe raw -> ready -> register -> ingest workflow
- business-readable readiness dashboard fields
- documented publish criteria per county-year
- documented rollback criteria per county-year
- dataset-family prep checklists
- source schema drift response SOP

## Acceptance criteria

- a non-engineer operator can prepare raw files using the documented contract
- import outputs are tax-year scoped and do not overwrite prior years
- readiness clearly explains why a county-year is not quote-ready
- publish decisions are auditable
- rollback is possible without developer intervention
- data prep, validation, and publish steps are repeatable for both Harris and Fort Bend

## Owner type

- engineering
- operations

## Depends on

- Stage 1

## Can run in parallel with

- Stage 0
- Stage 3
- Stage 4
- Stage 5

## Launch-critical

- yes

## Business result

Dwellio can reliably bring county-year data online instead of relying on ad hoc developer intervention.

---

# Stage 3 - Public read-model foundation hardening
**Status:** implemented foundation, needs production hardening
**Goal:** preserve and harden the public-safe architecture already chosen

## Why this stage exists

This is one of the repo's biggest strengths.
It should be treated as a permanent architectural rule, not a temporary implementation detail.

## Included

- explicit contract documentation for each public route
- public payload safety checklist
- route-level smoke tests
- owner masking verification
- restricted-data leakage policy
- fallback behavior documentation
- public error-state and unsupported-state contract documentation

## Excluded

- rewriting the public route family into a new generic API namespace
- moving public routes off read-model/public-safe surfaces
- exposing admin inspection behavior publicly

## Deliverables

- stable public contract inventory
- route-level smoke test checklist
- owner masking verification checklist
- zero restricted-data leakage policy
- documented fallback and unsupported behavior
- frontend-safe payload contract documentation

## Acceptance criteria

- no public route exposes internal debug fields, raw comps, or restricted listing artifacts
- search, parcel, refined quote, and instant quote remain on public-safe surfaces
- unsupported states are explicit, not silent
- route contracts are stable enough to support frontend and QA automation
- admin inspection remains separate from public flows

## Owner type

- engineering
- architecture

## Depends on

- Stage 1
- Stage 2 for county-year readiness confidence

## Can run in parallel with

- Stage 0
- Stage 4
- Stage 5

## Launch-critical

- yes

## Business result

Dwellio keeps its strongest architectural boundary intact while growing product capability.

---

# Stage 4 - Instant quote serving maturity
**Status:** meaningful partial implementation
**Goal:** make instant quote a versioned, supportability-aware, production-ready acquisition surface without confusing it with the refined quote engine or the downstream protest valuation engine

## Why this stage exists

The instant quote architecture is already much more advanced than a generic precompute design.
It now needs the full V5 modernization layer around contracts, supportability, warnings, rollout controls, and tax-profile semantics before later valuation work starts to depend on ambiguous quote behavior.

## Included

- baseline freeze and versioning for the current instant-quote engine
- typed value-basis and tax-basis metadata for public-safe and internal-safe consumers
- warning taxonomy and internal/public product-state taxonomy
- extension of the existing county onboarding/readiness capability model for instant-quote supportability
- planned `instant_quote_tax_profile` contract
- opportunity-vs-savings separation
- shadow-savings comparison and launch-gate planning
- refresh audit, telemetry, and calibration planning
- explicit supported / unsupported / suppressed behavior spec

## Excluded

- replacing the refined quote path
- treating instant quote as final protest analysis
- using packet rendering as the definition layer for quote logic
- building a full tax simulator for MVP V1
- using broad county-average tax fallback where county-specific uncertainty should remain explicit
- exposing internal tax-rate diagnostics publicly

## Deliverables

- instant-quote baseline/version definition
- typed value/tax metadata contract for quote responses
- warning taxonomy and system-state taxonomy for quote behavior
- county capability extensions for quote supportability and suppression behavior
- `instant_quote_tax_profile` planning contract
- opportunity-vs-savings policy
- shadow-savings rollout and feature-gating plan
- refresh audit, telemetry, and calibration runbook

## Acceptance criteria

- the current instant-quote baseline is frozen and later changes can be compared against it
- same-year vs prior-year basis handling is visible internally and carried through typed contract semantics
- unsupported and suppressed states remain public-safe
- warning states and product/supportability states are explicit and reusable by downstream funnel copy
- county supportability remains derived from one source of truth that extends the onboarding/readiness model instead of duplicating it
- quote outputs distinguish opportunity signals from true savings signals
- quote serving remains fast and deterministic
- instant quote is documented as acquisition-oriented, not final protest analysis
- telemetry and audit planning support monitoring refresh quality, supportability drift, and calibration drift

## Owner type

- engineering
- data/valuation
- product

## Depends on

- Stage 2
- Stage 3

## Can run in parallel with

- Stage 5 design work

## Launch-critical

- yes

## Business result

Dwellio gets a fast estimate surface whose contract, supportability semantics, and rollout controls are explicit enough to support later funnel and valuation work without credibility drift.

---

# Stage 5 - Public quote-to-lead funnel hardening
**Status:** partial
**Goal:** finish the current soft-gated lead funnel as a stable lead-generation system whose copy and CTA behavior are driven by Stage 4 quote states before expanding into full onboarding

## Why this stage exists

The repo already supports a parcel-page-based quote-to-lead flow.
That is meaningful progress, but it is not customer onboarding.

## Included

- funnel-state mapping for quote-ready, unsupported county, unsupported property type, missing quote-ready row, and configuration/reachability failure
- UX copy and CTA behavior for each funnel state
- duplicate lead handling rules
- lead context-status analytics
- parcel-year attribution integrity checks
- lead event auditability
- admin-friendly lead review/reporting

## Excluded

- account creation
- represented-customer creation
- signed representation flows
- payment onboarding

## Deliverables

- funnel-state mapping for all public acquisition outcomes
- UX copy matrix for each state
- duplicate lead policy
- attribution integrity checks
- lead event audit/reporting design
- admin review/reporting requirements for lead demand

## Acceptance criteria

- every lead submission preserves county, parcel, requested tax year, and served tax year context
- unsupported users can still be captured without pretending a quote exists
- funnel messaging reflects Stage 4 warning/product states without redefining quote logic locally
- lead capture never impersonates account creation or signed representation
- analytics can distinguish quote-ready vs unsupported demand
- quote-to-lead remains aligned with canonical backend contracts

## Owner type

- engineering
- product/growth

## Depends on

- Stage 3
- Stage 4

## Can run in parallel with

- Stage 2
- Stage 9 design work

## Launch-critical

- yes

## Business result

Dwellio gets a stable acquisition funnel that honestly matches what the product can currently deliver.

---

# Off-Board Tranche OB1 - Lead reporting and ops visibility
**Status:** intentionally reprioritized
**Goal:** turn the completed Stage 5 lead contracts into a protected internal reporting surface while Stage 6 remains blocked

## Why this tranche exists

Stage 6 remains the next critical-path build item, but it is intentionally not being opened yet.

Rather than drifting into downstream product workflow or pretending valuation is ready, Dwellio can safely pull forward a narrow internal tranche that:

- improves operator visibility into current lead demand
- uses only Stage 5 lead evidence already persisted in the repo
- stays behind protected admin/internal surfaces
- does not redefine quote supportability, represented-customer workflow, or protest valuation logic

This is an explicit off-board reprioritization, not a replacement for Stage 6.

## Included

- protected admin lead reporting surface
- reporting queries for quote-ready vs unsupported demand
- duplicate-demand review support
- lead-event drill-down from reporting aggregates into raw submission evidence
- operator-facing visibility into fallback demand and lead quality mix

## Excluded

- public analytics widgets
- customer-facing reporting
- represented-customer or case workflow
- packet, agreement, billing, filing, or customer-account behavior
- advanced equity valuation logic

## Deliverables

- admin reporting surface definition for current lead demand
- reporting query/service layer scoped to `leads` and `lead_events`
- protected admin/API or page implementation for lead review
- smoke or integration validation for the reporting path
- execution-board reconciliation showing this as an intentional off-board tranche

## Acceptance criteria

- operators can review quote-ready, unsupported county, unsupported property, and reachable-but-unquoted demand without DB access
- reporting preserves the distinction between `requested_tax_year` and `served_tax_year`
- operators can drill from aggregates into raw `lead_submitted` evidence
- duplicate-demand review uses the canonical Stage 5 grouping rules
- no public route gains admin reporting behavior
- no Stage 6 valuation behavior is stubbed or implied by this tranche

## Owner type

- engineering
- operations

## Depends on

- Stage 5

## Can run in parallel with

- Stage 6 remaining blocked

## Launch-critical

- no, but high leverage for current operator usability

## Business result

Dwellio gets an internal reporting surface that makes the existing lead funnel operationally useful without skipping ahead into valuation-dependent workflow.

---

# Stage 6 - Advanced equity valuation engine
**Status:** major missing build item
**Goal:** build the defendable protest-analysis engine that produces the core equity value conclusions Dwellio will rely on for case review, packet content, and filing decisions

## Why this stage exists

This is arguably the most important missing build item in the roadmap.
The protest is based on the equity valuation analysis, not on packet rendering alone.
If the advanced equity engine is not explicit, the roadmap overstates downstream stages and understates the actual core logic the business depends on.
Packet generation is downstream of valuation, not a substitute for it.

## Included

- subject parcel feature normalization for protest use
- equity comp candidate generation
- comp eligibility and exclusion rules
- comp ranking and scoring logic
- adjustment logic for subject-to-comp differences
- equity value conclusion logic
- confidence and supportability scoring
- savings-impact logic tied to equity conclusions
- reviewer override and re-run interaction model
- packet-ready valuation explanation outputs
- Harris and Fort Bend validation on representative real or production-shaped sample data

## Excluded

- final packet artifact rendering
- customer-facing portal work
- statewide modeling expansion
- pretending instant quote is the final protest engine
- using packet generation, packet copy, or quote copy as the source of valuation truth

## Deliverables

- equity comp selection rules
- comp ranking and scoring model
- adjustment-engine design and implementation plan
- equity value conclusion logic
- confidence/supportability model
- reviewer override workflow definition
- packet-ready explanation output contract
- validation set and evaluation approach for Harris and Fort Bend

## Acceptance criteria

- Dwellio can generate a defendable equity analysis for representative Harris and Fort Bend sample cases
- comp selection, adjustments, and value conclusions are explainable and auditable
- supportability/confidence is visible internally
- reviewer overrides are explicit and do not silently mutate baseline model output
- downstream case and packet workflows can consume the valuation output without inventing new logic
- the stage is explicitly documented as the first protest-grade valuation layer, not an extension of instant quote or packet rendering
- the stage is validated on realistic data, not just fixtures

## Owner type

- engineering
- data/valuation
- reviewer operations

## Depends on

- Stage 2
- Stage 4

## Can run in parallel with

- Stage 5
- Stage 7 design work

## Launch-critical

- yes

## Business result

Dwellio gets the actual protest-analysis engine that the rest of the internal workflow and final packet are built on, with packet generation consuming valuation outputs instead of defining them.

---

# Stage 7 - Internal case workflow expansion
**Status:** partial foundation already exists
**Goal:** turn the current protest-case foundation into a usable internal operator workflow and define the represented-customer model around the new valuation engine before agreement implementation

## Why this stage exists

The repo already has case CRUD, notes, status history, valuation linkage, and internal review surfaces.
This stage expands that into a true operational workflow and prevents later agreement work from having to invent core workflow entities on the fly.

## Included

- richer case queue behavior
- case assignment flows
- case review statuses
- manual value override workflows
- note discipline
- hearing-related workflow hooks
- case-level SLA and deadline visibility
- parcel-year lineage preservation
- explicit distinction between lead, represented customer, and protest case
- pre-agreement representation-status dependency notes

## Excluded

- e-sign implementation
- final agreement packet generation
- customer portal work
- submission automation

## Deliverables

- operator case-state machine
- case assignment rules
- manual-review rules for weak, odd, or incomplete cases
- queue filters and escalation views
- override audit policy
- represented-customer model definition
- lead-to-represented-customer-to-case transition rules
- dependency notes for later agreement gating

## Acceptance criteria

- internal users can find, assign, review, update, and advance cases without DB access
- status history remains append-only where required
- parcel-year and valuation linkage remains intact
- case review can distinguish "not enough evidence yet" from "do not pursue"
- internal notes remain internal-only
- workflow clearly distinguishes lead, represented customer, and protest case
- later agreement implementation does not need to invent the case model

## Owner type

- engineering
- operations
- product/workflow

## Depends on

- Stage 1
- Stage 2

## Can run in parallel with

- Stage 4
- Stage 5
- Stage 7
- Stage 9 design work

## Launch-critical

- yes

## Business result

Dwellio can operate real internal case work instead of only proving the schema foundation exists.

---

# Stage 7 - Evidence packet foundation completion
**Status:** partial foundation exists
**Goal:** complete the packet review layer without pretending final packet generation already exists

## Why this stage exists

The repo already has packet foundation work, which is valuable.
This stage finishes the internal preparation workflow before packet artifact generation is built.

## Included

- packet queue improvements
- packet detail review improvements
- narrative/item editing flows
- comp-set review workflows
- packet readiness statuses
- reviewer QA rules
- packet lineage to valuation run and case

## Excluded

- final packet PDF assembly
- county-formatted final document generation
- automated filing package upload bundles

## Deliverables

- packet readiness state machine
- packet QA checklist
- reviewer editing rules for packet items and comp sets
- packet completeness rules
- packet-to-case linkage and audit logging improvements

## Acceptance criteria

- internal users can inspect and review packet structures consistently
- packet structures remain aligned to canonical schema names
- packet support remains internal-only
- packet readiness can be tracked independently from case status
- packet review can proceed even before final PDF generation exists

## Owner type

- engineering
- reviewer operations

## Depends on

- Stage 6

## Can run in parallel with

- Stage 8 design work
- Stage 9
- Stage 11 design work

## Launch-critical

- yes

## Business result

Dwellio gets a real internal evidence-preparation workflow without overstating current automation.

---

# Stage 8 - Final packet / PDF generator
**Status:** deferred build
**Goal:** build the first true business-usable generated evidence package

## Why this stage exists

The current packet generator is still stubbed.
This is one of the most important engineering gaps between internal support foundation and a business-usable protest operation.

## Included

- final packet assembly service
- rendered HTML/PDF output
- packet versioning
- output storage
- packet regeneration rules
- packet approval workflow
- export-safe bundle generation for later filing workflows

## Excluded

- full county submission automation
- attorney workflow orchestration
- statewide template expansion

## Deliverables

- packet generation service implementation plan
- packet rendering template system design
- output versioning model
- packet generation admin controls
- storage and retrieval rules
- packet preview and approval step
- packet regeneration audit trail

## Acceptance criteria

- a real case can produce a non-stub packet artifact
- generated packet content matches reviewed packet structures
- packet versions are stored and auditable
- packet generation failures are surfaced clearly
- reviewers can approve a packet version for downstream use
- packet artifact state is explicit: draft, generated, approved, superseded, failed
- output is compatible with downstream filing-prep workflows

## Owner type

- engineering
- reviewer operations

## Depends on

- Stage 7

## Can run in parallel with

- Stage 9
- Stage 10 design work
- Stage 11 design work

## Launch-critical

- yes

## Business result

This is the stage where Dwellio moves from "packet foundation exists" to "we can generate a real evidence package."

---

# Stage 9 - Agreements and representation workflow
**Status:** deferred build
**Goal:** create the actual legal/customer authorization layer required before a real protest-operation launch

## Why this stage exists

The current lead funnel is not full onboarding.
Before Dwellio can represent customers at scale, it needs a complete representation and authorization workflow, not just document handling.

## Included

- service agreement generation
- representation/authorization workflow
- represented-status transitions
- e-sign completion handling
- resend/failure handling
- document versioning
- signed document storage
- agreement status visibility in admin
- gating rules between lead, represented customer, and case progression

## Excluded

- billing implementation
- customer portal expansion beyond minimum representation visibility
- filing automation

## Deliverables

- agreement packet generation rules
- representation status model
- represented-status transition rules
- e-sign webhook processing design
- signed document storage model
- admin agreement status views
- cannot-advance-without-authorization gate rules
- agreement resend and failure workflows

## Acceptance criteria

- a lead cannot be treated as a represented customer without completed authorization
- signed documents are versioned and retrievable
- admin users can see representation status clearly
- agreement failures do not silently block downstream work
- case workflow can distinguish unsigned vs authorized customers
- represented-status transitions are explicit and auditable

## Owner type

- engineering
- legal/operations
- product

## Depends on

- Stage 0
- Stage 6

## Can run in parallel with

- Stage 8
- Stage 10
- Stage 11

## Launch-critical

- yes

## Business result

Dwellio gains the customer-authorization layer required for an actual operating protest company.

---

# Stage 10 - Billing and economic operations
**Status:** deferred build
**Goal:** build the actual billing layer after agreements, with the MVP billing model explicitly chosen before implementation is finalized

## Why this stage exists

Billing is not present in the current repo and should not be mixed into the current soft-gated lead funnel prematurely.
The fee model also affects agreement terms, customer messaging, receipts, finance operations, and later customer access.

## Included

- billing model selection
- payment profile creation
- invoice / receipt workflow
- billing status in admin
- failure / retry / refund policy support
- linkage between case status and billing status
- reconciliation exports

## Excluded

- anonymous lead billing
- pretending billing exists before agreement and represented-customer state
- advanced finance/reporting beyond MVP operational needs

## Deliverables

- documented MVP billing model decision
- billing state machine
- billing integration design
- payment authorization workflow
- invoice and receipt model
- admin billing views
- failed payment workflows
- reconciliation export requirements

## Acceptance criteria

- the MVP billing model is selected before implementation details are finalized
- billing status is visible per represented customer/case
- billing failures do not corrupt case workflow state
- receipts and audit history are retained
- finance/admin can reconcile billing against cases
- billing is not triggered from anonymous lead capture

## Owner type

- engineering
- finance/operations
- product

## Depends on

- Stage 9

## Can run in parallel with

- Stage 11
- Stage 13 minimum customer-access planning

## Launch-critical

- yes

## Business result

Dwellio becomes economically operable, not just technically impressive.

---

# Stage 11 - Filing preparation and deadline-governance workflow
**Status:** deferred build
**Goal:** build deadline-aware filing preparation before building full submission automation

## Why this stage exists

The earlier generic plan jumped too quickly to automated county submission.
This roadmap treats filing preparation, deadline rules, already-filed checks, and county requirement visibility as a separate launch-critical workflow.

## Included

- filing-readiness checklist
- county-specific submission-prep workflow
- required artifact checklist per county
- manual filing support workflow
- filing-attempt logging model
- confirmation-proof schema requirements
- deadline priority queue
- notice-date handling
- county default deadline policy
- already-filed checks
- duplicate-risk checks
- agent-conflict checks
- filing-readiness gating

## Excluded

- end-to-end automated submission adapters
- statewide deadline rule expansion
- customer-facing filing portal work

## Deliverables

- filing-readiness checklist
- deadline and notice-date policy
- county-specific submission-prep workflow
- manual filing support workflow
- filing-attempt logging model
- confirmation-proof schema
- duplicate-risk and already-filed rules
- county-specific missing-requirement visibility design

## Acceptance criteria

- Dwellio can prepare a case for filing without assuming full automation exists
- filing-ready status is visible in admin
- county-specific missing requirements are visible before submission attempt
- manual filing path is documented and operationally real
- confirmation-proof expectations exist before submission build starts
- notice-date and default deadline handling are explicit
- agent-conflict and duplicate-risk states are visible before filing

## Owner type

- operations
- engineering
- legal/process owner

## Depends on

- Stage 6
- Stage 8
- Stage 9

## Can run in parallel with

- Stage 10
- Stage 12 design work

## Launch-critical

- yes

## Business result

This stage removes the false assumption that filing automation is the next obvious build step.

---

# Stage 12 - County submission and confirmation-proof system
**Status:** deferred build
**Goal:** add county submission workflow only after filing prep, agreement, packet generation, and admin readiness are real

## Why this stage exists

This is the correct place for county filing automation or semi-automation.
Not earlier.
The workflow must support both county-specific automation and operator-assisted fallback.

## Included

- county-specific submission adapters or operator-assisted workflows
- submission-attempt records
- confirmation-proof storage
- retry/failure handling
- duplicate-submission prevention
- county-specific method support
- auditable submission workflows

## Excluded

- pretending a public bulk API exists if one does not
- set-and-forget automation without human fallback
- statewide rollout

## Deliverables

- Harris submission workflow
- Fort Bend submission workflow
- submission failure queue
- retry rules
- confirmation-proof capture workflow
- submission audit logs
- operator fallback path

## Acceptance criteria

- no represented case is submitted without required prerequisites
- failed submission attempts are visible and recoverable
- confirmation proof is stored per submission
- duplicate submission risk is controlled
- county-specific workflows are explicit and auditable
- operator-assisted fallback exists wherever full automation is unsafe or unavailable

## Owner type

- engineering
- filing operations

## Depends on

- Stage 9
- Stage 10
- Stage 11

## Can run in parallel with

- Stage 13 minimum customer-access planning
- Stage 15 hardening design work

## Launch-critical

- yes

## Business result

This is the first stage where Dwellio can credibly say it has a real filing system.

---

# Stage 13 - Minimum viable customer access layer, then broader portal/notifications
**Status:** deferred build
**Goal:** build the minimum customer-facing post-signup experience needed for launch without overbuilding a portal before internal workflows are real

## Why this stage exists

The repo currently has lead capture, not customer account operations.
A launchable MVP likely needs a minimum customer access layer before it needs a rich portal.

## Included

- customer account creation/auth
- signed document access
- major status visibility
- submission confirmation visibility when available
- customer-safe notification boundaries
- support contact/escalation surface
- later expansion path for richer portal and notification features

## Excluded

- exposing internal packet/comps/reviewer notes by default
- broad customer self-service that backend workflows cannot yet support
- pretending the customer portal is richer than the internal operating workflow

## Deliverables

- minimum customer auth/account design
- signed document access controls
- customer-safe major status timeline definition
- notification framework requirements for major events only
- support/escalation surface requirements
- later-stage portal expansion notes

## Acceptance criteria

- customers see only customer-safe data
- signed documents and major case-status changes are visible to the customer
- customer notifications match actual backend workflow state
- internal packet/comps/reviewer notes remain protected unless intentionally exposed later
- richer customer self-service remains out of scope unless backend workflow supports it

## Owner type

- engineering
- product/customer operations

## Depends on

- Stage 9
- Stage 10
- Stage 12 for submission visibility

## Can run in parallel with

- Stage 15 design work

## Launch-critical

- yes

## Business result

Dwellio becomes a business customers can interact with after signup, not just a lead funnel plus an internal admin system.

---

# Stage 14 - Controlled live pilot
**Status:** launch-validation stage
**Goal:** validate the real operating workflow on a controlled number of live represented cases before broad release

## Why this stage exists

A production MVP should not jump directly from build completion to broad launch.
Dwellio needs a controlled pilot that proves the real workflow works across agreements, packet generation, filing prep, submission/proof capture, and support handling.

## Included

- limited live case cohort
- Harris and Fort Bend pilot selection
- manual fallback allowance
- end-to-end operational rehearsal
- support handling validation
- pilot issue logging and exit review

## Excluded

- broad public launch
- statewide expansion
- marketing-scale demand generation

## Deliverables

- pilot cohort definition
- pilot success criteria
- live-case checklist
- fallback policy for pilot cases
- issue log and remediation review
- pilot closeout report

## Acceptance criteria

- represented customers complete the agreement flow successfully
- real cases can produce approved packet artifacts
- filing preparation can be completed on real cases
- submission/proof capture works with automation or manual fallback as designed
- support/escalation handling works for pilot customers
- unresolved pilot failures are tracked before broad launch

## Owner type

- operations
- engineering
- customer support
- product owner

## Depends on

- Stage 9
- Stage 10
- Stage 11
- Stage 12
- Stage 13

## Can run in parallel with

- Stage 15 final hardening activities that do not invalidate pilot behavior

## Launch-critical

- yes

## Business result

Dwellio validates the real business workflow before making broad customer-facing promises.

---

# Stage 15 - Release hardening and launch readiness
**Status:** final cross-cutting launch stage
**Goal:** make the whole system safe enough for real customers, real counties, and real operational use

## Why this stage exists

This stage decides whether Dwellio is only well-designed or actually launchable.
It should occur after the core operating workflow exists and after a controlled live pilot has exposed real weaknesses.

## Included

- route contract tests
- packet-generation tests
- agreement/billing tests
- submission workflow tests
- regression coverage
- quote-quality monitoring
- readiness telemetry
- failure alerting
- operator SOPs
- filing-prep SOPs
- manual fallback SOPs
- customer support SOPs
- billing reconciliation SOPs
- isolated database usage rules
- release-environment config verification
- secret handling
- admin token discipline and future RBAC/user-management expectations
- backup/restore validation
- audit-log review
- rollback drill
- known-limitations register
- go/no-go launch rubric

## Excluded

- new product-scope expansion
- statewide rollout
- nice-to-have portal polish beyond MVP needs

## Deliverables

- launch checklist
- environment readiness checklist
- operator runbooks
- release rollback plan
- smoke-test list
- known-limitations register
- RBAC/user-management hardening requirements
- backup/restore validation evidence
- audit-log and secret/config review checklist
- go/no-go launch rubric

## Acceptance criteria

- required subsystems are tested at their actual maturity level
- no developer-only environment assumptions remain hidden
- isolated DB constraints are documented for operators and developers
- backup/restore validation is completed
- alert-routing ownership is defined
- rollback procedure is rehearsed
- known limitations are documented
- no customer-facing promise exceeds what the system can actually do

## Owner type

- engineering
- operations
- security/release owner
- product owner

## Depends on

- Stage 14

## Can run in parallel with

- none that materially change launch scope

## Launch-critical

- yes

## Business result

This stage turns a strong repo into a launch-ready MVP V1 with explicit safety boundaries.

---

## 8. Parallel work tracks

The roadmap is ordered, but these work tracks can overlap once dependencies are satisfied.

### Track A - County/data/read-model/quote hardening

Primary stages:

- Stage 1
- Stage 2
- Stage 3
- Stage 4
- Stage 5

Purpose:

- protect public architecture
- stabilize county-year operations
- ensure quote and lead acquisition remain truthful and auditable

### Track B - Internal operating workflow

Primary stages:

- Stage 6
- Stage 7
- Stage 8
- Stage 11
- Stage 12

Purpose:

- turn internal foundations into a real reviewer/operator workflow
- create packet artifacts
- prepare and execute filing safely

### Track C - Business-operating and customer-facing launch layers

Primary stages:

- Stage 0
- Stage 9
- Stage 10
- Stage 13
- Stage 14
- Stage 15

Purpose:

- make the business legally operable
- make the economics real
- give customers minimum safe access
- validate launch safety before broad release

### Recommended concurrency window

Once Stage 1 is complete and Stage 0 is defined enough to remove ambiguity:

- Stage 2, Stage 3, and Stage 4 can overlap
- Stage 5 can proceed once Stage 3 and Stage 4 are stable enough
- Stage 6 can proceed once Stage 4 contract boundaries are stable enough to keep quote logic and protest valuation distinct
- Stage 7 can proceed once Stage 6 output shape is stable while Stage 10 design is being finalized
- Stage 8, Stage 9, and Stage 12 can overlap after the Stage 6/7 model is stable
- Stage 13 and Stage 14 should start only after earlier operating dependencies are clear

---

## 9. Launch gates

### Gate 1 - Governance and scope truth locked

Required:

- Stage 0 and Stage 1 complete
- live implementation-status ledger exists
- launch scope is explicit
- no stale doc overrides current reality

### Gate 2 - County-year operations are repeatable

Required:

- Stage 2 complete
- Harris and Fort Bend onboarding is repeatable and auditable
- publish and rollback criteria are documented

### Gate 3 - Public acquisition layer is truthful and stable

Required:

- Stage 3, Stage 4, and Stage 5 complete
- public route safety is verified
- instant quote behavior is versioned, supportability-aware, and business-safe
- quote-to-lead flow does not overpromise onboarding or representation

### Gate 4 - Internal operating workflow is real

Required:

- Stage 6, Stage 7, and Stage 8 complete
- internal users can run case review and packet generation without DB access
- packet artifacts are versioned, reviewable, and failure-aware

### Gate 5 - Representation, billing, and filing governance are ready

Required:

- Stage 9, Stage 10, and Stage 11 complete
- represented-customer state is explicit
- billing model is selected and implemented
- filing-readiness and deadline governance are visible and auditable

### Gate 6 - Submission and customer access are operational

Required:

- Stage 12 and Stage 13 complete
- submissions are auditable and recoverable
- confirmation proof is stored
- customer access shows only safe, real workflow state

### Gate 7 - Pilot and hardening are passed

Required:

- Stage 14 and Stage 15 complete
- controlled live pilot succeeds
- rollback, recovery, alerting, and known limitations are documented
- go/no-go launch rubric is satisfied

---

## 10. Major launch blockers

The highest-value unresolved launch blockers remain:

1. final packet/PDF generation
2. agreements and representation workflow
3. billing and economic operations
4. deadline and filing-governance workflow
5. county submission and confirmation-proof system
6. minimum viable customer access layer
7. release hardening, recovery, and launch governance
8. controlled live pilot execution

If these are not completed, Dwellio may be an advanced repo, but it is not yet a full business-ready protest platform.

---

## 11. Exact recommended build order from today

This is the corrected order from the current repo state:

1. Stage 0 - Launch operating model and compliance gate
2. Stage 1 - Repo baseline freeze and authority cleanup
3. Stage 2 - County data operations hardening
4. Stage 3 - Public read-model foundation hardening
5. Stage 4 - Instant quote serving maturity
6. Stage 5 - Public quote-to-lead funnel hardening
7. Stage 6 - Internal case workflow expansion
8. Stage 7 - Evidence packet foundation completion
9. Stage 8 - Final packet / PDF generator
10. Stage 9 - Agreements and representation workflow
11. Stage 10 - Billing and economic operations
12. Stage 11 - Filing preparation and deadline-governance workflow
13. Stage 12 - County submission and confirmation-proof system
14. Stage 13 - Minimum viable customer access layer, then broader portal/notifications
15. Stage 14 - Controlled live pilot
16. Stage 15 - Release hardening and launch readiness

Use the parallel work tracks to compress schedule, but do not violate the dependency rules.

---

## 12. What not to do next

Avoid these mistakes:

- do not reopen backend stack debates unless there is a true reason
- do not rewrite current public routes into a new generic `/api/v1/...` family without a real migration plan
- do not describe lead capture as full onboarding
- do not describe packet foundation as completed packet generation
- do not assume county filing automation is already part of the current MVP
- do not let stale milestone docs override actual route/service reality
- do not jump to rich customer portal work before agreements, billing, packet generation, filing readiness, and customer-safe minimum access exist
- do not promise customer-facing functionality that only exists internally
- do not treat pilot validation and launch governance as optional afterthoughts

### Non-goals for MVP V1

The following are intentionally out of scope for this roadmap:

- statewide rollout beyond Harris and Fort Bend
- commercial property support
- non-SFR support
- fully unattended county onboarding for arbitrary new counties
- broad customer self-service beyond minimum safe access
- speculative architecture rewrites that do not solve a current launch blocker

### Current strongest repo assets to preserve

The following are competitive strengths and should be treated as assets, not incidental implementation details:

- county readiness/admin ops
- public/internal boundary discipline
- instant quote serving architecture
- manual county prep runbook
- telemetry and scalability review posture
- packet foundation before generator
- read-model-only public route discipline

---

## 13. Final summary

The corrected truth is simple:

**Dwellio is no longer a blank-slate MVP.**
It is already a structured FastAPI/Postgres/read-model platform with real county operations, real public quote/lead surfaces, and real internal case/packet foundations.

That is good news.

The remaining work is not about reinventing the stack.
It is about completing the missing launch-critical layers - packet generation, agreements, billing, filing prep, submission/proof, customer access, pilot validation, and release hardening - without weakening the strong architecture already in place.

This roadmap is the planning document for doing exactly that.
