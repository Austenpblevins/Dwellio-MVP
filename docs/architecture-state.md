# Dwellio Architecture State

This document records the current implementation state of the Dwellio repository.

It is the single current implementation-status ledger for the repository and the authoritative status ledger for what is implemented, partially implemented, deferred, or superseded in code.
It is not the primary source for intended architecture or product design.

Design authority lives in:
- `docs/source_of_truth/`
- `docs/architecture/`
- canonical schema and migration files

## Document Metadata

- Last verified: `2026-04-20`
- Verified by: `Codex`
- Verified against:
  - latest migration: `0056_stage22_ingestion_step_runs.sql`
  - public routes checked: `yes`
  - admin routes checked: `yes`
  - key tests checked: `tests/unit/test_stage17_instant_quote_migration_contract.py` (executed), `tests/integration/test_public_parcel_flows.py`, `tests/integration/test_stage15_workflow_contracts.py`, `tests/integration/test_stage16_lead_funnel_release_hardening.py`, `tests/unit/test_case_admin_api.py` (reviewed)
- Authority level: `Implementation status only`
- Scope: `Repository reality, not launch readiness`

## Status Legend

- `Implemented`: code exists and is wired into the current repo shape
- `Partial`: meaningful foundation exists, but the subsystem is not complete end-to-end
- `Deferred`: intentionally not implemented yet
- `Superseded`: older approach/doc path retained only for history and should not guide new work

## How To Read This File

- `Implemented` means the capability exists in code.
- `Implemented` does not automatically mean production-ready, live-data complete, or launch-approved.
- Every status claim in this document should point to concrete repo evidence.
- If evidence is missing, the item should be marked `Partial` or `Deferred`.

## Current Implementation State

| subsystem | status | current reality | evidence |
|---|---|---|---|
| Backend/API framework | Implemented | FastAPI app wires public and admin routers into one canonical API surface. | `app/main.py`, `app/api/router.py` |
| Public search | Implemented | Canonical public search and autocomplete routes are wired to public-safe search reads. | `app/api/routes/search.py`, `tests/integration/test_public_parcel_flows.py` |
| Public parcel summary | Implemented | Parcel-year public summary route serves masked, public-safe parcel facts. | `app/api/routes/parcel.py`, `docs/public_parcel_summary_stage13.md` |
| Public quote | Implemented | Read-model-backed quote and explanation routes are part of the canonical public flow. | `app/api/routes/quote.py`, `docs/final_implementation_summary.md` |
| Instant quote service | Partial | Stage 17 adds a separate instant-quote route and cache-backed serving layer without replacing the refined quote path. | `app/api/routes/quote.py`, `app/services/instant_quote.py`, `docs/architecture/instant-quote-service-spec.md`, `tests/unit/test_stage17_instant_quote_migration_contract.py` |
| Lead capture | Implemented | Canonical `POST /lead` persists lead rows plus attribution and parcel-year context. | `app/api/routes/leads.py`, `app/services/lead_capture.py`, `docs/final_implementation_summary.md` |
| Public web funnel | Partial | The web app supports search to parcel to quote-to-lead, but not full customer signup, agreements, or billing. | `apps/web/app/`, `docs/stage16_lead_funnel_frontend_ux.md` |
| County ingestion framework | Implemented | Shared ingestion, import-batch, validation, lineage, publish/rollback, and job orchestration backbone exists. | `app/ingestion/service.py`, `app/jobs/cli.py`, `docs/ingestion_framework.md`, `docs/runbooks/MANUAL_COUNTY_FILE_PREP.md` |
| Harris county support | Partial | Harris adapter, onboarding, and manual-prep path exist, but current workflow still includes fixture/manual-prep realities. | `app/county_adapters/harris/adapter.py`, `docs/harris_adapter.md`, `docs/runbooks/MANUAL_COUNTY_FILE_PREP.md` |
| Fort Bend county support | Partial | Fort Bend adapter, onboarding, and manual-prep path exist, but current workflow still includes fixture/manual-prep realities. | `app/county_adapters/fort_bend/adapter.py`, `docs/fort_bend_adapter.md`, `docs/runbooks/MANUAL_COUNTY_FILE_PREP.md` |
| County-year readiness/admin ops | Implemented | Readiness, onboarding, scalability review, source-file review, validation, publish, rollback, and inspection routes are wired. | `app/api/routes/admin.py`, `app/services/county_onboarding.py`, `docs/runbooks/COUNTY_ONBOARDING_CONTRACT.md`, `docs/runbooks/STAGE24_SCALABILITY_BOTTLENECK_REVIEW.md` |
| Case workflow foundation | Partial | Internal case CRUD, notes, status history, and hearing-linked review exist, but not the full operator workbench. | `app/services/case_ops.py`, `app/api/routes/admin.py`, `docs/stage14_protest_support_foundation.md` |
| Evidence packet foundation | Partial | Internal packet review structures, packet items, and comp sets exist, but not final package generation. | `app/services/case_ops.py`, `app/api/routes/admin.py`, `docs/stage14_protest_support_foundation.md` |
| Evidence PDF generation | Deferred | Packet generation and PDF assembly are not implemented beyond scaffold/stub level. | `app/services/packet_generator.py`, `docs/final_implementation_summary.md` |
| Filing automation | Deferred | No county submission adapter or end-to-end filing workflow is implemented yet. | `docs/final_implementation_summary.md`, absence of submission routes/services in `app/api/routes/` and `app/services/` |
| Agreements/e-sign | Deferred | No implemented agreement packet, e-sign completion flow, or webhook-driven authorization workflow exists. | absence of agreement/esign routes/services, `docs/final_implementation_summary.md` |
| Billing/payments | Deferred | No implemented Stripe/payment workflow exists in the active product path. | absence of billing/stripe routes/services, `docs/final_implementation_summary.md` |
| Customer dashboard | Deferred | No customer case portal, notification center, or signed-document dashboard is implemented. | `apps/web/app/`, `docs/final_implementation_summary.md` |
| Observability/reporting | Partial | Strong internal readiness, telemetry, runbooks, and reporting scripts exist, but centralized monitoring remains follow-on work. | `docs/architecture/testing-observability-security.md`, `infra/scripts/report_quote_quality_monitor.py`, `docs/runbooks/STAGE24_SCALABILITY_BOTTLENECK_REVIEW.md` |

## Public Surface Inventory

Current canonical public routes:
- `GET /healthz`
- `GET /search`
- `GET /search/autocomplete`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}`
- `POST /lead`

Notes:
- public routes must stay read-model/public-safe
- public routes must not expose restricted comps, debug fields, or internal workflow data

## Internal Surface Inventory

Current canonical internal/admin surfaces include:
- county-year readiness
- county onboarding contract
- scalability review
- search inspection
- import-batch inspection
- source-file and validation review
- publish / rollback / retry-maintenance
- case review routes
- packet review routes

Primary evidence:
- `app/api/routes/admin.py`
- admin pages in `apps/web/app/admin/`

## Current Boundaries

The following are intentionally true in the current repo state:

- public APIs are read-model based
- property scope is currently SFR-focused
- county scope is currently Harris and Fort Bend
- internal case/packet structures exist before full filing automation
- packet foundation exists before packet PDF generation
- lead capture exists before full customer onboarding
- some county workflows remain fixture-backed or manual-prep dependent

## Known Gaps

The following are not yet implemented in the repository:

- full compliance/legal operating workflow
- service agreement generation and e-sign completion flow
- billing/payment workflow
- county protest submission automation
- confirmation-proof capture workflow
- customer portal and customer notifications
- final evidence PDF/package generation
- full production case workbench with all planned operator roles

## Drift Risks

This document will drift if:
- routes change without updating the public/internal surface inventory
- a subsystem is marked `Implemented` without code/test evidence
- design intent from other docs is copied here as if it were implemented
- stale milestone summaries are treated as current-reality status

## Update Rules

Update this file whenever a change does any of the following:
- adds, removes, or materially changes a public route
- adds, removes, or materially changes an admin route
- changes current county/property support scope
- moves a subsystem from `Partial` to `Implemented`
- explicitly defers, supersedes, or replaces an existing subsystem
- adds a major new workflow surface, job family, or operator capability

Do not update this file for:
- minor refactors that do not change repo-visible capability
- speculative future design
- roadmap-only ideas not yet evidenced in code

## Verification Checklist

Before marking any item `Implemented`, confirm at least one of:
- route exists and is wired
- service is used by a wired route/job
- migration/schema support exists and is consumed
- test coverage exists for the contract
- runbook/doc accurately reflects the current workflow

If those checks are not met, mark the item `Partial` or `Deferred`.

## Change Log

- `2026-04-20`: rebuilt the file as a repo-reality status ledger with evidence-backed subsystem rows, route inventories, and maintenance rules
