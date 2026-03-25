# Stage 14 Protest Support Foundation

This document records the Stage 14 operational foundation added for protest-case review and evidence-packet support.

## Scope

Included in Stage 14:
- internal protest case CRUD foundation
- internal case review APIs
- internal evidence packet review APIs
- admin pages for reviewing cases and packets
- canonical packet support structures needed for later evidence generation
- valuation and recommendation linkage preserved into case review

Explicitly deferred beyond Stage 14:
- full legal filing automation
- automatic packet generation and PDF assembly
- attorney workflow orchestration
- public exposure of internal evidence structures

## Canonical Structure Mapping

Source-of-truth precedence wins over imported prompt wording.

Stage 14 prompt wording referenced:
- `evidence_packet_sections`
- `evidence_packet_comps`

Canonical schema implemented instead:
- `evidence_packet_items`
- `evidence_comp_sets`
- `evidence_comp_set_items`

This keeps packet structure aligned with `docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md` and avoids introducing a duplicate packet architecture.

## Internal Case Lifecycle

Internal admin endpoints:
- `GET /admin/cases`
- `POST /admin/cases`
- `GET /admin/cases/{protest_case_id}`
- `POST /admin/cases/{protest_case_id}/notes`
- `POST /admin/cases/{protest_case_id}/status`

Lifecycle behavior:
- protest cases remain parcel-year centric
- case creation links `client_id`, `parcel_id`, `tax_year`, and optional `valuation_run_id`
- `client_parcels` is maintained on case creation to preserve downstream client-to-parcel linkage
- status changes append `case_status_history` rows instead of overwriting history
- notes remain internal-only

## Lead and Agreement Boundary

Stage 14 does not replace the existing public lead funnel.

Current boundary:
- public `POST /lead` remains the canonical public lead entry point
- Stage 14 focuses on internal case and packet support after lead/client/agreement context exists
- representation agreements and client linkage remain part of the canonical path into `protest_cases`

## Evidence Packet Support

Internal admin endpoints:
- `GET /admin/packets`
- `POST /admin/packets`
- `GET /admin/packets/{evidence_packet_id}`

Packet support behavior:
- packet headers remain in `evidence_packets`
- narrative/section rows live in `evidence_packet_items`
- comparable support groupings live in `evidence_comp_sets`
- comp membership rows live in `evidence_comp_set_items`
- packet rows may link back to `valuation_runs`
- comp-set rows may link to `parcel_sales` without exposing restricted sales artifacts in public flows

## Admin Review Workflow

Admin pages:
- `/admin/cases`
- `/admin/cases/{caseId}`
- `/admin/packets`
- `/admin/packets/{packetId}`

Review intent:
- case queue shows parcel-year identity, client linkage, workflow status, note/packet/hearing counts, and quote-safe valuation context
- case detail shows notes, status history, assignment rows, hearing rows, and linked packet headers
- packet queue shows packet readiness by parcel-year
- packet detail shows packet items and comp-set rows for manual review

## Valuation and Recommendation Linkage

Stage 14 keeps quote-safe valuation linkage intact by surfacing:
- `valuation_run_id` on protest cases and evidence packets
- recommendation and expected savings context from `v_quote_read_model` in internal case review
- parcel-year identity across quote -> lead/client/agreement -> protest case -> packet support

## Security Boundary

- public APIs remain unchanged
- public quote and search routes remain read-model based
- admin case and packet endpoints are protected by the existing admin-token dependency
- restricted MLS or listing artifacts are not added to public payloads
- packet support rows are internal workflow data only
