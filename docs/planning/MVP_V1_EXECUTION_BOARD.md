# Dwellio MVP V1 Master Execution Board

## Purpose

This is the authoritative execution board for the production-ready MVP V1 plan.
It turns the roadmap into a working delivery system for Stages 2-15.

Use this file for:

- ticket sequencing
- priority management
- dependency tracking
- launch-blocker tracking
- cross-stage execution visibility

Do not use this file to redefine product scope or intended architecture.
Use the roadmap and source-of-truth docs for that.

Primary planning inputs:

- [MVP_V1_ROADMAP.md](./MVP_V1_ROADMAP.md)
- [architecture-state.md](../architecture-state.md)
- [COUNTY_ONBOARDING_CONTRACT.md](../runbooks/COUNTY_ONBOARDING_CONTRACT.md)
- [testing-observability-security.md](../architecture/testing-observability-security.md)

## Operating Rules

- Every ticket must have one owner before execution starts.
- Do not mark a ticket `Done` unless its "done when" statement is actually true.
- If a code change affects routes, jobs, migrations, workflow states, or operator behavior, update [architecture-state.md](../architecture-state.md) or explicitly note `no status-doc impact`.
- If the roadmap changes, this board must be reconciled the same day.
- Tickets may run in parallel only if their dependencies are satisfied.

## Priority Legend

- `P0`: launch-critical critical-path work
- `P1`: high-value parallel work that materially reduces launch risk
- `P2`: follow-on or optimization work that should not block earlier critical path

## Status Legend

- `Ready`: can be started now without violating dependencies
- `Blocked`: should not start until listed dependencies are complete
- `Done`: completed and verified

## Current Recommended Starting Tranche

Start here first:

- `S2-T1`
- `S2-T2`
- `S2-T3`
- `S2-T4`
- `S2-T5`
- `S3-T1`
- `S3-T2`
- `S5-T1`

Second wave once the first tranche stabilizes:

- `S3-T3`
- `S3-T4`
- `S4-T1`
- `S4-T2`
- `S5-T2`
- `S5-T3`

## Master Execution Board

### Stage 2 - County data operations hardening

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S2-T1` | Finalize Harris/Fort Bend county-year onboarding checklist | `P0` | `Done` | none | Each county-year has one operator-facing checklist covering raw files, prep, validation, publish, and rollback |
| `S2-T2` | Define the raw -> ready -> register -> ingest workflow contract | `P0` | `Done` | none | A non-engineer can follow the workflow without guessing file state, naming, or step order |
| `S2-T3` | Clarify readiness dashboard status meanings | `P0` | `Done` | none | Every readiness state clearly explains why a county-year is or is not quote-ready |
| `S2-T4` | Document publish / rollback / retry-maintenance rules | `P0` | `Done` | none | Operators know when to publish, roll back, retry, or escalate without developer intervention |
| `S2-T5` | Create schema-drift, malformed-file, and historical-validation SOP | `P1` | `Done` | none | There is a standard response for file-shape changes, malformed inputs, and suspicious year-over-year validation results |

### Stage 3 - Public read-model foundation hardening

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S3-T1` | Lock the canonical public route inventory | `P0` | `Done` | none | There is one approved list of public routes and their intended public-safe purpose |
| `S3-T2` | Run a public payload safety and owner-masking audit | `P0` | `Done` | `S3-T1` | Each public route is checked for debug leakage, restricted data, raw comps, and masking mistakes |
| `S3-T3` | Define unsupported-state and fallback contract | `P0` | `Done` | `S2-T3`, `S3-T1`, `S3-T2` | Unsupported county, unsupported property, missing readiness, and system-failure states are explicit and user-safe |
| `S3-T4` | Define route smoke-test matrix | `P1` | `Done` | `S3-T1`, `S3-T3` | Every public route has a minimum smoke-test or contract-test expectation |
| `S3-T5` | Define frontend-safe payload guarantees | `P1` | `Ready` | `S3-T2`, `S3-T3` | Frontend consumers know which fields are stable, optional, derived, or never public |

### Stage 4 - Instant quote serving maturity

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S4-T1` | Create instant-quote readiness checklist and supportability thresholds | `P0` | `Ready` | `S2-T3`, `S3-T3` | Operators can tell whether a county-year is safe to serve for instant quote and why |
| `S4-T2` | Lock tax-rate basis and basis-status policy | `P0` | `Ready` | `S3-T3` | Same-year vs prior-year basis handling is documented for ops, product, QA, and customer-safe behavior |
| `S4-T3` | Define refresh-run audit and quote-quality monitoring SOP | `P1` | `Blocked` | `S4-T1`, `S4-T2` | Refresh quality, stale data, bad estimates, and supportability drift can be reviewed and escalated consistently |
| `S4-T4` | Finalize public-safe estimate copy set | `P1` | `Blocked` | `S4-T1`, `S4-T2` | Instant quote copy is useful for acquisition without implying final protest analysis or guaranteed outcomes |
| `S4-T5` | Define instant-quote telemetry and suppression reporting requirements | `P1` | `Blocked` | `S4-T1`, `S4-T3` | Product and ops can monitor refresh health, supportability mix, suppressed outcomes, and quote drift |

### Stage 5 - Public quote-to-lead funnel hardening

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S5-T1` | Define all funnel states | `P0` | `Done` | none | Quote-ready, unsupported county, unsupported property, missing quote-ready row, and system/config failure each have a clear state definition |
| `S5-T2` | Create UX copy matrix and CTA rules for each funnel state | `P0` | `Blocked` | `S3-T3`, `S4-T4`, `S5-T1` | Every funnel state has approved user messaging and CTA behavior |
| `S5-T3` | Define duplicate-lead and parcel-year attribution rules | `P0` | `Ready` | `S5-T1` | Repeat submissions, requested tax year, served tax year, county, and parcel context are handled consistently |
| `S5-T4` | Define lead-event auditability and admin lead reporting requirements | `P1` | `Blocked` | `S5-T1`, `S5-T3` | Ops can reconstruct what the user saw and submitted and review lead volume and quality |
| `S5-T5` | Define quote-ready vs unsupported-demand analytics | `P1` | `Blocked` | `S4-T1`, `S5-T1`, `S5-T3` | Reporting separates supported demand from interest in unsupported or unreachable cases |

### Stage 6 - Advanced equity valuation engine

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S6-T1` | Define subject-feature normalization and equity comp candidate rules | `P0` | `Blocked` | `S2-T3` | Subject parcel inputs and initial comp candidate generation rules are explicit for protest use |
| `S6-T2` | Implement comp eligibility, exclusion, ranking, and scoring logic | `P0` | `Blocked` | `S6-T1` | Comp selection is defendable, auditable, and not dependent on hidden reviewer intuition |
| `S6-T3` | Implement adjustment logic and equity value conclusion rules | `P0` | `Blocked` | `S6-T2` | Subject-to-comp adjustments and final equity conclusions are explicit and reviewable |
| `S6-T4` | Implement confidence/supportability scoring and savings-impact logic | `P0` | `Blocked` | `S6-T3` | Internal users can see whether an equity conclusion is strong enough to support downstream protest work |
| `S6-T5` | Implement reviewer override and re-run workflow | `P0` | `Blocked` | `S6-T3`, `S6-T4` | Reviewer overrides are auditable and do not silently replace baseline model output |
| `S6-T6` | Validate the equity engine on representative Harris and Fort Bend sample data | `P0` | `Blocked` | `S6-T4`, `S6-T5` | The engine produces production-credible results on realistic county data, not just fixtures |

### Stage 7 - Internal case workflow expansion

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S7-T1` | Define the represented-customer model and case-state machine | `P0` | `Blocked` | `S2-T3`, `S6-T6` | Lead, represented customer, and protest case are distinct and their allowed transitions are explicit |
| `S7-T2` | Define operator case queues, assignment rules, and escalation views | `P0` | `Blocked` | `S7-T1` | Internal users can find, assign, and escalate cases without DB access |
| `S7-T3` | Define case review statuses, override audit rules, and SLA visibility | `P0` | `Blocked` | `S7-T1` | Case review can distinguish weak, incomplete, do-not-pursue, and escalated states with auditable overrides |
| `S7-T4` | Define lead -> represented customer -> case transition rules | `P0` | `Blocked` | `S5-T3`, `S7-T1` | Downstream agreement work does not need to invent customer/case transitions |
| `S7-T5` | Define hearing hooks, deadline visibility, and parcel-year lineage rules | `P1` | `Blocked` | `S7-T2`, `S7-T3` | Hearing-related workflow hooks and deadline visibility exist without breaking lineage or status history |

### Stage 8 - Evidence packet foundation completion

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S8-T1` | Define packet readiness state machine | `P0` | `Blocked` | `S7-T2`, `S7-T3`, `S6-T6` | Packet readiness is tracked independently from case status with explicit internal-only states |
| `S8-T2` | Define reviewer QA checklist and packet-item editing rules | `P0` | `Blocked` | `S8-T1` | Reviewers have standard rules for packet edits, comp-set edits, and narrative QA |
| `S8-T3` | Define packet completeness rules and lineage audit requirements | `P0` | `Blocked` | `S8-T1` | Packet contents can be tied back to case, valuation run, and reviewer actions |
| `S8-T4` | Define packet queue and detail-view workflow improvements | `P1` | `Blocked` | `S8-T1`, `S8-T2` | Packet reviewers can find, inspect, and advance packet work consistently |

### Stage 9 - Final packet / PDF generator

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S9-T1` | Design packet rendering and template architecture | `P0` | `Blocked` | `S8-T2`, `S8-T3` | There is an approved rendering approach for HTML/PDF artifacts that matches reviewed packet structures |
| `S9-T2` | Implement packet generation service and artifact-state model | `P0` | `Blocked` | `S9-T1` | Real cases can generate non-stub packet artifacts with explicit draft/generated/approved/failed states |
| `S9-T3` | Implement packet versioning, storage, and regeneration audit | `P0` | `Blocked` | `S9-T2` | Packet versions are stored, retrievable, and auditable through regeneration events |
| `S9-T4` | Implement packet preview, approval, and failure-handling workflow | `P0` | `Blocked` | `S9-T2`, `S9-T3` | Reviewers can preview, approve, retry, and diagnose generation failures without manual DB work |

### Stage 10 - Agreements and representation workflow

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S10-T1` | Implement agreement package model and represented-status transitions | `P0` | `Blocked` | `S7-T1`, `S7-T4` | The agreement package and represented-status transitions match the Stage 0 operating model |
| `S10-T2` | Implement e-sign provider flow and webhook handling | `P0` | `Blocked` | `S10-T1` | Agreements can be sent, completed, and reconciled without silent failures |
| `S10-T3` | Implement signed-document storage, versioning, and retrieval | `P0` | `Blocked` | `S10-T1`, `S10-T2` | Signed documents are versioned, retrievable, and attached to the correct represented customer and case |
| `S10-T4` | Implement admin agreement visibility plus resend/failure workflows | `P1` | `Blocked` | `S10-T2`, `S10-T3` | Admin can see current agreement status and recover from send, sign, or webhook failures |
| `S10-T5` | Implement cannot-advance-without-authorization gates | `P0` | `Blocked` | `S10-T3`, `S10-T4` | Leads and cases cannot proceed into represented workflows without completed authorization |

### Stage 11 - Billing and economic operations

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S11-T1` | Codify the MVP billing rules in system behavior | `P0` | `Blocked` | `S10-T1` | The product and admin workflow use the contingency fee formula and invoicing timing approved in Stage 0 |
| `S11-T2` | Implement payment authorization and billing state machine | `P0` | `Blocked` | `S10-T2`, `S11-T1` | Billing status is explicit and does not corrupt case or agreement state |
| `S11-T3` | Implement invoice, receipt, and reconciliation flows | `P0` | `Blocked` | `S11-T2` | Finance/admin can issue invoices, store receipts, and reconcile case outcomes to billing events |
| `S11-T4` | Implement admin billing views plus failure/retry/refund workflows | `P1` | `Blocked` | `S11-T2`, `S11-T3` | Billing failures are visible and recoverable without breaking represented-customer workflows |

### Stage 12 - Filing preparation and deadline-governance workflow

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S12-T1` | Implement filing-readiness checklist and county requirement model | `P0` | `Blocked` | `S9-T4`, `S10-T5` | County-specific prerequisites are visible before any submission attempt |
| `S12-T2` | Implement notice-date, deadline, duplicate, already-filed, and agent-conflict rules | `P0` | `Blocked` | `S7-T5`, `S10-T5` | Filing-prep logic prevents duplicate, conflicted, or late-case handling from being implicit |
| `S12-T3` | Implement filing-attempt logging and confirmation-proof schema | `P0` | `Blocked` | `S12-T1`, `S12-T2` | Filing attempts and proof requirements are recorded before full submission automation starts |
| `S12-T4` | Implement manual filing workflow and operator prep queue | `P1` | `Blocked` | `S12-T1`, `S12-T2` | There is a real manual fallback path for filing prep and operator handling |
| `S12-T5` | Implement filing-ready gating in admin workflow | `P0` | `Blocked` | `S12-T1`, `S12-T2`, `S12-T3` | Admin cannot mark a case filing-ready without satisfying the required prerequisites |

### Stage 13 - County submission and confirmation-proof system

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S13-T1` | Implement Harris submission workflow | `P0` | `Blocked` | `S11-T3`, `S12-T5` | Harris cases can be submitted through the approved workflow with auditable outcomes |
| `S13-T2` | Implement Fort Bend submission workflow | `P0` | `Blocked` | `S11-T3`, `S12-T5` | Fort Bend cases can be submitted through the approved workflow with auditable outcomes |
| `S13-T3` | Implement submission failure queue, retry rules, and duplicate prevention | `P0` | `Blocked` | `S13-T1`, `S13-T2` | Failed submissions are visible, recoverable, and protected against duplicate filing |
| `S13-T4` | Implement confirmation-proof capture, storage, and audit trail | `P0` | `Blocked` | `S12-T3`, `S13-T1`, `S13-T2` | Every submission has traceable proof storage and audit history |

### Stage 14 - Minimum viable customer access layer

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S14-T1` | Implement customer auth/account foundation | `P0` | `Blocked` | `S10-T3` | Represented customers can safely access a customer account without exposing internal-only data |
| `S14-T2` | Implement signed-document access and customer-safe major-status timeline | `P0` | `Blocked` | `S14-T1`, `S10-T3`, `S11-T3` | Customers can view their signed documents and major case states in a safe, accurate way |
| `S14-T3` | Implement major-event notifications and support-escalation surface | `P1` | `Blocked` | `S14-T1`, `S14-T2` | Customer notifications reflect real backend state and provide a clear support path |
| `S14-T4` | Implement submission-visibility mapping for customer-safe states | `P1` | `Blocked` | `S13-T4`, `S14-T2` | Submission confirmations appear only when they are actually available and customer-safe |

### Stage 15 - Controlled live pilot

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S15-T1` | Define pilot cohort, success criteria, and fallback policy | `P0` | `Blocked` | `S13-T4`, `S14-T2` | Pilot scope, success criteria, and fallback rules are approved before live-case execution begins |
| `S15-T2` | Run the live-case checklist across pilot cases | `P0` | `Blocked` | `S15-T1` | Pilot cases complete the real operational checklist across agreements, packeting, filing prep, submission, and support |
| `S15-T3` | Operate pilot issue log and remediation workflow | `P0` | `Blocked` | `S15-T2` | Every pilot failure is logged, triaged, and assigned to remediation or explicit deferral |
| `S15-T4` | Publish pilot closeout and go/no-go recommendation | `P0` | `Blocked` | `S15-T3` | Pilot outcomes are summarized and unresolved risks are explicit before broad launch |

### Stage 16 - Release hardening and launch readiness

| ID | Ticket | Priority | Status | Depends on | Done when |
|---|---|---|---|---|---|
| `S16-T1` | Build the launch checklist and environment-readiness checklist | `P0` | `Blocked` | `S15-T4` | There is one checklist covering environment readiness, launch prerequisites, and remaining safety checks |
| `S16-T2` | Implement the regression, smoke, and contract-test plan | `P0` | `Blocked` | `S15-T4` | Required subsystems are tested at their actual maturity level with no hidden developer-only assumptions |
| `S16-T3` | Complete backup/restore and rollback drill with evidence | `P0` | `Blocked` | `S15-T4` | Backup, restore, and rollback have been rehearsed and evidenced |
| `S16-T4` | Complete alerting, secret/config, audit-log, and RBAC review | `P0` | `Blocked` | `S15-T4` | Alert ownership, secret handling, audit logging, and access controls are reviewed and tightened for launch |
| `S16-T5` | Publish known-limitations register and final go/no-go rubric | `P0` | `Blocked` | `S16-T1`, `S16-T2`, `S16-T3`, `S16-T4` | No customer-facing promise exceeds actual system behavior and the launch decision is explicit |

## Cross-Stage Critical Path

This is the highest-signal dependency spine:

1. `S2-T1` -> `S2-T4`
2. `S3-T1` -> `S3-T3`
3. `S4-T1` -> `S4-T3`
4. `S5-T1` -> `S5-T3`
5. `S6-T1` -> `S6-T6`
6. `S7-T1` -> `S7-T4`
7. `S8-T1` -> `S8-T3`
8. `S9-T1` -> `S9-T4`
9. `S10-T1` -> `S10-T5`
10. `S11-T1` -> `S11-T3`
11. `S12-T1` -> `S12-T5`
12. `S13-T1` -> `S13-T4`
13. `S14-T1` -> `S14-T2`
14. `S15-T1` -> `S15-T4`
15. `S16-T1` -> `S16-T5`

## Launch Blockers

### Code Blockers

| blocker | stage/tickets |
|---|---|
| Advanced equity valuation engine | `S6-T1` to `S6-T6` |
| Final packet generation and artifact approvals | `S9-T1` to `S9-T4` |
| Agreements, e-sign, signed-doc storage, and authorization gates | `S10-T1` to `S10-T5` |
| Billing state machine, invoices, receipts, and reconciliation | `S11-T1` to `S11-T4` |
| Filing-readiness, deadline, duplicate, and conflict logic | `S12-T1` to `S12-T5` |
| Harris and Fort Bend submission workflows plus proof capture | `S13-T1` to `S13-T4` |
| Customer account access, signed-doc visibility, and major status timeline | `S14-T1` to `S14-T4` |
| Launch hardening, regression coverage, rollback evidence, alerting, and RBAC review | `S16-T1` to `S16-T5` |

### Non-Code Blockers

| blocker | owner type |
|---|---|
| Active registration before agent-representation launch | founder / compliance |
| Counsel-reviewed agreement package for `Dwellio Tax LLC` | legal / founder |
| County-specific filing SOPs for Harris and Fort Bend | filing ops / founder |
| Final fee-language review matching the approved contingency formula | legal / finance / founder |
| Support and escalation SOPs for represented customers | customer ops |
| Pilot cohort selection and staffing plan | operations / founder |
| Go/no-go launch signoff process | founder / release owner |

## Notes

- Stage 0 decisions are locked in the roadmap and remain launch-gating constraints.
- Stage 1 governance is largely in place, but the PR docs-impact rule still needs to be enforced operationally.
- This board should be reviewed weekly while Stages 2-5 are active and before opening any Stage 6+ execution work.
