# Admin Lead Reporting Surface

This document closes `OB1-T1` for the current repo shape.

It defines the approved protected admin surface for lead reporting and operator drill-down while Stage 6 remains blocked.

This is a surface-definition contract only.
It does not implement the reporting queries, API routes, or admin pages yet.

## Purpose

The lead funnel now has canonical Stage 5 rules for:

- funnel state mapping
- duplicate-demand grouping
- lead-event auditability
- quote-ready vs unsupported-demand analytics

`OB1-T1` turns those contracts into one approved internal reporting surface so `OB1-T2` and later implementation work can proceed without inventing a second admin workflow.

## Boundary rules

- The reporting surface must remain behind protected admin/internal access.
- The reporting surface must consume canonical Stage 5 lead evidence from `leads` and `lead_events`.
- The reporting surface must not redefine quote supportability, fallback semantics, or demand buckets locally.
- The reporting surface must not create represented-customer, case, packet, agreement, billing, filing, or customer-account behavior.
- Stage 6 advanced equity valuation remains blocked and must not be implied through this surface.

## Canonical surface shape

The approved surface follows the current repo admin pattern:

- internal API routes under `/admin/...`
- protected admin pages under `/admin/...`
- token-protected backend access using the existing admin access model

Approved planned surface:

- page: `/admin/leads`
- optional detail page or equivalent drill-down target: `/admin/leads/{leadId}`
- list/reporting API: `GET /admin/leads`
- lead detail API: `GET /admin/leads/{lead_id}`

This keeps the lead-reporting surface aligned with the existing internal route family used for:

- `/admin/readiness`
- `/admin/cases`
- `/admin/packets`
- `/admin/ops/...`

## Primary operator jobs this surface must support

The surface must let operators do four things without direct DB access:

1. Review lead volume and demand mix.
2. Separate quote-ready demand from unsupported or reachable-but-unquoted demand.
3. Inspect duplicate-demand groups without deleting or silently merging history.
4. Drill from an aggregate or row into the raw `lead_submitted` evidence that explains what was submitted and what quote/funnel state existed at that time.

## Required top-level page sections

The `/admin/leads` surface must include these sections or equivalent affordances.

### 1. KPI summary

At minimum, show:

- total accepted leads for the current filter window
- `quote_ready_demand`
- `reachable_unquoted_demand`
- `unsupported_county_demand`
- `unsupported_property_demand`
- fallback-applied lead count
- probable duplicate-group count

### 2. Demand-mix breakdown

Operators must be able to review demand mix grouped by:

- primary demand bucket
- county
- requested tax year
- source channel

### 3. Lead results table

The main table must include at least:

- submission timestamp
- `lead_id`
- county
- account number
- `requested_tax_year`
- `served_tax_year`
- demand bucket
- fallback applied flag
- source channel
- email present flag
- phone present flag
- consent-to-contact flag
- duplicate-group indicator

### 4. Drill-down entry point

Every row in the reporting table must provide a clear path to inspect the underlying lead and event evidence.

## Required filters

The reporting surface must support at least these filters:

- date range
- `county_id`
- `requested_tax_year`
- `served_tax_year`
- primary demand bucket
- fallback applied true/false
- `source_channel`
- duplicate-group only true/false
- quote-ready only true/false

Optional but useful filters:

- `funnel_stage`
- UTM source/medium/campaign
- email present true/false
- phone present true/false
- consent-to-contact true/false

## Canonical drill-down requirements

The lead detail drill-down must show the operator enough evidence to reconstruct both the submission and the reporting classification.

### Detail sections

At minimum, the detail surface must show:

#### 1. Lead identity and submission record

- `lead_id`
- created timestamp
- county
- account number
- `requested_tax_year`
- `source_channel`

#### 2. Contact snapshot

- owner name
- email value when stored
- phone value when stored
- consent-to-contact flag

#### 3. Classification snapshot

- `quote_context.status`
- canonical primary demand bucket derived from that status
- `county_supported`
- `property_supported`
- `quote_ready`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`

#### 4. Quote context snapshot

- `parcel_id`
- `property_type_code`
- `protest_recommendation`
- `expected_tax_savings_point`
- `defensible_value_point`

These fields are visible for internal ops inspection only and must not be mirrored into public surfaces.

#### 5. Attribution snapshot

- `anonymous_session_id`
- `funnel_stage`
- UTM source
- UTM medium
- UTM campaign
- UTM term
- UTM content

#### 6. Raw evidence view

The operator must be able to inspect the raw `lead_submitted` payload or an exact normalized rendering of it.

This is required so the reporting surface can be audited back to the event source of truth.

## Duplicate-review requirements

The `/admin/leads` surface must support duplicate-demand review using the canonical Stage 5 grouping key:

- `county_id`
- `account_number`
- `requested_tax_year`

The UI does not need destructive merge/dedupe actions in this tranche.

It does need to show:

- the size of the duplicate group
- the most recent submission in the group
- whether fallback or demand bucket changed across submissions
- links to inspect each submission separately

## Required route and page contract

### `GET /admin/leads`

This planned route should return:

- filter echo
- KPI summary
- demand-mix summaries
- paginated lead rows
- duplicate-group summary data

The response must remain internal/admin-only.

### `GET /admin/leads/{lead_id}`

This planned route should return:

- canonical lead row fields needed for admin review
- the matching `lead_submitted` event evidence
- normalized classification fields used by reporting
- duplicate-group metadata for peer submissions

### `/admin/leads`

This planned page should be the main operator entry point for lead reporting.

It should support:

- filter controls
- summary cards
- demand-mix tables or charts
- lead results table
- links into drill-down

### `/admin/leads/{leadId}`

This planned page or equivalent detail drawer should support:

- full submission inspection
- raw event evidence review
- duplicate-group context

## Non-requirements for this tranche

`OB1-T1` does not require:

- editing a lead
- deleting or merging leads
- case creation from a lead
- represented-customer conversion
- customer messaging or outreach automation
- packet, agreement, filing, billing, or Stage 6 valuation inspection
- visitor-level analytics before lead submission

## Relationship to existing contracts

This surface must use, not replace, the following sources of truth:

- [LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md](./LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md)
- [LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md](./LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md)
- [QUOTE_READY_VS_UNSUPPORTED_DEMAND_ANALYTICS.md](./QUOTE_READY_VS_UNSUPPORTED_DEMAND_ANALYTICS.md)
- [PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md](./PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md)

## Repo evidence informing this design

Current admin architecture evidence:

- `app/api/routes/admin.py`
- `app/api/deps/admin_auth.py`
- `apps/web/app/admin/_lib/api.ts`
- `apps/web/app/admin/_lib/AdminOpsNav.tsx`
- `docs/setup/local-development.md`

Current lead evidence and reporting-contract inputs:

- `app/services/lead_capture.py`
- `tests/unit/test_lead_capture.py`
- `tests/integration/test_stage16_lead_funnel_release_hardening.py`
