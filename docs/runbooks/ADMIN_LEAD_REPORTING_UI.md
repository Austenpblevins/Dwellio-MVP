# Admin Lead Reporting UI

This runbook covers the protected internal lead reporting surface added for the off-board `OB1` tranche.

## Access model

- Operators sign in through `/admin/login`.
- The web app stores the internal admin token cookie and forwards it only to protected admin routes.
- Backend API access remains protected by `x-dwellio-admin-token` or the `dwellio_admin_token` cookie.

## Canonical routes

Internal backend routes:

- `GET /admin/leads`
- `GET /admin/leads/{lead_id}`

Internal web pages:

- `/admin/leads`
- `/admin/leads/{leadId}`

These routes are internal/admin-facing by contract.
They expose lead-event evidence and reporting details that must not appear in public parcel, quote, or lead responses.

## What this surface is for

Use the lead reporting surface to:

- review accepted lead volume
- separate quote-ready demand from unsupported or reachable-but-unquoted demand
- inspect fallback demand
- review duplicate parcel-year submissions
- drill into the raw `lead_submitted` evidence for a specific lead

Do not use this surface to:

- create represented customers or cases
- trigger outreach automation
- interpret Stage 6 valuation quality
- inspect packet, filing, or billing workflows

## Main page workflow

Start at `/admin/leads`.

### 1. Apply filters

The page supports:

- county
- requested tax year
- served tax year
- demand bucket
- source channel
- submitted date range
- fallback applied
- duplicate-only
- quote-ready-only

### 2. Review KPI cards

Use the top summary cards to quickly assess:

- accepted leads
- quote-ready demand
- reachable-but-unquoted demand
- duplicate group count

### 3. Review demand mix

Use the demand-mix section to confirm whether current demand is concentrated in:

- supported quote-ready demand
- unsupported county demand
- unsupported property demand
- in-scope but not-yet-quote-ready demand

### 4. Review duplicate groups

Use the duplicate-group section to identify:

- parcel-years with repeated submissions
- whether fallback appeared across the group
- whether demand-bucket state changed across submissions

### 5. Inspect lead rows

Use the main results table to inspect:

- submission timestamp
- parcel-year context
- demand classification
- contactability
- duplicate-review status

Open a row through the `Inspect lead` action for full drill-down.

## Lead detail workflow

Open `/admin/leads/{leadId}` to inspect one accepted lead in detail.

The detail page is the canonical drill-down surface for:

- contact snapshot
- attribution snapshot
- quote context snapshot
- duplicate peer submissions
- raw `lead_submitted` payload

## Interpretation rules

- `quote_ready_demand` means the lead was submitted from a quote-ready parcel-year state.
- `reachable_unquoted_demand` means the parcel-year was in scope but no quote-ready row existed at submission time.
- `unsupported_county_demand` and `unsupported_property_demand` must remain separate.
- `served_tax_year` may differ from `requested_tax_year`; this is fallback disclosure, not unsupported demand.
- Raw event evidence is the internal audit source of truth.

## Operational cautions

- Do not copy raw event payloads into public surfaces or customer communication blindly.
- Do not treat repeated submissions as a reason to delete history.
- Do not infer valuation-engine strength or protest viability from lead reporting alone.
- Do not convert a lead into a case or represented-customer workflow from this surface unless a later stage explicitly adds that behavior.
