# Quote-Ready Vs Unsupported-Demand Analytics

This document closes `S5-T5` for the current repo shape.

It defines how Dwellio should classify and report quote-to-lead demand so ops can distinguish:

- supported demand that reached a quote-ready state
- unsupported demand that reflects out-of-scope county or property requests
- reachable-but-not-yet-quote-ready demand

This is an analytics and reporting contract only.
It does not create a new frontend analytics service, redefine quote supportability, or open Stage 6 advanced equity valuation work.

## Canonical signal source

For this stage, the canonical analytics source is accepted lead capture:

- `POST /lead`
- `lead_events` rows where `event_code = "lead_submitted"`

The required classification fields come from:

- `lead_events.event_payload.quote_context.status`
- `lead_events.event_payload.quote_context.county_supported`
- `lead_events.event_payload.quote_context.property_supported`
- `lead_events.event_payload.quote_context.quote_ready`
- `lead_events.event_payload.quote_context.requested_tax_year`
- `lead_events.event_payload.quote_context.served_tax_year`
- `lead_events.event_payload.quote_context.tax_year_fallback_applied`
- `lead_events.event_payload.quote_context.tax_year_fallback_reason`
- `lead_events.event_payload.quote_context.county_id`
- `lead_events.event_payload.funnel_stage`
- `leads.source_channel`

Important constraint:

- Stage 5 analytics must consume the Stage 4 and Stage 5 state contracts; it must not invent a second local notion of quote supportability.

## Canonical demand buckets

Every accepted lead submission must be classified into exactly one primary demand bucket based on `quote_context.status`.

| `quote_context.status` | Primary analytics bucket | Meaning |
|---|---|---|
| `quote_ready` | `quote_ready_demand` | The user submitted from a parcel-year state where Dwellio had a quote-ready row and a supported public quote path. |
| `missing_quote_ready_row` | `reachable_unquoted_demand` | The request was within current business scope, but no quote-ready row was available at submission time. |
| `unsupported_property_type` | `unsupported_property_demand` | The county was in scope, but the resolved parcel is outside the current SFR-only property boundary. |
| `unsupported_county` | `unsupported_county_demand` | The request is outside the Harris/Fort Bend MVP public scope. |

No other primary bucket should be created locally for this stage.

## Required reporting split

Admin reporting must, at minimum, support these top-level rollups:

### 1. Quote-ready demand

Count accepted lead submissions where:

- `quote_context.status = "quote_ready"`

This is the core measure of supported public acquisition demand.

### 2. Unsupported demand

Count accepted lead submissions where:

- `quote_context.status = "unsupported_property_type"`
- `quote_context.status = "unsupported_county"`

These must remain separate from each other.
Do not collapse county-boundary interest and property-boundary interest into one generic unsupported bucket.

### 3. Reachable-but-unquoted demand

Count accepted lead submissions where:

- `quote_context.status = "missing_quote_ready_row"`

This bucket is not unsupported demand.
It represents in-scope requests that were reachable through the funnel but not yet quote-ready.

## Non-lead operational failure handling

`system_or_config_failure` remains a valid funnel state for UX and support purposes, but it is not currently emitted through the lead capture status contract.

For this stage:

- do not fold route failures into unsupported demand
- do not backfill system failures as `missing_quote_ready_row`
- track route or config failures through operational monitoring and error logs, not by mutating lead analytics definitions

This preserves the distinction between:

- business-scope demand
- quote-readiness gaps
- operational failures

## Required dimensions

The analytics layer must be able to segment the above demand buckets by at least:

- submission day
- `county_id`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `source_channel`
- `funnel_stage`

Optional but useful slices:

- UTM source/medium/campaign
- contactability quality (`email_present`, `phone_present`, `consent_to_contact`)

## Required ratio and trend views

The MVP reporting contract must support at least:

### 1. Supported-demand share

`quote_ready_demand / total accepted lead submissions`

### 2. Unsupported-demand mix

Break out:

- `unsupported_county_demand / total accepted lead submissions`
- `unsupported_property_demand / total accepted lead submissions`

### 3. Reachable-but-unquoted share

`reachable_unquoted_demand / total accepted lead submissions`

This is the primary signal for parcel-year demand that sits inside the intended boundary but lacks quote-ready publication.

### 4. Fallback share within supported demand

Within `quote_ready_demand`, report:

- exact-year served demand
- prior-year fallback served demand

Do not treat fallback itself as unsupported demand.

## Classification guardrails

- Use `quote_context.status` as the primary bucket source of truth.
- Do not reclassify demand using `protest_recommendation`, `expected_tax_savings_point`, or `defensible_value_point`.
- Do not treat `served_tax_year != requested_tax_year` as unsupported demand.
- Do not use packet, case, agreement, billing, or Stage 6 valuation artifacts to retroactively re-bucket lead demand.
- Do not infer customer intent from route failures that never produced an accepted lead submission.

## What this stage can and cannot prove

This stage can support:

- accepted-lead demand mix
- supported vs unsupported submission mix
- in-scope-but-not-quote-ready demand mix
- source-channel and attribution analysis for accepted leads

This stage cannot yet support:

- full visitor-level funnel abandonment analytics
- reliable counts of users who saw a quote state but never submitted a lead
- a separate client-side analytics event stream
- valuation-engine demand analysis

## Relationship to other contracts

- Stage 4 defines quote-state semantics and supportability behavior.
- `S5-T1` defines the canonical funnel states.
- `S5-T3` defines duplicate-demand grouping and parcel-year attribution.
- `S5-T4` defines raw auditability and admin reporting evidence.

This document only defines how those existing signals are rolled up into analytics categories.

## Evidence in repo

Primary implementation evidence:

- `app/services/lead_capture.py`
- `tests/unit/test_lead_capture.py`
- `tests/integration/test_stage16_lead_funnel_release_hardening.py`

Companion docs:

- [PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md](./PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md)
- [LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md](./LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md)
- [LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md](./LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md)
- [LEAD_FUNNEL_UX_COPY_MATRIX.md](./LEAD_FUNNEL_UX_COPY_MATRIX.md)
