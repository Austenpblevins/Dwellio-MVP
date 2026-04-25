# Lead Event Auditability And Admin Reporting

This document closes `S5-T4` for the current repo shape.

It defines the minimum lead-event evidence and admin reporting contract needed for Dwellio ops to:

- reconstruct what a user submitted
- reconstruct the funnel and quote context the backend resolved at submission time
- review lead volume, source mix, and lead quality without redefining quote supportability locally

This is an auditability and reporting contract only.
It does not add new public routes, customer onboarding flows, represented-customer workflows, or Stage 6 valuation behavior.

## Canonical write evidence

Current lead capture writes to:

- `leads`
- `lead_events`

The canonical public write route remains:

- `POST /lead`

Current guaranteed event coverage for this stage:

- every accepted submission creates one `lead_submitted` row in `lead_events`

This ticket does not require additional event families such as `quote_viewed` or `form_started`.
Those may exist later, but they are not required for the current admin reporting baseline.

## Minimum reconstructable submission snapshot

For every accepted lead, ops must be able to reconstruct the submission from the combination of:

- the `leads` row
- the matching `lead_events` row where `event_code = "lead_submitted"`

### Fields that must remain reconstructable

Request-intent and attribution:

- `county_id`
- `account_number`
- `requested_tax_year`
- `owner_name`
- `email`
- `phone`
- `source_channel`
- `anonymous_session_id`
- `funnel_stage`
- UTM fields:
  - `source`
  - `medium`
  - `campaign`
  - `term`
  - `content`

Resolved quote and parcel context:

- `status`
- `county_supported`
- `property_supported`
- `quote_ready`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `parcel_id`
- `property_type_code`
- `protest_recommendation`
- `expected_tax_savings_point`
- `defensible_value_point`

Contact evidence:

- `owner_name`
- `email_present`
- `phone_present`
- `consent_to_contact`

## Canonical event payload contract

For the current repo shape, `lead_events.event_payload` must preserve this structure for `lead_submitted`:

```json
{
  "anonymous_session_id": "string|null",
  "funnel_stage": "string|null",
  "utm": {
    "source": "string|null",
    "medium": "string|null",
    "campaign": "string|null",
    "term": "string|null",
    "content": "string|null"
  },
  "quote_context": {
    "status": "quote_ready|missing_quote_ready_row|unsupported_property_type|unsupported_county",
    "county_supported": true,
    "property_supported": true,
    "quote_ready": true,
    "county_id": "harris",
    "account_number": "1001001001001",
    "requested_tax_year": 2026,
    "served_tax_year": 2025,
    "tax_year_fallback_applied": true,
    "tax_year_fallback_reason": "requested_year_unavailable",
    "parcel_id": "uuid|null",
    "property_type_code": "sfr|null",
    "protest_recommendation": "file_protest|null",
    "expected_tax_savings_point": 975.0,
    "defensible_value_point": 320000.0
  },
  "contact": {
    "owner_name": "Alex Example",
    "email_present": true,
    "phone_present": true,
    "consent_to_contact": true
  }
}
```

Important boundary rules:

- raw lead/event history is the audit source of truth
- admin reporting may aggregate from this payload, but must not destroy the raw event trail
- frontend consumers must not depend on `lead_events.event_payload`
- Stage 6 advanced equity valuation must not be inferred from lead-event payload design

## How ops reconstruct what the user saw

This stage does not require storing a literal rendered copy snapshot in `lead_events`.

Ops reconstruct the user-visible state by combining:

- `lead_events.event_payload.quote_context.status`
- `lead_events.event_payload.quote_context.tax_year_fallback_*`
- `lead_events.event_payload.funnel_stage`
- the canonical funnel-state contract in [PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md](./PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md)
- the canonical CTA/copy matrix in [LEAD_FUNNEL_UX_COPY_MATRIX.md](./LEAD_FUNNEL_UX_COPY_MATRIX.md)

This is sufficient for the current maturity level because:

- Stage 4 owns quote-state and supportability semantics
- Stage 5 owns approved CTA/copy behavior for those states
- `lead_submitted` preserves the resolved state snapshot at submission time

## Admin reporting requirements

The MVP admin/reporting layer must support at least the following views or equivalent query outputs.

### 1. Submission volume

Ops must be able to review lead counts grouped by:

- submission day
- `source_channel`
- `county_id`
- `requested_tax_year`
- `quote_context.status`

### 2. Supported vs unsupported demand

Ops must be able to separate:

- `quote_ready`
- `missing_quote_ready_row`
- `unsupported_property_type`
- `unsupported_county`

This reporting must use the persisted event snapshot and must not redefine supportability outside the canonical quote/funnel contracts.

### 3. Fallback demand

Ops must be able to review:

- how often `tax_year_fallback_applied = true`
- which requested years are falling back
- which served years are actually being used

### 4. Contactability quality

Ops must be able to review lead quality using at least:

- email present vs missing
- phone present vs missing
- consent to contact true vs false

### 5. Duplicate-demand review

Ops must be able to group repeated submissions using the duplicate-review contract from [LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md](./LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md), especially:

- `county_id`
- `account_number`
- `requested_tax_year`

Rollups may flag probable duplicates, but separate submissions must remain individually auditable.

## Admin reporting non-requirements

This ticket does not require:

- silent write-time deduplication
- creation of represented-customer, case, agreement, or billing records
- customer-facing analytics surfaces
- packet, filing, or Stage 6 valuation reporting
- reconstruction of every intermediate page render before submission

## Operator review rules

- Admin tools may summarize lead quality and volume, but must allow drill-down to the original `lead_submitted` event.
- Unsupported demand must remain visible as demand, not discarded as noise.
- Missing quote-readiness demand must remain distinct from unsupported-county and unsupported-property demand.
- Reporting must preserve the difference between `requested_tax_year` and `served_tax_year`.
- Reporting must treat `protest_recommendation` and quote-point fields as point-in-time submission context, not as a case decision or valuation-engine result.

## Evidence in repo

Primary implementation evidence:

- `app/services/lead_capture.py`
- `app/api/routes/leads.py`
- `tests/unit/test_lead_capture.py`
- `tests/integration/test_stage16_lead_funnel_release_hardening.py`

Companion planning and contract docs:

- [PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md](./PUBLIC_ROUTE_AND_FUNNEL_CONTRACT.md)
- [LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md](./LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md)
- [LEAD_FUNNEL_UX_COPY_MATRIX.md](./LEAD_FUNNEL_UX_COPY_MATRIX.md)
- [QUOTE_READY_VS_UNSUPPORTED_DEMAND_ANALYTICS.md](./QUOTE_READY_VS_UNSUPPORTED_DEMAND_ANALYTICS.md)
