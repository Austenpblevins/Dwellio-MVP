# Lead Capture Duplicate And Attribution Rules

This document closes `S5-T3` for the current repo shape.

It defines how `POST /lead` preserves parcel-year context, how repeated submissions are handled, and which identifiers downstream reporting should use when grouping duplicate demand.

## Canonical write surfaces

Lead capture writes to:

- `leads`
- `lead_events`

The public write route remains:

- `POST /lead`

No alternate onboarding or represented-customer write surface is implied here.

## Attribution snapshot rules

Each accepted submission captures the request plus the resolved parcel/quote context at the time of submission.

Request-intent fields:

- `county_id`
- `account_number`
- `requested_tax_year`
- `owner_name`
- `email`
- `phone`
- `source_channel`
- `anonymous_session_id`
- `funnel_stage`
- UTM fields

Resolved context fields:

- `context_status`
- `county_supported`
- `property_supported`
- `quote_ready`
- `parcel_id`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `property_type_code`
- `protest_recommendation`
- `expected_tax_savings_point`
- `defensible_value_point`

The important parcel-year distinction is:

- `requested_tax_year` is the user’s intent
- `served_tax_year` is the parcel/quote year actually resolved at submission time

Frontend and admin consumers must never collapse those into one field.

## Duplicate-submission rules

Current MVP behavior is additive, not suppressive:

- every accepted `POST /lead` request creates a new `leads` row
- every accepted `POST /lead` request creates a `lead_submitted` event in `lead_events`
- repeated submissions are preserved as separate demand records rather than silently deduplicated on write

This is intentional for the current stage because:

- it preserves attribution snapshots
- it preserves changing fallback/quote context over time
- it avoids hiding repeat demand behind silent request-path dedupe

## Canonical duplicate-grouping key

For reporting and downstream admin review, the primary duplicate-demand grouping key is:

- `county_id`
- `account_number`
- `requested_tax_year`

Secondary grouping aids:

- normalized email when present
- normalized phone when present
- `anonymous_session_id` when contact info is absent
- `source_channel`

Non-keys for duplicate grouping:

- `served_tax_year`
- `tax_year_fallback_applied`
- `protest_recommendation`

Those fields are context snapshots and may change over time for the same parcel-year demand.

## State-specific rules

### `quote_ready`

- persist the resolved `parcel_id` when available
- persist quote context in `lead_events.event_payload.quote_context`
- preserve both requested and served tax years when fallback occurred

### `missing_quote_ready_row`

- still accept the lead
- preserve `parcel_id` when parcel context resolved
- preserve the missing-quote state explicitly instead of fabricating quote readiness

### `unsupported_property_type`

- still accept the lead
- preserve `property_type_code` when available
- do not mark the lead as quote-ready

### `unsupported_county`

- still accept the lead
- preserve the user-requested county/account/tax-year context
- leave `parcel_id` unresolved rather than inventing parcel linkage

## Downstream handling rules

- Repeat submissions must remain auditable as separate lead records.
- Admin reporting may roll up duplicates, but raw lead/event history must remain reconstructable.
- No duplicate rule in this stage creates represented-customer records, case records, agreement state, or billing state.
- Stage 6 advanced equity valuation is out of scope here and must not be inferred from lead duplication or attribution behavior.
