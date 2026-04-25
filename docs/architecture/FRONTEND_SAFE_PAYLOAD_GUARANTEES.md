# Frontend-Safe Payload Guarantees

This document closes `S3-T5` for the current repo shape.

It defines which public-response fields frontend consumers may treat as stable, which fields must be treated as optional or derived, and which internal fields must never be expected on public routes.

## Contract rules

- Frontend code may depend only on fields declared in the canonical public response models.
- Required fields in those models are the stable rendering contract for the current MVP.
- Optional fields may be `null` or absent from meaningful business states and must not be used as implicit status flags.
- Derived fields are safe to display, but frontend code must not recompute or reinterpret them as if they were raw source truth.
- Internal diagnostics, reviewer-only reasoning, raw comps, raw county capability metadata, and rollout telemetry are never part of the frontend contract.

## Route guarantees

### `GET /healthz`

Stable:

- `status`

Never public:

- dependency diagnostics
- secret/config detail
- database internals

### `GET /search`
### `GET /search/autocomplete`

Stable:

- `county_id`
- `account_number`
- `parcel_id`
- `address`
- `match_basis`
- `match_score`
- `confidence_label`

Optional:

- `tax_year`
- `situs_zip`
- `owner_name`

Derived:

- `match_score`
- `confidence_label`

Never public:

- admin/debug ranking traces
- owner-matching debug reasons
- restricted listing or MLS artifacts

### `GET /parcel/{county_id}/{tax_year}/{account_number}`

Stable:

- `county_id`
- `tax_year`
- `account_number`
- `parcel_id`
- `address`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `data_freshness_label`
- `completeness_score`
- `warning_codes`
- `public_summary_ready_flag`
- `caveats`

Optional:

- `owner_name`
- property summary fields
- value summary fields
- exemption summary fields
- tax summary fields
- owner/value/exemption/tax nested summary objects

Derived:

- `owner_summary`
- `value_summary`
- `exemption_summary`
- `tax_summary`
- `caveats`

Never public:

- raw owner-source reconciliation internals
- tax-assignment debug metadata
- internal readiness/debug fields

### `GET /quote/{county_id}/{tax_year}/{account_number}`

Stable:

- `county_id`
- `tax_year`
- `account_number`
- `parcel_id`
- `address`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `data_freshness_label`
- `explanation_bullets`

Optional:

- value-point fields
- savings-point fields
- `estimated_contingency_fee`
- `confidence`
- `basis`
- `protest_recommendation`
- `explanation_json`

Derived:

- `protest_recommendation`
- `explanation_bullets`
- `explanation_json`

Never public:

- raw comp sets
- reviewer-only rationale
- valuation-run identifiers

### `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`

Stable:

- `county_id`
- `tax_year`
- `account_number`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `data_freshness_label`
- `explanation_json`
- `explanation_bullets`

Derived:

- `explanation_json`
- `explanation_bullets`

Never public:

- raw comp evidence
- internal scoring traces

### `GET /quote/instant/{county_id}/{tax_year}/{account_number}`

Stable:

- `supported`
- `county_id`
- `tax_year`
- `account_number`
- `basis_code`
- `requested_tax_year`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`
- `data_freshness_label`
- `explanation`
- `disclaimers`

Optional:

- `subject`
- `estimate`
- `unsupported_reason`
- `next_step_cta`

Derived:

- `basis_code`
- `estimate`
- `explanation`
- `disclaimers`

Never public:

- `quote_version`
- tax-rate basis status internals
- warning taxonomy payloads
- county capability records
- tax-profile internals
- shadow savings fields
- rollout telemetry such as `public_rollout_state`

### `POST /lead`

Stable:

- `status`
- `lead_id`
- `context_status`
- `lead_capture_allowed`
- `county_supported`
- `quote_ready`
- `requested_tax_year`

Optional:

- `property_supported`
- `parcel_id`
- `served_tax_year`
- `tax_year_fallback_applied`
- `tax_year_fallback_reason`

Derived:

- `context_status`
- fallback metadata carried from resolved parcel/quote context

Never public:

- full `lead_events.event_payload`
- UTM/debug attribution internals
- hidden admin workflow state

## Frontend handling rules

- Use explicit state fields such as `supported`, `context_status`, and `tax_year_fallback_applied` instead of inferring state from missing optional fields.
- Treat all money, value, and property-detail fields as nullable even when they are often present in healthy paths.
- Treat nested summary objects as additive convenience payloads, not as proof that raw source truth is complete.
- Do not bind UI behavior to internal field names seen in tests, logs, or admin flows unless those fields are part of the public response models above.
