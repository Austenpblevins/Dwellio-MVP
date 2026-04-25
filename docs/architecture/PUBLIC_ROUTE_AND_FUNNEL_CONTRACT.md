# Public Route And Funnel Contract

This document locks the Stage 3 and Stage 5 public-surface contract currently supported by the repo.

Tickets covered here:

- `S3-T1` Lock the canonical public route inventory
- `S3-T2` Run a public payload safety and owner-masking audit
- `S5-T1` Define all funnel states

This document does not finalize:

- unsupported-state UX copy and CTA behavior (`S5-T2`)

Companion docs:

- [FRONTEND_SAFE_PAYLOAD_GUARANTEES.md](./FRONTEND_SAFE_PAYLOAD_GUARANTEES.md) closes `S3-T5`
- [LEAD_FUNNEL_UX_COPY_MATRIX.md](./LEAD_FUNNEL_UX_COPY_MATRIX.md) closes `S5-T2`
- [LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md](./LEAD_CAPTURE_DUPLICATE_AND_ATTRIBUTION_RULES.md) closes `S5-T3`
- [LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md](./LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md) closes `S5-T4`
- [QUOTE_READY_VS_UNSUPPORTED_DEMAND_ANALYTICS.md](./QUOTE_READY_VS_UNSUPPORTED_DEMAND_ANALYTICS.md) closes `S5-T5`

## 1. Canonical public route inventory

The approved public route family is:

| Route | Purpose | Public-safe contract |
|---|---|---|
| `GET /healthz` | Basic availability probe | No internal diagnostics or dependency details. |
| `GET /search` | Public parcel lookup by address-like query | Returns public-safe parcel matches only. |
| `GET /search/autocomplete` | Lightweight typeahead suggestions | Returns public-safe parcel suggestions only. |
| `GET /parcel/{county_id}/{tax_year}/{account_number}` | Public parcel summary | Returns masked/public-safe parcel facts, tax metadata, caveats, and tax-year fallback metadata. |
| `GET /quote/{county_id}/{tax_year}/{account_number}` | Refined public quote summary | Returns quote read-model outputs plus tax-year fallback metadata. |
| `GET /quote/{county_id}/{tax_year}/{account_number}/explanation` | Refined quote explanation | Returns public explanation content only. |
| `GET /quote/instant/{county_id}/{tax_year}/{account_number}` | Instant estimate surface | Returns the separate instant-quote contract without exposing internal readiness/debug metadata. |
| `POST /lead` | Quote-to-lead capture | Persists lead/contact intent while returning public-safe funnel context status. |

No additional public `/api/v1/...` family is approved for MVP V1.

## 2. Payload-safety audit

### Search and autocomplete

Allowed:

- parcel identity fields needed for routing
- public address
- masked/public-safe owner display
- public match confidence labels used by the current UI contract

Not allowed:

- admin/debug candidate fields
- owner-similarity debug reasons
- restricted listing or MLS artifacts
- internal ranking traces

### Parcel summary

Allowed:

- public parcel identity and address
- masked/public-safe owner display
- value, exemption, and tax summary fields
- user-facing caveats
- tax-year fallback metadata

Not allowed:

- raw CAD owner snapshots such as `cad_owner_name`
- ownership reconciliation internals such as `owner_source_basis` or `owner_confidence_score`
- raw JSON blobs such as `component_breakdown_json`
- tax-assignment debug metadata such as `assignment_method`, `assignment_confidence`, or `assignment_reason_code`
- restricted comps, MLS, or packet/internal workflow data

### Refined quote

Allowed:

- public quote values and recommendation fields
- public explanation bullets/json used by the quote experience
- tax-year fallback metadata

Not allowed:

- raw comp sets
- internal valuation-run identifiers
- admin/debug traces or reviewer-only rationale

### Instant quote

Allowed:

- public instant estimate subject, estimate, explanation, disclaimers, and unsupported reason
- tax-year fallback metadata

Not allowed:

- internal tax-rate basis state such as `tax_rate_basis_status`
- internal supportability diagnostics
- raw neighborhood/segment debug data

### Lead capture

Allowed:

- `accepted` response state
- public-safe funnel context status
- county/property/quote-ready booleans
- parcel-year identity needed to preserve context

Not allowed:

- internal event payload internals
- hidden admin workflow status
- packet/case data

Operator auditability and admin reporting requirements for accepted lead submissions are defined separately in [LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md](./LEAD_EVENT_AUDITABILITY_AND_ADMIN_REPORTING.md).

## 3. Owner-masking contract

Public owner display rules:

- Individual owner names are masked to a privacy-safe display.
- Public entity-style owner names may remain visible when they are already public-record entity labels.
- When confidence is limited, the surface should prefer conservative display and a caveat instead of exposing reconciliation details.
- Search and parcel surfaces must stay aligned on masked/public-safe owner presentation.

## 4. Funnel state definitions

These are the approved Stage 5 funnel states.

| Funnel state | Meaning | Current repo evidence |
|---|---|---|
| `quote_ready` | The parcel-year is within supported scope and a quote-ready row exists. | `LeadCreateResponse.context_status = "quote_ready"` |
| `unsupported_county` | The request is outside the Harris/Fort Bend MVP boundary. | `LeadCreateResponse.context_status = "unsupported_county"` |
| `unsupported_property_type` | The county is supported, but the parcel is not in the current SFR-supported property scope. | `LeadCreateResponse.context_status = "unsupported_property_type"` |
| `missing_quote_ready_row` | The parcel-year is in a supported boundary, but there is not yet a quote-ready read-model row to serve. | `LeadCreateResponse.context_status = "missing_quote_ready_row"` |
| `system_or_config_failure` | A public route cannot complete because of an internal service/config/runtime failure rather than a supported unsupported-state. | Route-level failure only today; broader fallback UX remains for `S3-T3` and `S5-T2`. |

Important boundary:

- `system_or_config_failure` is a defined funnel state for planning and support purposes even though it is not yet emitted as a lead `context_status`.
- `unsupported_county`, `unsupported_property_type`, and `missing_quote_ready_row` are graceful business-state outcomes, not generic 5xx failures.

## 5. Known route-level behaviors that remain intentional

- Parcel and refined quote lookups return `404` when no requested-year or fallback-year row exists.
- Instant quote may return `404` for missing subject data or `501` when the capability is intentionally not implemented for the request path.
- Lead capture remains the canonical quote-to-lead public write surface.

## 6. Unsupported-state and fallback contract

This is the Stage 3 unsupported/fallback contract for the current repo shape.

| State | Trigger | Current public behavior | User-safe rule |
|---|---|---|---|
| `unsupported_county` | The request is outside Harris/Fort Bend scope. | Public quote funnel should not imply quote support; `POST /lead` returns `context_status = "unsupported_county"` when the county is submitted. | Explain the county boundary honestly and keep capture optional rather than pretending a quote exists. |
| `unsupported_property_type` | The parcel resolves, but the property is outside the SFR-only MVP boundary. | Parcel-year context may still exist; `POST /lead` returns `context_status = "unsupported_property_type"`. | Keep the boundary explicit and do not fabricate quote readiness. |
| `missing_readiness_or_quote_row` | The parcel-year is in scope, but quote-support data is not ready or no quote row exists. | Parcel summary may still serve with fallback metadata; refined quote routes return `404` when no requested-year or fallback-year row exists; `POST /lead` returns `context_status = "missing_quote_ready_row"` when parcel context exists but quote readiness does not. | Treat as a graceful pending/unsupported business state, not a silent success. |
| `prior_year_fallback` | The requested year is unavailable but a supported prior year exists. | Parcel, refined quote, and instant quote surfaces disclose fallback using `requested_tax_year`, `served_tax_year`, `tax_year_fallback_applied`, `tax_year_fallback_reason`, and `data_freshness_label`. | Fallback must be explicit in payload and UI; do not silently masquerade as current-year data. |
| `system_or_config_failure` | Backend config, reachability, or runtime failure prevents a trustworthy response. | Public pages must show an explicit config/reachability failure; route-level behavior remains ordinary 5xx/infra failure when the server cannot satisfy the request. | Never translate a system failure into `unsupported_county`, `unsupported_property_type`, or `quote_ready`. |

Important distinctions:

- `unsupported_county`, `unsupported_property_type`, and `missing_readiness_or_quote_row` are business-state outcomes.
- `prior_year_fallback` is a served-data state, not an error state.
- `system_or_config_failure` is an operational failure and must stay visibly distinct from unsupported demand.

## 7. Per-route fallback matrix

| Route | Exact-year success | Fallback behavior | Unsupported/failure behavior |
|---|---|---|---|
| `GET /search` | Returns `200` with public-safe results. | No year fallback semantics. | Empty result sets remain `200`; system failures remain explicit failures, not fake results. |
| `GET /search/autocomplete` | Returns `200` with public-safe suggestions. | No year fallback semantics. | Empty suggestion sets remain `200`; system failures remain explicit failures. |
| `GET /parcel/{county_id}/{tax_year}/{account_number}` | Returns `200` parcel summary. | Serves nearest prior available year and discloses fallback metadata. | Returns `404` when no parcel-year identity can be resolved for the requested year or any prior year. |
| `GET /quote/{county_id}/{tax_year}/{account_number}` | Returns `200` refined quote. | Serves nearest prior available quote row and discloses fallback metadata. | Returns `404` when no quote row exists for the requested year or any prior year. |
| `GET /quote/{county_id}/{tax_year}/{account_number}/explanation` | Returns `200` public explanation. | Serves nearest prior available explanation and discloses fallback metadata. | Returns `404` when no explanation-capable quote row exists for the requested year or any prior year. |
| `GET /quote/instant/{county_id}/{tax_year}/{account_number}` | Returns `200` instant quote payload. | May serve prior-year basis and discloses fallback metadata. | Returns `404` for missing parcel-year identity and `501` when the capability is intentionally unavailable for the request. |
| `POST /lead` | Returns `200` accepted lead contract. | Preserves parcel-year fallback context in the response payload when applicable. | Returns graceful business-state `context_status` values for unsupported county/property or missing quote-ready rows; system failures should remain explicit failures. |

## 8. Audit evidence in code and tests

Primary repo evidence:

- `app/api/routes/health.py`
- `app/api/routes/search.py`
- `app/api/routes/parcel.py`
- `app/api/routes/quote.py`
- `app/api/routes/leads.py`
- `app/services/address_resolver.py`
- `app/services/parcel_summary.py`
- `tests/integration/test_public_parcel_flows.py`
- `tests/unit/test_quote_api.py`
- `tests/unit/test_lead_capture.py`
- `tests/unit/test_search_services.py`

This contract is intentionally narrow.
It locks the public route family, the payload-safety audit, and the funnel-state vocabulary without introducing new routes or expanding product scope.
