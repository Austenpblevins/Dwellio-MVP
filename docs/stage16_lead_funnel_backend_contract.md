# Stage 16 Lead Funnel Backend Contract

This note records the backend contract added for Stage 16 PR 1.

## Canonical route

Public lead capture remains:
- `POST /lead`

Search and quote routes remain unchanged:
- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`

## Lead persistence

The backend continues to use the existing canonical table family:
- `leads`
- `lead_events`

Additive `leads` fields:
- `account_number`
- `owner_name`
- `consent_to_contact`

Structured attribution and funnel context are persisted in `lead_events.event_payload`, including:
- `anonymous_session_id`
- `funnel_stage`
- UTM fields
- quote and parcel context

## Unsupported and missing-context behavior

`POST /lead` accepts lead capture even when quote context is incomplete.

The response returns `status = accepted` plus a `context_status` of:
- `quote_ready`
- `missing_quote_ready_row`
- `unsupported_property_type`
- `unsupported_county`

This keeps lead capture available without changing the canonical quote/search read-model strategy.

## Boundary rules

- No alternate public lead route was added.
- No heavy live comp analysis was added to request paths.
- No restricted MLS or listing data is exposed in the lead response.
- Quote/search remain backed by the existing public-safe read models.
