# Stage 13 Public Parcel Summary MVP

This document describes the public parcel search and parcel summary behavior introduced for Stage 13.

## Public Pages

Routes:
- `/`
- `/search?address={query}`
- `/parcel/{county_id}/{tax_year}/{account_number}`

Behavior:
- The home page submits address or account-number searches into the canonical public search API.
- The search-results page shows public-safe parcel matches with parcel-year identity.
- The parcel page shows value summary, exemptions, tax-rate components, total effective rate, privacy-safe owner display, and data caveats.
- If a quote-ready row exists in `v_quote_read_model`, the parcel page also renders a narrow quote-safe teaser.

## Public API Sources

Public endpoints used by the web app:
- `GET /search?address={query}`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`

Backing read models and views:
- `v_search_read_model`
- `parcel_summary_view`
- `v_quote_read_model`

The public web app does not query staging tables or canonical tables directly.

## Tax-Year Fallback Policy

Public parcel and quote routes use a deterministic "most recent available year" fallback:
- first try the exact requested `tax_year`
- if that row is unavailable, serve `max(tax_year)` where `tax_year <= requested_tax_year` for the same `county_id` and `account_number`
- if neither the requested year nor any prior year exists, return `404`

Public response metadata fields:
- `requested_tax_year`: the year requested in the route
- `served_tax_year`: the year actually served from the read model or parcel summary view
- `tax_year_fallback_applied`: `true` when a prior year is served
- `tax_year_fallback_reason`: public-safe reason code such as `requested_year_unavailable`
- `data_freshness_label`: `current_year` or `prior_year_fallback`

Important boundary:
- prior-year fallback must be disclosed explicitly in the payload and UI
- the API must not silently present prior-year data as requested-year data

## Owner Privacy Restrictions

Public owner display rules:
- Individual owner names are masked to initials plus final surname token when a public owner label is available.
- Entity-style names such as `LLC`, `Inc`, `Trust`, `Bank`, or district-style owners remain visible because they are already entity public-record names.
- If no stable public-safe owner label is available, the owner display is hidden and a caveat can explain the limitation.

Examples:
- `Alex Example` -> `A. Example`
- `Alex Jordan Example` -> `A. J. Example`
- `Oak Meadow Holdings LLC` -> `Oak Meadow Holdings LLC`

## Warning and Caveat Display

The parcel endpoint returns both:
- `warning_codes`
- `caveats[]` with `code`, `severity`, `title`, and `message`

Page behavior:
- `critical` caveats are rendered first in a red treatment.
- `warning` caveats use amber treatment.
- `info` caveats use neutral treatment.
- Low completeness and limited owner confidence add explicit user-facing caveats instead of exposing internal blocker language.

## Quote-Safe Public Behavior

Quote behavior is secondary in Stage 13 and remains read-model only:
- no live comp generation
- no raw comp evidence exposure
- no restricted MLS/listing disclosure

When a quote row exists, the parcel page can show:
- defensible value
- expected tax savings
- recommendation
- explanation bullets

Operational note:
- canonical quote routes return quote payloads only when precomputed read-model rows exist
- when no quote row exists for the requested year or any prior year, `GET /quote/{county_id}/{tax_year}/{account_number}` and `/explanation` return `404`
- the Stage 13 closeout path may populate quote-support rows from canonical `parcel_summary_view` inputs when deeper feature and comp tables are still sparse, and the public explanation payload must state that limitation explicitly

## Stage Boundary

Included in Stage 13:
- public parcel search
- public parcel summary page
- privacy-safe owner display
- caveat and warning display
- read-model-backed quote teaser behavior when quote rows already exist

Explicitly deferred beyond Stage 13:
- new protest-case admin surfaces
- evidence packet workflows
- live valuation or comp generation in request paths
- any alternate public search or quote architecture

## Validation Notes

- Keep `v_search_read_model` and `v_quote_read_model` as the only canonical public read models for Stage 13.
- Use `npm run build` as the frontend parity gate. If Turbopack fails only inside a restricted sandbox while the same build passes in a normal local environment, treat that as environment-specific and non-blocking for this stage.

## Sample Search Payload

```json
{
  "results": [
    {
      "county_id": "harris",
      "tax_year": 2026,
      "account_number": "1001001001001",
      "parcel_id": "f9723e7c-5ec5-49d4-a7a3-0a0df83ef2e1",
      "address": "101 Main St, Houston, TX 77002",
      "situs_zip": "77002",
      "owner_name": "A. Example",
      "match_basis": "address_exact",
      "match_score": 0.98,
      "confidence_label": "very_high"
    }
  ]
}
```

## Sample Parcel Payload

```json
{
  "county_id": "harris",
  "tax_year": 2026,
  "requested_tax_year": 2026,
  "served_tax_year": 2026,
  "tax_year_fallback_applied": false,
  "tax_year_fallback_reason": null,
  "data_freshness_label": "current_year",
  "account_number": "1001001001001",
  "address": "101 Main St, Houston, TX 77002",
  "owner_name": "A. Example",
  "completeness_score": 86.0,
  "warning_codes": ["missing_geometry"],
  "public_summary_ready_flag": true,
  "owner_summary": {
    "display_name": "A. Example",
    "owner_type": "individual",
    "privacy_mode": "masked_individual_name",
    "confidence_label": "medium"
  },
  "value_summary": {
    "market_value": 450000,
    "assessed_value": 350000,
    "appraised_value": 350000,
    "certified_value": 345000,
    "notice_value": 350000
  },
  "exemption_summary": {
    "exemption_value_total": 100000,
    "homestead_flag": true,
    "over65_flag": false,
    "disabled_flag": false,
    "disabled_veteran_flag": false,
    "freeze_flag": false,
    "exemption_type_codes": ["homestead"],
    "raw_exemption_codes": ["HS"]
  },
  "tax_summary": {
    "effective_tax_rate": 0.021,
    "estimated_taxable_value": 245000,
    "estimated_annual_tax": 5145,
    "component_breakdown": [
      {
        "unit_type_code": "county",
        "unit_name": "Harris County",
        "rate_component": "maintenance",
        "rate_value": 0.01
      }
    ]
  },
  "caveats": [
    {
      "code": "missing_geometry",
      "severity": "info",
      "title": "Map geometry pending",
      "message": "Map geometry is not linked yet, but the parcel summary can still be reviewed."
    }
  ]
}
```

## Sample Quote Payload

```json
{
  "county_id": "harris",
  "tax_year": 2026,
  "requested_tax_year": 2026,
  "served_tax_year": 2026,
  "tax_year_fallback_applied": false,
  "tax_year_fallback_reason": null,
  "data_freshness_label": "current_year",
  "account_number": "1001001001001",
  "address": "101 Main St, Houston, TX 77002",
  "defensible_value_point": 320000,
  "expected_tax_savings_point": 975,
  "protest_recommendation": "file_protest",
  "explanation_bullets": [
    "Comparable evidence supports a lower value."
  ]
}
```
