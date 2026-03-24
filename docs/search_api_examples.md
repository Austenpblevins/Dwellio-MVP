# Search API Examples

Stage 11 adds the public-facing search and parcel-summary read path on top of `search_documents`, `v_search_read_model`, and `parcel_summary_view`.

## Endpoints

- `GET /search?address={query}`
- `GET /search/autocomplete?query={query}`
- `GET /parcel/{county_id}/{tax_year}/{account_number}`

## Search Result Payload

```json
{
  "results": [
    {
      "county_id": "harris",
      "tax_year": 2026,
      "account_number": "1001001001001",
      "parcel_id": "11111111-1111-1111-1111-111111111111",
      "address": "101 Main St, Houston, TX 77002",
      "situs_zip": "77002",
      "owner_name": "Alex Example",
      "match_basis": "address_exact",
      "match_score": 0.98,
      "confidence_label": "very_high"
    }
  ]
}
```

## Autocomplete Payload

```json
{
  "suggestions": [
    {
      "county_id": "harris",
      "tax_year": 2026,
      "account_number": "1001001001001",
      "parcel_id": "11111111-1111-1111-1111-111111111111",
      "address": "101 Main St, Houston, TX 77002",
      "situs_zip": "77002",
      "owner_name": "Alex Example",
      "match_basis": "address_prefix",
      "match_score": 0.99,
      "confidence_label": "high"
    }
  ]
}
```

## Parcel Summary Payload

```json
{
  "county_id": "harris",
  "tax_year": 2026,
  "account_number": "1001001001001",
  "parcel_id": "11111111-1111-1111-1111-111111111111",
  "address": "101 Main St, Houston, TX 77002",
  "owner_name": "Alex Example",
  "property_type_code": "sfr",
  "living_area_sf": 2100.0,
  "market_value": 450000.0,
  "assessed_value": 350000.0,
  "notice_value": 350000.0,
  "exemption_value_total": 100000.0,
  "effective_tax_rate": 0.021,
  "estimated_annual_tax": 5145.0,
  "completeness_score": 90.0,
  "warning_codes": [],
  "public_summary_ready_flag": true
}
```

## Rebuild Process

The search index rebuild path uses the SQL helper introduced in Stage 11:

```sql
SELECT dwellio_refresh_search_documents(NULL, NULL);
```

Python services can trigger the same rebuild through `SearchIndexService.rebuild_search_documents(...)`.

## Match-Basis Labels

Public search results return a stable `match_basis` label so ranking remains explainable without exposing internal debug details.

Current labels used by the public search path:

- `account_exact`
- `address_exact`
- `address_trigram`
- `search_text_trigram`
- `owner_fallback`
- `account_prefix`
- `address_prefix`
- `owner_prefix`

The public contract is:

- return the best-ranked public-safe parcel candidates
- keep ranking deterministic
- expose `match_basis`, `match_score`, and `confidence_label`
- do not expose internal score components or matched-field details

## Confidence Logic

`confidence_label` is deterministic and basis-aware.

Examples:

- `account_exact` and `address_exact` -> `very_high`
- `address_prefix` and strong `account_prefix` matches -> `high`
- `address_trigram` and `search_text_trigram` depend on score thresholds
- `owner_fallback` is capped lower than exact address/account matches

This prevents broad fallback matches from looking as strong as exact parcel/address matches even when raw similarity scores are close.

## Internal Inspectability

For admin/debug use only, Dwellio exposes:

- `GET /admin/search/inspect?query={query}&limit={n}`

This internal route returns:

- normalized query forms
- candidate ranking order
- `confidence_reasons`
- matched fields
- score components such as address similarity, search-text similarity, and owner similarity

This route is intentionally separate from the public `/search` contract.

## Public vs Internal Boundary

Public `/search` and `/search/autocomplete` responses do **not** include:

- `confidence_reasons`
- matched-field arrays
- score-component breakdowns
- internal debug ranking details

Those details are internal-only and should remain on admin/debug surfaces.
