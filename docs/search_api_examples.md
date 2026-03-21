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
      "confidence_label": "high"
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
