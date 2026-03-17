# API Contracts (Final Reconciled)

Public APIs:
- GET /search?address={query}
- GET /quote/{county_id}/{tax_year}/{account_number}
- GET /quote/{county_id}/{tax_year}/{account_number}/explanation
- POST /lead

Backed by:
- `v_search_read_model`
- `v_quote_read_model`
