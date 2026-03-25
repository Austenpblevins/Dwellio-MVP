# Stage 16 Lead Funnel Frontend UX

This note records the public-web UX added in Stage 16 PR 2.

## Public flow

The public web app still uses the canonical public backend routes:
- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

## Funnel shape

- `/` is now the Dwellio quote-funnel landing page
- `/search` selects the parcel-year to review
- `/parcel/{countyId}/{taxYear}/{accountNumber}` is the progressive quote-to-lead page

## UX behavior

- quote-ready parcels show defensible value, expected savings, recommendation, and explanation
- missing quote-ready rows keep the lead path open instead of ending in a dead end
- unsupported county or unsupported property type states are explained clearly
- lead capture stays soft-gated with email required and phone optional
- attribution is stored client-side and submitted through the existing `POST /lead` contract

## Boundaries preserved

- no alternate public search or quote architecture was introduced
- no internal/admin fields are exposed in public UI
- restricted MLS or listing artifacts remain absent from public pages
