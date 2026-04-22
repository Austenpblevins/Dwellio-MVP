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

## How the flow works

1. The visitor enters an address or account number on `/`.
2. `/search?address=...` resolves the public parcel candidates from `v_search_read_model`.
3. The selected parcel opens the parcel-year route.
4. The parcel route reads `parcel_summary_view` and, when available, `v_quote_read_model` plus the canonical quote explanation route.
5. The parcel page shows a value-first quote summary when a quote row exists, or a graceful unsupported/pending state when it does not.
6. The visitor can submit the existing `POST /lead` contract from the same parcel-year page without leaving the public flow.

## UX behavior

- quote-ready parcels show defensible value, expected savings, recommendation, and explanation
- missing quote-ready rows keep the lead path open instead of ending in a dead end
- unsupported county or unsupported property type states are explained clearly
- lead capture stays soft-gated with email required and phone optional
- attribution is stored client-side and submitted through the existing `POST /lead` contract

## Supported scope

- counties: `harris`, `fort_bend`
- property scope: `sfr`
- quote request path: precomputed public-safe read models only

## Unsupported handling

- unsupported county: the parcel page explains county rollout limits and still allows email capture
- unsupported property type: the parcel page explains the current SFR-only boundary and still allows email capture
- missing quote-ready row: parcel facts remain available and the lead flow offers an email notification path instead of a dead end
- prior-year fallback: parcel/quote surfaces must disclose the served year and fallback reason instead of pretending the result is current-year
- system/config failure: public pages render an explicit configuration/reachability error instead of pretending the quote exists or translating the failure into an unsupported-demand state

## Known limitations

- first name is collected in the web form for user messaging, but the current backend lead contract does not persist a dedicated `first_name` field yet
- the public funnel is parcel-page based; there is no separate alternate quote route or modal architecture
- quote completeness still depends on county-year readiness and whether a `v_quote_read_model` row exists for the parcel-year
- the web app stores attribution in session storage and submits it through the existing lead route; there is no separate frontend analytics service in this stage

## Boundaries preserved

- no alternate public search or quote architecture was introduced
- no internal/admin fields are exposed in public UI
- restricted MLS or listing artifacts remain absent from public pages
