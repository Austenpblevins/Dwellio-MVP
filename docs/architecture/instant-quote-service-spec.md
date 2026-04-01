# Instant Quote Service Spec

The Stage 17 instant quote service is an additive public read path for Dwellio's lead funnel.

Canonical route:
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}`

Boundary rules:
- keep `GET /search`, `GET /parcel`, and refined `GET /quote` contracts unchanged
- keep instant quote separate from the refined defensible-value quote engine
- use `instant_quote_subject_cache` as the request-time serving layer, rebuilt from `instant_quote_subject_view`
- use precomputed `instant_quote_neighborhood_stats` and `instant_quote_segment_stats`
- reuse `parcel_summary_view`, `parcel_effective_tax_rate_view`, and parcel-year fallback behavior
- do not expose raw confidence scores, target assessed value, target PSF, or internal diagnostics publicly

Runtime flow:
1. resolve parcel-year identity through `instant_quote_subject_cache`
2. apply deterministic prior-year fallback when exact year is unavailable
3. load neighborhood and segment assessed-PSF stats
4. blend segment and neighborhood medians when support allows
5. apply light deterministic size and age adjustments
6. translate the resulting assessment-basis estimate into a public savings range
7. constrain or suppress numeric output when tax protections or low confidence make public precision unsafe

Assessment basis:
- use the canonical basis order already embedded in the parcel stack:
  - `certified_value`
  - `appraised_value`
  - `assessed_value`
  - `market_value`
  - `notice_value`

Fallback tiers:
- `segment_within_neighborhood`
- `neighborhood_only`
- `unsupported`

Public behavior:
- returns `200` with `supported=false` when the parcel resolves but instant quote support is unsafe or incomplete
- returns `404` when the parcel-year identity cannot be resolved for the requested year or any prior year

Observability:
- request-path structured logs
- background best-effort inserts into `instant_quote_request_logs`
- refresh history persisted in `instant_quote_refresh_runs`
- readiness counts exposed through county-year readiness derived fields from the serving artifacts
