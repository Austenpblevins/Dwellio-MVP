# Instant Quote Service Spec

The Stage 17 instant quote service is an additive public read path for Dwellio's lead funnel.

Canonical route:
- `GET /quote/instant/{county_id}/{tax_year}/{account_number}`

Boundary rules:
- keep `GET /search`, `GET /parcel`, and refined `GET /quote` contracts unchanged
- keep instant quote separate from the refined defensible-value quote engine
- use `instant_quote_subject_cache` as the request-time serving layer
- rebuild the serving cache from county-year-scoped canonical parcel-year inputs instead of expanding broad derived views on every refresh
- use precomputed `instant_quote_neighborhood_stats` and `instant_quote_segment_stats`
- reuse `parcel_summary_view`, `parcel_effective_tax_rate_view`, and parcel-year fallback behavior
- do not expose raw confidence scores, target assessed value, target PSF, or internal diagnostics publicly

Runtime flow:
1. resolve parcel-year identity through `instant_quote_subject_cache`
2. keep the requested quote year on the parcel-year identity even when tax-rate basis falls back
3. use the refresh-selected tax-rate basis year already stamped into `instant_quote_subject_cache`
4. apply deterministic prior-year parcel-year fallback only when the exact quote year is unavailable
5. load neighborhood and segment assessed-PSF stats
6. blend segment and neighborhood medians when support allows
7. apply light deterministic size and age adjustments
8. translate the resulting assessment-basis estimate into a public savings range
9. constrain or suppress numeric output when tax protections or low confidence make public precision unsafe

Tax-rate basis behavior:
- instant quote may temporarily use the nearest prior usable tax-rate basis year for the current quote year
- this happens automatically inside the refresh path, not through a public product mode
- current-year quotes remain current-year quotes even when the effective tax rate comes from a prior adopted year
- once current-year effective tax-rate coverage is usable, refresh automatically switches back without an annual code change
- internal refresh/readiness metadata separately classifies the selected basis as one of:
  - `prior_year_adopted_rates`
  - `current_year_unofficial_or_proposed_rates`
  - `current_year_final_adopted_rates`
- basis year and basis status are different truths:
  - basis year answers which tax year supplied the effective rate
  - basis status answers whether that basis should be treated internally as prior-year adopted, same-year unofficial/proposed, or same-year final adopted
- same-year rates default to `current_year_unofficial_or_proposed_rates` unless the internal county-year adoption-status metadata explicitly marks them as final adopted
- this classification stays internal to refresh, readiness, validation, and admin surfaces and is not a public quote mode
- requested-year usability is not row-floor only:
  - keep the `20` supportable-subject floor
  - also require strong effective-tax-rate coverage and tax-assignment completeness on the current-year quoteable cohort
  - require measured parcel continuity before prior-year fallback is treated as usable

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
- refresh history includes the selected tax-rate basis year, fallback flag, deterministic reason code, internal basis-status classification, basis-status reason, supportable-subject counts, coverage ratios, and parcel-continuity diagnostics used to justify the choice
- readiness counts exposed through county-year readiness derived fields from the serving artifacts
