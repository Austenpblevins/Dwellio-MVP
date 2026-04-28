# Fort Bend Derived Bathroom Features

This contract is an additive Fort Bend valuation layer.

It does **not** rewrite canonical Fort Bend `full_baths`.
Canonical Fort Bend public/source-of-truth behavior remains:

- `parcel_improvements.full_baths` stays null
- `parcel_summary_view.full_baths` stays null
- public quote behavior is unchanged by this layer

## Why this layer exists

Fort Bend residential segments expose bathroom-related source signals that are useful for valuation, but not strong enough to rewrite canonical bathroom truth across every row.

This layer preserves that distinction by:

- keeping canonical truth conservative
- materializing a separate valuation-ready bathroom feature row
- preserving ambiguity and quarter-bath cases explicitly

## Storage

The derived rows are persisted in:

- `fort_bend_valuation_bathroom_features`

Feature-engineering payload builders can also carry a nested:

- `valuation_bathroom_features`

That nested payload is additive and mirrors the derived bathroom record when a valuation consumer chooses to include this layer.

## Authoritative source files

Accepted source files:

- `~/county-data/<tax_year>/raw/fort_bend/WebsiteResidentialSegs.csv`
- `~/county-data/<tax_year>/raw/fort_bend/Fort Bend_Website_ResidentialSegments.txt`

`PropertyDataExport4558084.txt` is useful for audit work, but this derived bathroom layer reads the residential-segment file because it already contains:

- `QuickRefID`
- `PropertyNumber`
- `vTSGRSeg_ImpNum`
- `fSegType`
- `fPlumbing`
- `fNumHalfBath`
- `fNumQuaterBath`

## Primary improvement selection

The same deterministic Fort Bend primary residential improvement rule used by the upstream characteristics contract is reused here.

Selection order:

1. largest summed main-area square footage across `MA`, `MA1.5`, `MA2`, `MA3`, `MA4`, `MAA`
2. richer explicit story structure across `MA`, `MA2`, `MA3`, `MA4`
3. lower numeric `vTSGRSeg_ImpNum`

Rule version:

- `fort_bend_primary_residential_improvement_v1`

## Derived fields

Each row stores:

- county / tax year / parcel linkage
- `quick_ref_id`
- `selected_improvement_number`
- `plumbing_raw`
- `half_baths_raw`
- `quarter_baths_raw`
- `full_baths_derived`
- `half_baths_derived`
- `quarter_baths_derived`
- `bathroom_equivalent_derived`
- `bathroom_count_status`
- `bathroom_count_confidence`
- `bathroom_flags`
- raw distinct-value arrays for auditability

## Normalization rules

Normalization rule version:

- `fort_bend_bathroom_features_v1`

### `exact_supported`

Requirements:

- one nonnegative integer plumbing value
- zero or one nonnegative integer half-bath value
- no positive quarter-bath signal
- no conflicting multiple values

Derivation:

- `full_baths_derived = plumbing_raw`
- `half_baths_derived = half_baths_raw or 0`
- `quarter_baths_derived = 0`
- `bathroom_equivalent_derived = full + 0.5 * half`
- confidence `high`

### `reconciled_fractional_plumbing`

Requirements:

- one plumbing value ending in `.5`
- no positive quarter-bath signal
- no conflicting multiple values

Derivation:

- `full_baths_derived = floor(plumbing_raw)`
- `half_baths_derived = max(raw_half_baths, 1)`
- `quarter_baths_derived = 0`
- `bathroom_equivalent_derived = plumbing_raw`
- confidence `medium`

Flags:

- `fractional_plumbing_source`
- `half_bath_imputed_from_fractional_plumbing` when the half bath had to be inferred

### `quarter_bath_present`

Requirements:

- one nonnegative integer plumbing value
- positive quarter-bath value
- no conflicting multiple values

Derivation:

- keep `quarter_baths_derived` explicit
- do not collapse quarter baths into canonical full baths
- `bathroom_equivalent_derived = full + 0.5 * half + 0.25 * quarter`
- confidence `medium`

Flag:

- `quarter_bath_present`

### `ambiguous_bathroom_count`

Used when the selected improvement has conflicting or impossible source state, including:

- multiple plumbing values
- multiple half-bath values
- multiple quarter-bath values
- invalid negative values
- unsupported fractional combinations

Behavior:

- preserve raw values and flags
- leave `full_baths_derived` null unless already safely derivable
- confidence `low`

### `incomplete_bathroom_count`

Used when some bathroom source exists, but the selected improvement lacks enough non-conflicting data to derive a safe full-bath result.

Current Fort Bend 2026 examples are rare to nonexistent, but the state is kept explicit for future refreshes.

Behavior:

- keep any safe singular `half_baths_derived` / `quarter_baths_derived`
- leave `full_baths_derived` null
- confidence `low`

### `no_bathroom_source`

Used when the selected improvement has no plumbing, half-bath, or quarter-bath source values at all.

This includes source-present Fort Bend accounts where only accessory or non-characteristic segment rows are present for the selected improvement. Those rows are materialized explicitly instead of being dropped so valuation consumers can distinguish "present in source, but no usable bathroom signal" from "absent from the residential segment source entirely."

Behavior:

- all derived bath counts remain null
- confidence `none`
- `bathroom_flags` may include:
  - `source_present_without_characteristic_segment`
  - `selected_improvement_without_characteristic_segment`

## Canonical non-go decision

Canonical Fort Bend `full_baths` remains intentionally unsupported.

Why:

- `fPlumbing` is useful enough for valuation-derived features
- it is still not proven strong enough to become canonical parcel truth across all rows
- quarter-bath and fractional cases need explicit flags, not silent collapse

## Rebuild workflow

Apply migrations first, then materialize the derived bathroom layer on the isolated Stage 21 database:

```bash
DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev \
DWELLIO_ENV=stage21_dev \
python3 -m infra.scripts.run_migrations
```

```bash
DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev \
DWELLIO_ENV=stage21_dev \
python3 - <<'PY'
from app.jobs import job_features

job_features.run(county_id="fort_bend", tax_year=2026)
PY
```

`job_features` remains a generic job entrypoint. Counties without a county-specific additive feature materialization currently no-op after schema readiness checks rather than raising.

## Consumption guidance

Valuation and model work should prefer:

- `fort_bend_valuation_bathroom_features`
- or a consumer-owned feature payload that nests `valuation_bathroom_features` from this table

Public/read-model consumers should continue to treat canonical Fort Bend `full_baths` as unsupported.
