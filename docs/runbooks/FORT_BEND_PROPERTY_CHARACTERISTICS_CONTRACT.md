# Fort Bend Property Characteristics Contract

Fort Bend property characteristics are now derived through the normal upstream prep, normalize, and publish pipeline.

This contract is intentionally conservative:

- `bedrooms`, `half_baths`, `stories`, and `pool_flag` are supported
- `full_baths` remains unsupported and stays null
- `total_rooms` remains unsupported and stays null
- `garage_spaces` remains unsupported and stays null
- `quarter_baths` is reviewed but not promoted into the canonical schema

Related additive contract:

- valuation consumers may use the separate Fort Bend derived bathroom layer documented in [FORT_BEND_DERIVED_BATHROOM_FEATURES.md](/Users/nblevins/Desktop/dwellio/docs/runbooks/FORT_BEND_DERIVED_BATHROOM_FEATURES.md)
- that layer is additive and does not change canonical Fort Bend `full_baths`

## Authoritative raw files

Primary Fort Bend files used by the current contract:

- `PropertyExport.txt`: parcel grain and situs fields for the prepared property roll
- `OwnerExport.txt`: owner and market-value fields
- `ExemptionExport.txt`: raw exemption codes
- `WebsiteResidentialSegs.csv`: authoritative residential segment family for supported characteristics
- `Fort Bend_Website_ResidentialSegments.txt`: accepted alternate filename for the same residential-segment contract
- `PropertyDataExport4558080.txt`: authoritative `living_area_sf` source via `SquareFootage`
- `Fort Bend Tax Rate Source.csv`: school-district/entity resolution used during prep

Reviewed but not authoritative for the supported characteristics contract:

- `PropertyDataExport4558084.txt`: useful corroborating segment export, but not sufficient alone because it does not carry `vTSGRSeg_PoolValue`
- `PropertyDataExport4558083.txt`: improvement export reviewed during audit, but it does not provide a safer exact full-bath or garage-space contract than the residential segment family

Canonical source-path policy:

- Preferred long-term operator layout is the flattened Fort Bend raw root under `~/county-data/<tax_year>/raw/fort_bend/`
- `WebsiteResidentialSegs.csv` is the canonical filename
- `Fort Bend_Website_ResidentialSegments.txt` is an accepted backward-compatible alias
- If the residential segment file does not contain the required characteristics columns, prep fails loudly

## Supported fields

### `bedrooms`

Authoritative source:

- primary residential improvement `fBedrooms`

Rule:

- group residential segment rows by `QuickRefID` and `vTSGRSeg_ImpNum`
- select one primary residential improvement per parcel
- use the maximum non-negative `fBedrooms` value found on that selected improvement's main-area segment family

### `half_baths`

Authoritative source:

- primary residential improvement `fNumHalfBath`

Rule:

- group by the selected primary residential improvement
- use the maximum non-negative `fNumHalfBath`
- if the selected improvement has the residential main-area segment family but no explicit half-bath value, normalize to `0`
- if the source emits an invalid negative half-bath value, do not promote it

Why zero fill is allowed:

- Fort Bend uses both explicit `0` and blank `fNumHalfBath` values on otherwise populated main-area rows
- this contract treats blank-on-selected-main-area as "no half bath reported" rather than a second unsupported state

### `stories`

Authoritative source:

- primary residential improvement segment types `MA`, `MA2`, `MA3`, `MA4`

Rule:

- count unique explicit story-level segment codes on the selected primary residential improvement
- `MA` = first story
- `MA2` = second story
- `MA3` = third story
- `MA4` = fourth story

Conservative exclusions:

- `MA1.5` and `MAA` are not promoted into integer `stories`
- they may help primary-improvement selection, but they do not imply a safe canonical whole-number story count

### `pool_flag`

Authoritative source:

- any residential-segment row with non-zero `vTSGRSeg_PoolValue`
- or explicit pool segment type `RP`

Rule:

- pool is parcel-level, not primary-improvement-only
- if any segment row carries a non-zero pool value or `RP`, canonical `pool_flag = true`
- otherwise canonical `pool_flag = false` when residential segment evidence exists

Supported pool codes/signals:

- non-zero `vTSGRSeg_PoolValue`
- segment type `RP`

Not treated as sufficient on their own:

- `SPA`
- accessory or misc segment types without a direct pool signal

## Primary residential improvement selection

Fort Bend multi-improvement parcels are handled explicitly to avoid count inflation.

Selection order:

1. largest summed main-area square footage across the improvement's `MA` / `MA1.5` / `MA2` / `MA3` / `MA4` / `MAA` segment family
2. more explicit story-level segments (`MA`, `MA2`, `MA3`, `MA4`)
3. lower numeric `vTSGRSeg_ImpNum`

Why this matters:

- some parcels contain more than one residential improvement candidate
- parcel-wide max or parcel-wide union logic can overstate bedrooms, half baths, or stories
- this contract selects one primary residential improvement for room-story characteristics and keeps pool parcel-level

## Explicit non-go decisions

### `full_baths`

Status:

- intentionally unsupported

Why:

- the observed `fPlumbing` / `Plumbing` values are numeric, but they are not proven to mean exact full-bath count
- the same `fPlumbing` value appears across materially different half-bath patterns
- no safe deterministic formula from `fPlumbing`, `fNumHalfBath`, and `fNumQuaterBath` was established

Current contract:

- canonical `full_baths` remains null
- no heuristic estimate is applied

### `total_rooms`

Status:

- intentionally unsupported

Why:

- `fRooms` is effectively empty in the audited Fort Bend source bundle

### `garage_spaces`

Status:

- intentionally unsupported

Why:

- garage-related segment types such as `AG` and `DG` exist
- they support presence or area-style analysis, but not a safe exact stall count contract

### `quarter_baths`

Status:

- reviewed but not promoted

Why:

- `fNumQuaterBath` exists but is extremely sparse
- there is no established canonical destination field for it today

## Schema impact

No additive database migration was required for this Fort Bend characteristics contract.

Existing canonical fields now used more fully:

- `parcel_improvements.bedrooms`
- `parcel_improvements.half_baths`
- `parcel_improvements.stories`
- `parcel_improvements.pool_flag`
- corresponding `improvements.*` detail fields

Existing canonical fields intentionally left null for Fort Bend:

- `parcel_improvements.full_baths`
- `parcel_improvements.total_rooms`
- `parcel_improvements.garage_spaces`

## Rebuild workflow

Prepare Fort Bend `2026` property-roll output:

```bash
python3 -m infra.scripts.prepare_manual_county_files \
  --county-id fort_bend \
  --tax-year 2026 \
  --dataset-type property_roll \
  --raw-root ~/county-data/2026/raw \
  --ready-root ~/county-data/2026/ready
```

Run the standard backfill pipeline:

```bash
python3 -m infra.scripts.run_historical_backfill \
  --counties fort_bend \
  --tax-years 2026 \
  --dataset-types property_roll \
  --ready-root ~/county-data/2026/ready
```

Refresh and validate Stage 21 quote support:

```bash
python3 - <<'PY'
from app.jobs import job_refresh_instant_quote, job_validate_instant_quote
job_refresh_instant_quote.run(county_id="fort_bend", tax_year=2026)
job_validate_instant_quote.run(county_id="fort_bend", tax_year=2026)
PY
```

## Residual caveats

- prepared-roll coverage across all Fort Bend rows remains lower than quote-cohort coverage because non-residential and sparse records still exist in the county export
- supportable Fort Bend SFR rows now receive the supported characteristics through the normal pipeline
- if Fort Bend introduces a new exact bath-count source later, `full_baths` can be revisited, but the current contract intentionally does not guess
