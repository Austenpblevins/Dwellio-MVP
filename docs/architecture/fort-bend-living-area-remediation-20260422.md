# Fort Bend Living-Area Remediation

Date: `2026-04-22`

## Summary

Fort Bend `bldg_sqft -> living_area_sf` was remediated on the isolated Stage 21 DB after audit evidence showed the old denominator was behaving like total component area instead of true living area.

The code-side correction now does two things:

- keeps `bldg_sqft` as the prepared Fort Bend living-area value used by canonical mapping
- preserves the prior all-component total as `gross_component_area_sf` plus `living_area_source` in the prepared Fort Bend property-roll output

For immediate isolated-DB validation, canonical `parcel_improvements.living_area_sf` for Fort Bend 2026 was corrected from the official FBCAD `PropertyDataExport ... SquareFootage` file and `job_refresh_instant_quote` was rerun for `fort_bend/2026`.

## Source Of Truth

Authoritative Fort Bend living area used for the remediation:

- official property export field: `SquareFootage`
- file: `/tmp/fbcad_2026_audit/Property Data Export - Redacted/Property DataExport_Property/PropertyDataExport4585723.txt`

The target parcel that triggered the audit still shows the core issue clearly:

- account: `8118-49-006-0150-907`
- CAD id: `R537643`
- address: `1239 Carswell Grove DR, Missouri City, TX 77459`
- official living area: `2023`
- prior Dwellio quote denominator: `2663`
- prior denominator matched all improvement components, not living area

Official reference:

- <https://esearch.fbcad.org/property/view/r537643>

## DB Correction Scope

The isolated Stage 21 correction loaded `289,658` official `SquareFootage` rows.

Matched canonical Fort Bend 2026 parcel-improvement rows:

- `288,155`

Changed Fort Bend 2026 `parcel_improvements.living_area_sf` rows:

- `283,755`

Observed change magnitude across matched rows:

- mean sqft delta: `916.55`
- median sqft delta: `742`

## Before / After Guardrails

County-level instant-quote support guardrails stayed stable after the correction:

- support rate before: `0.9930466513664263`
- support rate after: `0.9930466513664263`
- denominator count before/after: `278,427`
- supportable count before/after: `276,491`
- monitored zero-savings quote share before: `0.5208333333333334`
- monitored zero-savings quote share after: `0.5208333333333334`
- blocker distribution before/after:
  `supportable = 276,491`, `missing_living_area = 1,452`, `missing_effective_tax_rate = 484`

Interpretation:

- the denominator correction materially changed Fort Bend quote magnitudes
- it did not create a county-wide support-rate collapse
- it did not create a new zero-share spike in the monitored baseline

## Target Parcel Impact

Target parcel before correction:

- living area: `2663`
- subject assessed PSF: `155.67067217423957`
- target PSF: `142.29073046497814`
- equity value estimate: `378920.2152282368`
- reduction estimate: `35630.784771763196`
- public savings estimate: `1048.0439032766426`

Target parcel after correction:

- living area: `2023`
- subject assessed PSF: `204.91893227879387`
- target PSF: `175.99766889172835`
- equity value estimate: `356043.28416796646`
- reduction estimate: `58507.715832033544`
- public savings estimate: `1720.9459534834346`

Interpretation:

- the target parcel moved in the expected direction
- corrected living area increased both subject PSF and neighborhood target PSF
- the final Fort Bend quote became more conservative on value but stronger on reduction signal

## 55-Parcel Live Sample

The same 55-parcel Fort Bend audit sample was rerun after the correction.

Key outcomes:

- exact official living-area match after correction: `55 / 55`
- sample mean living-area drop: `920.18` sqft
- sample median living-area drop: `742` sqft
- mean subject assessed PSF change: `+37.93%`
- mean target PSF change: `+32.81%`
- mean savings delta: `+86.18`
- median savings delta: `0`
- rows with `|savings delta| >= 100`: `28`
- rows with `|savings delta| >= 500`: `12`
- rows with `|savings delta| >= 1000`: `5`
- zero-to-positive savings shifts: `8`
- positive-to-zero savings shifts: `7`
- support flips in sample: `1`

The one sample support flip was:

- account `0221-00-000-0970-906`
- before: supported with `$0`
- after: `low_confidence_refined_review`
- after raw reduction/savings signal increased materially, but confidence dropped below the public threshold

Interpretation:

- the correction improves denominator truth
- quote outputs moved materially, so this was not a cosmetic fix
- some parcels now surface stronger but less stable protest signal and should remain refined-review constrained

## Rollout Posture

Recommended Fort Bend posture after this remediation:

- base instant quote: `acceptable with caveat after correction`
- translated V5 savings rollout: `keep disabled until recalibrated`

Why translated rollout should stay disabled:

- Stage 5 through Stage 8 Fort Bend shadow/state/flagged rollout artifacts were calibrated on the old denominator
- the denominator correction changed Fort Bend quote magnitudes enough that those rollout decisions should be rerun before any translated Fort Bend expansion resumes
- the current default feature flag posture already keeps translated rollout off, which is still the right posture

## Files

- code fix: [prepare_manual_county_files.py](/Users/nblevins/Desktop/dwellio/infra/scripts/prepare_manual_county_files.py)
- parser update: [parse.py](/Users/nblevins/Desktop/dwellio/app/county_adapters/fort_bend/parse.py)
- remediation helper: [apply_fort_bend_living_area_correction.py](/Users/nblevins/Desktop/dwellio/infra/scripts/apply_fort_bend_living_area_correction.py)
- structured artifact: [fort-bend-living-area-remediation-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/fort-bend-living-area-remediation-20260422.json)
