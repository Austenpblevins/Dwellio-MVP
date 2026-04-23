# Fort Bend Canonical Living-Area Rebuild Closeout

Date: `2026-04-22`

## Summary

Fort Bend is now fixed through the canonical raw-to-final pipeline, not through a remediation-only database correction.

This rebuild:

- defines `living_area_sf` and `gross_component_area_sf` as separate first-class fields
- uses the official Fort Bend property summary export as the authoritative living-area source
- preserves the all-component segment total separately for auditability
- rebuilds Fort Bend `2026` from the original raw files through adapter-ready prep, historical backfill, parcel warehouse normalization, instant quote refresh, tax-profile materialization, and rollout revalidation

Normal Fort Bend rebuilds should now use the canonical prep + backfill path. The legacy correction script remains only as a backstop for historical repair.

## Canonical Fort Bend area contract

Authoritative Fort Bend area fields:

- `living_area_sf`: quote-facing living area used in PSF math and public quote calculations
- `gross_component_area_sf`: total structural/component area summed from `WebsiteResidentialSegs.csv`
- `living_area_source`: lineage field describing how `living_area_sf` was populated

Authoritative raw living-area source:

- `PropertyDataExport*.txt`
- field: `SquareFootage`

Gross component source:

- `WebsiteResidentialSegs.csv`
- derived as the sum of component/segment area

Why the old behavior was wrong:

- the older Fort Bend path treated total component area as quote-facing living area
- that folded in non-living segments such as attached garage and porch
- the result overstated the PSF denominator and distorted quote math

## Raw-source rebuild inputs

Fort Bend `2026` was rebuilt from these original raw files:

- `/Users/nblevins/county-data/2026/raw/fort_bend/PropertyExport.txt`
- `/Users/nblevins/county-data/2026/raw/fort_bend/OwnerExport.txt`
- `/Users/nblevins/county-data/2026/raw/fort_bend/ExemptionExport.txt`
- `/Users/nblevins/county-data/2026/raw/fort_bend/WebsiteResidentialSegs.csv`
- `/Users/nblevins/county-data/2026/raw/fort_bend/Fort Bend Tax Rate Source.csv`
- `/Users/nblevins/county-data/2026/raw/fort_bend/Fort Bend_Property Data -3-27-2026 - Redacted/PropertyProperty-E/PropertyDataExport4558080.txt`

Canonical adapter-ready outputs written during this rebuild:

- `/tmp/fort_bend_canonical_rebuild_ready/fort_bend_property_roll_2026.csv`
- `/tmp/fort_bend_canonical_rebuild_ready/fort_bend_tax_rates_2026.csv`

Historical backfill result:

- property roll import batch: `01dc6af6-f692-4625-894a-a6a8fc0e5b2a`
- tax rates import batch: `b5f43a89-8b02-4bb5-b85b-167a41a8dcdb`

## Target parcel verification

Target parcel:

- account: `8118-49-006-0150-907`
- address: `1239 Carswell Grove DR, Missouri City, TX 77459`

Canonical warehouse values after rebuild:

- `living_area_sf = 2023`
- `gross_component_area_sf = 2663`
- `living_area_source = property_summary_export`

Quote-facing math after canonical rebuild:

- `subject_assessed_psf = 204.91893227879387`
- `target_psf = 175.99766889172835`
- `equity_value_estimate = 356043.28416796646`
- `reduction_estimate_raw = 58507.715832033544`
- `savings_estimate_raw = 1193.3965067549461`
- public display range: `$825` to `$1,600`
- public midpoint display: `$1,200`

Interpretation:

- the target parcel now uses the canonical Fort Bend living area directly from the rebuilt source pipeline
- the current quote is still supported
- the shadow path matches the current path for this parcel, so the rebuild did not create a new translated-savings discrepancy here

## County-level rebuild effects

Fort Bend Stage 0 baseline after the canonical rebuild:

- `support_rate_all_sfr_flagged = 0.9947849885248198`
- `supportable = 276975`
- `denominator = 278427`
- `missing_living_area = 1452`
- `missing_effective_tax_rate = 0`
- `monitored_zero_savings_quote_share = 0.5208333333333334`
- `tax_rate_basis_status = current_year_unofficial_or_proposed_rates`
- `tax_rate_basis_reason = requested_year_usable`

Compared to the earlier remediation-era Fort Bend baseline:

- supportable count increased by `484`
- support rate improved from `0.9930466513664263` to `0.9947849885248198`
- `missing_effective_tax_rate` blockers dropped from `484` to `0`
- zero-share stayed stable

This means the full raw rebuild fixed two things at once:

1. the living-area denominator is now canonical
2. Fort Bend now also uses the rebuilt current-year tax-rate path rather than the earlier fallback-limited quote-support state

## Rebuilt Stage 5-8 Fort Bend validation

Fresh rebuilt-data artifacts:

- Stage 0: [fort-bend-canonical-rebuild-stage0-baseline-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/fort-bend-canonical-rebuild-stage0-baseline-20260422.json)
- Stage 5: [fort-bend-canonical-rebuild-stage5-shadow-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/fort-bend-canonical-rebuild-stage5-shadow-20260422.json)
- Stage 6: [fort-bend-canonical-rebuild-stage6-rollout-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/fort-bend-canonical-rebuild-stage6-rollout-20260422.json)
- Stage 7: [fort-bend-canonical-rebuild-stage7-flagged-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/fort-bend-canonical-rebuild-stage7-flagged-20260422.json)
- Stage 8: [fort-bend-canonical-rebuild-stage8-calibration-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/fort-bend-canonical-rebuild-stage8-calibration-20260422.json)

Key rebuilt-data findings:

- Stage 5 still shows meaningful total-exemption low-cash separation, but the max absolute shadow delta dropped from `1072.7895362146855` to `765.9929821069854`.
- Stage 6 state counts shifted only slightly:
  - `total_exemption_low_cash`: `8117 -> 8120`
  - `school_limited_non_school_possible`: `2442 -> 2444`
  - `opportunity_only_tax_profile_incomplete`: `11 -> 0`
- Stage 7 changed materially versus the earlier remediation branch:
  - old Fort Bend total-exemption cohort: `applied_count = 3`, `changed_count = 1`
  - rebuilt Fort Bend total-exemption cohort: `applied_count = 0`, `changed_count = 0`
- Stage 8 still rejects broader cohort expansion, and on rebuilt data it now also shows no active translated public estimate changes even inside the narrow total-exemption cohort

Why Stage 7 no longer applies translated savings on rebuilt data:

- the rebuilt-data Stage 8 gating kept the cohort constrained
- the flagged path returned `savings_translation_reason_code = shadow_tax_profile_not_quoteable` for the sampled total-exemption rows
- public state messaging remains honest, but translated public savings does not actually apply on the rebuilt Fort Bend data

## Final rollout posture

Fort Bend after the canonical rebuild should be treated as:

- base quote path: `acceptable`
- translated rollout: `keep disabled`

Explicit posture:

- base Fort Bend quote path is now safe to operate on the rebuilt canonical denominator
- `total_exemption_low_cash` should no longer be treated as an active translated-rollout cohort on rebuilt data
- `near_total_exemption_low_cash` remains implemented but dormant
- `school_limited_non_school_possible` remains disabled
- `manual_review_shadow_quoteable` remains disabled
- `high_opportunity_low_cash` remains disabled
- `opportunity_only_tax_profile_incomplete` remains disabled

This is stricter than the earlier remediation branch, and it is the safer answer.

## Operational conclusion

Fort Bend is now fully closed for scale on the data-pipeline side:

- the rebuild is canonical
- no patch-style database correction is required for the normal workflow
- the county contract is now explicit in prep, adapter parsing, warehouse persistence, and docs

Fort Bend is not broadly closed for translated-savings rollout:

- keep translated rollout disabled unless a future corrected-data validation produces new evidence
- any future expansion must start from the rebuilt artifacts above, not the earlier remediation-era Fort Bend artifacts

## Files changed in this canonical rebuild body of work

- prep workflow: [prepare_manual_county_files.py](/Users/nblevins/Desktop/dwellio/infra/scripts/prepare_manual_county_files.py)
- Fort Bend parser: [parse.py](/Users/nblevins/Desktop/dwellio/app/county_adapters/fort_bend/parse.py)
- canonical field mapping: [field_mappings.yaml](/Users/nblevins/Desktop/dwellio/config/counties/fort_bend/field_mappings.yaml)
- warehouse persistence: [repository.py](/Users/nblevins/Desktop/dwellio/app/ingestion/repository.py)
- additive schema: [0064_fort_bend_canonical_living_area_contract.sql](/Users/nblevins/Desktop/dwellio/app/db/migrations/0064_fort_bend_canonical_living_area_contract.sql)
- Fort Bend adapter doc: [fort_bend_adapter.md](/Users/nblevins/Desktop/dwellio/docs/fort_bend_adapter.md)
- prep runbook: [MANUAL_COUNTY_FILE_PREP.md](/Users/nblevins/Desktop/dwellio/docs/runbooks/MANUAL_COUNTY_FILE_PREP.md)
- legacy backstop helper: [apply_fort_bend_living_area_correction.py](/Users/nblevins/Desktop/dwellio/infra/scripts/apply_fort_bend_living_area_correction.py)
