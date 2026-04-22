# Instant Quote V5 Stage 1 Validation

Date: 2026-04-22

Scope:
- add additive typed metadata around `assessment_basis_value`
- preserve current public quote math and `InstantQuoteResponse` shape
- validate against the isolated Stage 21 DB on `localhost:55442/stage21_dev`

Implementation summary:
- migration `0057_stage24_instant_quote_assessment_basis_contract.sql` adds:
  - `assessment_basis_source_value_type`
  - `assessment_basis_source_year`
  - `assessment_basis_source_reason`
  - `assessment_basis_quality_code`
- `app/services/instant_quote.py` now carries those fields internally alongside `assessment_basis_value`
- no public savings translation change was made; the current model remains `reduction_estimate_times_effective_tax_rate`

Public contract guardrails:
- public response model shape was checked in unit tests and the new Stage 1 fields do not appear in `InstantQuoteResponse.model_dump()`
- public quote math was not changed in code
- the Stage 1 code diff added companion metadata and internal telemetry only; it did not change the public savings formula or the `support_blocker_code` CASE expression

Live refresh + validation:
- applied migration `0057` on the isolated Stage 21 DB
- refreshed `fort_bend` 2026 and `harris` 2026 on the isolated DB
- recorded refreshed guardrail output in `instant-quote-v5-stage1-guardrail-20260422.json`

Typed basis metadata population after refresh:
- Harris 2026 `assessment_basis_source_reason` counts:
  - `current_year_appraised`: `1090940`
  - `current_year_assessed`: `6`
  - `prior_year_appraised_fallback`: `437`
  - `prior_year_certified_fallback`: `78132`
  - `missing`: `2095`
- Fort Bend 2026 `assessment_basis_source_reason` counts:
  - `current_year_certified`: `278425`
  - `prior_year_certified_fallback`: `2`

Stage 0 baseline comparison:
- Harris support rate moved from `0.9976579117176111` to `0.9980266470924625` (`+0.0003687353748513811`)
- Harris supportable row count moved from `1168860` to `1169298` (`+438`)
- Harris denominator moved from `1171604` to `1171610` (`+6`)
- Harris blocker distribution moved from:
  - `supportable: 1168860`
  - `missing_living_area: 900`
  - `missing_assessment_basis: 1783`
  - `missing_effective_tax_rate: 61`
- Harris blocker distribution now is:
  - `supportable: 1169298`
  - `missing_living_area: 900`
  - `missing_assessment_basis: 1345`
  - `missing_effective_tax_rate: 61`
  - `missing_neighborhood_code: 6`
- Fort Bend support rate moved from `0.9930394681550281` to `0.9930466513664263` (`+0.0000071832113982539525`)
- Fort Bend supportable row count moved from `276489` to `276491` (`+2`)
- zero-share was unchanged for both counties

Harris `missing_assessment_basis` note:
- the refreshed cache still contains explicit Harris `missing_assessment_basis` rows and they remain tagged with `assessment_basis_source_reason = 'missing'`
- Stage 1 did not redefine that blocker into a new public reason code
- however, the refreshed cache reduced Harris `missing_assessment_basis` from `1783` to `1345`
- the same refreshed cache now contains `437` supportable rows with `prior_year_appraised_fallback`
- because that live-data shift affects supportability, it should be treated as a real rollout caveat before Stage 2 even though Stage 1 did not change the public savings formula

Drift investigation:
- `git diff` between the Stage 0 baseline commit and the Stage 1 commit shows no change to the public savings formula and no change to the `support_blocker_code` CASE expression
- the Stage 1 code change adds typed companion metadata and carries it through cache upsert comparison, but it does not change how `assessment_basis_value` is selected for quote supportability
- Harris post-refresh supportable rows now include:
  - `437` rows with `assessment_basis_source_reason = 'prior_year_appraised_fallback'`
  - `78062` rows with `assessment_basis_source_reason = 'prior_year_certified_fallback'`
- Fort Bend post-refresh supportable rows now include:
  - `2` rows with `assessment_basis_source_reason = 'prior_year_certified_fallback'`
- Harris also picked up `6` denominator rows with `assessment_basis_source_reason = 'current_year_assessed'` that are still blocked by `missing_neighborhood_code`

Interpretation:
- the live drift is real and it changes public supportability behavior, blocker distribution, and served quote eligibility
- the strongest evidence points to a stale-cache correction surfaced by the Stage 1 refresh rather than a new Stage 1 quote-logic change
- the Harris `+438` supportable movement is overwhelmingly explained by previously unmaterialized prior-year basis fallback rows, especially the `437`-row `prior_year_appraised_fallback` cohort
- the Fort Bend `+2` supportable movement is fully explained by the `2`-row `prior_year_certified_fallback` cohort

Recommendation:
- accept the Stage 1 drift as corrected baseline behavior, not as a rollback-worthy Stage 1 logic regression
- do not describe Stage 1 as purely metadata-only in rollout notes, because the refresh did change the live supportable cohort
- use `instant-quote-v5-stage1-guardrail-20260422.json` as the new Stage 1 baseline artifact before any Stage 2 work begins
