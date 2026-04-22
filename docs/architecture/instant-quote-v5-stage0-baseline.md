# Instant Quote V5 Stage 0 Baseline

This note freezes the verified starting state for the V5 rollout before any public quote logic changes.

## Current serving baseline

- `QUOTE_VERSION`: `instant_quote_v5_stage0_baseline`
- Current public savings model: `reduction_estimate * effective_tax_rate`
- V5 feature flag scaffold: `DWELLIO_INSTANT_QUOTE_V5_ENABLED` with default `false`
- Current public contract surfaces that must stay compatible until replacements are validated:
  - `unsupported_reason`
  - `warning_codes`
  - `public_summary_ready_flag`
  - `effective_tax_rate_basis_*`

## Current unsupported and constrained behavior

Current unsupported reasons are exposed through `InstantQuoteResponse.unsupported_reason` and currently include:

- `unsupported_property_type`
- `missing_living_area`
- `missing_assessment_basis`
- `missing_neighborhood_code`
- `missing_effective_tax_rate`
- `instant_quote_not_ready`
- `thin_market_support`
- `low_confidence_refined_review`
- `tax_limitation_uncertain`
- `implausible_savings_outlier`

Current warning-driven public behavior:

- `prior_year_assessment_basis_fallback` adds a public disclaimer when current-year assessed basis falls back to prior year.
- `freeze_without_qualifying_exemption` can suppress public savings through `tax_limitation_uncertain`.
- `assessment_exemption_total_mismatch` can suppress public savings through `tax_limitation_uncertain`.
- `missing_exemption_amount` reduces confidence and is part of the current quality signal.
- `homestead_flag_mismatch` reduces confidence and signals disagreement between assessment and exemption layers.
- `tax_rate_basis_fallback_applied`, `no_usable_tax_rate_basis`, and `tax_rate_basis_current_year_unofficial_or_proposed` are refresh-level telemetry and validation warnings that must remain visible during V5 shadow work.

## Existing baseline metrics to reuse

Stage 0 must reuse `InstantQuoteValidationReport` rather than inventing parallel metrics. The existing report already captures:

- support rate for `all_sfr_flagged`
- support rate for `strict_sfr_eligible`
- denominator-shift alerts
- zero-savings sample counts and zero-share
- blocker distribution
- extreme-savings monitoring

## Latest recorded live-data baseline in repo docs

The most recent recorded accepted shared-rollout baseline currently lives in [STAGE21_ISOLATED_DEV_DB.md](/Users/nblevins/Desktop/dwellio/docs/runbooks/STAGE21_ISOLATED_DEV_DB.md:58).

Recorded values there:

- Fort Bend 2026: support rate `0.9930394682`, zero-share `0.5306122449`
- Harris 2026: support rate `0.9980317582`, zero-share `0.44`

Those values are useful as the last repo-recorded baseline, but they do not replace a fresh isolated-DB validation run for V5 Stage 0.

## Fresh isolated-DB validation

Fresh Stage 0 validation was rerun on April 22, 2026 against the isolated Stage 21 database:

- host: `localhost`
- port: `55442`
- database: `stage21_dev`
- user: `stage21_admin`

Durable JSON capture:

- [instant-quote-v5-stage0-baseline-20260422.json](/Users/nblevins/Desktop/dwellio/docs/architecture/instant-quote-v5-stage0-baseline-20260422.json:1)

Fresh results compared with the repo-recorded reference:

- Harris 2026:
  fresh support rate `0.9976579117176111` vs reference `0.9980317582` (`-0.0003738464823889176`)
  fresh zero-share `0.44` vs reference `0.44` (`0.0`)
  fresh blockers `supportable=1168860, missing_living_area=900, missing_assessment_basis=1783, missing_effective_tax_rate=61`
  reference blockers `supportable=1169298, missing_living_area=900, missing_assessment_basis=1345, missing_effective_tax_rate=61`
  observed shift: `438` rows moved from `supportable` to `missing_assessment_basis`

- Fort Bend 2026:
  fresh support rate `0.9930394681550281` vs reference `0.9930394682` (effectively unchanged)
  fresh zero-share `0.5208333333333334` vs reference `0.5306122449` (`-0.00977891156666666`)
  fresh blockers `supportable=276489, missing_living_area=1452, missing_assessment_basis=2, missing_effective_tax_rate=484`
  reference blockers `supportable=276489, missing_living_area=1452, missing_assessment_basis=2, missing_effective_tax_rate=484`

Stage 0 completion interpretation:

- The isolated-DB baseline script ran successfully for both counties.
- Fresh baseline results are now recorded durably in-repo.
- Harris shows a small support-rate regression driven by higher `missing_assessment_basis` blockers, which should be treated as a follow-up caveat for later stages, not as a reason to fake Stage 0 completion.

## Fresh baseline command

Run this against an isolated Stage 21 database, not the protected baseline DB:

```bash
DWELLIO_DATABASE_URL='postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev' \
python3 -m infra.scripts.report_instant_quote_v5_stage0_baseline \
  --county-ids harris fort_bend \
  --tax-year 2026
```

If that command cannot connect or validation data is stale, Stage 0 is not complete.
