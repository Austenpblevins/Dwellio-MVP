# Instant Quote V5 Stage 8: Calibration And Cohort Expansion Review

Date: 2026-04-22
Branch: `codex/instant-quote-v5-stage08-calibration-expansion`
Guardrail baseline artifact: `docs/architecture/instant-quote-v5-stage1-guardrail-20260422.json`
Stage 7 handoff artifact: `docs/architecture/instant-quote-v5-stage7-flagged-savings-rollout-20260422.json`
Stage 8 calibration artifact: `docs/architecture/instant-quote-v5-stage8-calibration-20260422.json`

## Scope

Stage 8 reviews the Stage 5, Stage 6, and Stage 7 evidence together and decides whether any cohort should be expanded beyond the Stage 7 flagged rollout.

This stage does not:

- perform a broad public model replacement
- weaken Harris data-quality constraints
- weaken school-limited or refined-review safeguards

## Stage 8 Calibration Changes

Stage 8 makes the rollout more conservative, not broader:

- default `DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES` is now `total_exemption_low_cash`
- translated rollout now requires Stage 8 calibrated state-to-limiting-code alignment
- translated rollout now refuses to raise the public savings estimate above the current public estimate

In practice, that means:

- `total_exemption_low_cash` remains the only default-enabled public translation cohort
- `near_total_exemption_low_cash` stays implemented in code, but is not default-enabled until live rows exist
- broader env configuration alone is no longer enough to turn on translation for uncalibrated states

## Guardrail Result

With Stage 7 translation disabled, the fresh Stage 0 guardrail rerun matched `instant-quote-v5-stage1-guardrail-20260422.json` exactly.

Confirmed unchanged baseline values:

- Harris:
  - `support_rate_all_sfr_flagged = 0.9980266470924625`
  - `missing_assessment_basis = 1345`
- Fort Bend:
  - `support_rate_all_sfr_flagged = 0.9930466513664263`

That means Stage 8 did not introduce an unintended public regression outside enabled cohorts.

## Current Calibrated Rollout

The validated Stage 8 rollout configuration is:

- county ids: `fort_bend`
- rollout states: `total_exemption_low_cash`

Observed live behavior stayed narrow and justified:

- Fort Bend `total_exemption_low_cash`
  - translated rows applied: `3`
  - materially changed rows: `1`
  - key row: `6250-01-004-1600-907` changed from `1050.0 -> 0.0`

No other live cohort was translated under the calibrated default.

## Before / After Calibration Check

To pressure-test expansion risk, Stage 8 reviewed a broader candidate configuration:

- county ids: `harris,fort_bend`
- rollout states:
  - `total_exemption_low_cash`
  - `near_total_exemption_low_cash`
  - `high_opportunity_low_cash`
  - `opportunity_only_tax_profile_incomplete`
  - `school_limited_non_school_possible`
  - `shadow_quoteable_public_refined_review`

Pre-calibration exploratory run:

- Harris `high_opportunity_low_cash`
  - translation applied on `3` sampled rows
  - public estimate changed on `0` rows
- Fort Bend `high_opportunity_low_cash`
  - translation applied on `3` sampled rows
  - public estimate changed on `0` rows

Interpretation:

- broadening the env allowlist alone was enough to move fallback-driven high-opportunity rows onto the translated path
- those translations did not improve the public estimate
- that is not an evidence-backed expansion and should not happen by accident

Post-calibration candidate rerun:

- Harris `high_opportunity_low_cash`
  - translation applied on `0` sampled rows
- Fort Bend `high_opportunity_low_cash`
  - translation applied on `0` sampled rows

Interpretation:

- Stage 8 now blocks non-calibrated cohort expansion in code instead of relying on reviewer memory or env discipline

## Stage 8 Decisions

### `near_total_exemption_low_cash`

Decision:

- keep implemented but not default-enabled

Reason:

- isolated 2026 live rows observed:
  - Harris: `0`
  - Fort Bend: `0`

### Additional Fort Bend expansion

Decision:

- do not expand beyond `total_exemption_low_cash`

Reason:

- `school_limited_non_school_possible` rows remain unsupported with `tax_limitation_uncertain`
- `shadow_quoteable_public_refined_review` rows remain manual-review cases
- `high_opportunity_low_cash` broadening produced no public estimate improvement in the sampled live rows

### Harris expansion

Decision:

- Harris remains constrained

Reason:

- candidate high-opportunity rollout would have moved fallback-driven rows onto the translated path without producing better public estimates
- `opportunity_only_tax_profile_incomplete` still lacks enough tax-profile support for public numeric rollout
- Harris `missing_assessment_basis` remains explicit and was not absorbed into broader state logic

### School-limited and refined-review cohorts

Decision:

- still too risky for public flagged rollout

Reason:

- school-limited rows still need directional-only handling, not translated public cash promises
- refined-review shadow-signal rows still require manual review before safe public quote presentation

## Stage 8 Decision

Stage 8 is acceptable as a calibration stage because:

- the Stage 1 guardrail baseline remained intact outside enabled cohorts
- the only default-enabled translated cohort is still the evidence-backed Fort Bend `total_exemption_low_cash` set
- `near_total_exemption_low_cash` remains available for future live validation without being treated as proven today
- Harris, school-limited, and refined-review cohorts remain constrained
- the rollout is now more reversible and harder to broaden accidentally
