# Instant Quote V5 Stage 7: Feature-Flagged Savings Translation Rollout

Date: 2026-04-22
Branch: `codex/instant-quote-v5-stage07-flagged-savings-rollout`
Guardrail baseline artifact: `docs/architecture/instant-quote-v5-stage1-guardrail-20260422.json`
Stage 6 handoff artifact: `docs/architecture/instant-quote-v5-stage6-product-state-rollout-20260422.json`
Stage 7 rollout artifact: `docs/architecture/instant-quote-v5-stage7-flagged-savings-rollout-20260422.json`

## Scope

Stage 7 adds a feature-flagged savings-translation path that can replace the public savings estimate with the Stage 5 summary tax-profile-aware value for explicitly enabled cohorts only.

Public default behavior remains:

- `reduction_estimate * effective_tax_rate`

Stage 7 does not:

- globally replace the public savings model
- weaken county capability limitations
- weaken Harris `missing_assessment_basis` visibility
- promote fallback-driven profiles into high-certainty outputs

## Feature Flag Configuration

New settings:

- `DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED`
- `DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS`
- `DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES`

The validation run used:

- enabled: `true`
- county ids: `fort_bend`
- rollout states: `total_exemption_low_cash,near_total_exemption_low_cash`

This means the Stage 7 rollout was intentionally limited to Fort Bend exemption-limited low-cash cohorts only.

## Additive Persistence

Migration `0063_stage24_instant_quote_savings_translation_rollout.sql` adds additive request-log fields:

- `savings_translation_mode`
- `savings_translation_reason_code`
- `savings_translation_applied_flag`
- `selected_public_savings_estimate_raw`

These fields make the Stage 7 cohort switch inspectable without changing the public API shape.

## Guardrail Result

With `DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED=false`, the fresh guardrail rerun matched `instant-quote-v5-stage1-guardrail-20260422.json` exactly.

Confirmed unchanged baseline values:

- Harris:
  - `support_rate_all_sfr_flagged = 0.9980266470924625`
  - blocker distribution:
    - `supportable = 1169298`
    - `missing_assessment_basis = 1345`
    - `missing_effective_tax_rate = 61`
    - `missing_living_area = 900`
    - `missing_neighborhood_code = 6`
- Fort Bend:
  - `support_rate_all_sfr_flagged = 0.9930466513664263`
  - blocker distribution:
    - `supportable = 276491`
    - `missing_living_area = 1452`
    - `missing_effective_tax_rate = 484`

## Rollout Findings

### Enabled Cohort: Fort Bend `total_exemption_low_cash`

Sampled accounts:

- `3531-04-001-0410-914`
- `5016-01-001-0170-914`
- `6250-01-004-1600-907`

Observed behavior:

- all three rows remained in `public_rollout_state = total_exemption_low_cash`
- all three rows switched to `savings_translation_mode = v5_shadow_tax_profile_rollout`
- `3531-04-001-0410-914` stayed at `0.0 -> 0.0`
- `5016-01-001-0170-914` stayed at `0.0 -> 0.0`
- `6250-01-004-1600-907` changed from `1050.0 -> 0.0`

Interpretation:

- the Stage 7 rollout did real work where the old path was materially overstating likely current-year cash savings
- the translated result stayed paired with the explicit Stage 6 low-cash / exemption-limited messaging
- the translated output still carried fallback-profile uncertainty disclaimers

### Held Back: Fort Bend `school_limited_non_school_possible`

Sampled accounts:

- `5683-04-001-0340-907`
- `6250-07-004-0110-907`
- `0057-00-860-0001-908`

Observed behavior:

- all rows remained unsupported with `tax_limitation_uncertain`
- all rows stayed on `savings_translation_mode = current_public_formula`
- no public savings output was introduced

Interpretation:

- this cohort remains too uncertain for Stage 7 savings translation rollout
- Stage 6 state messaging remains the right behavior for now

### Held Back: Fort Bend `shadow_quoteable_public_refined_review`

Sampled accounts:

- `2545-02-001-0300-907`
- `0029-00-000-1186-901`

Observed behavior:

- both rows remained unsupported with `low_confidence_refined_review`
- both rows stayed on `savings_translation_mode = current_public_formula`
- no public quote promotion occurred

Interpretation:

- even though the internal shadow path shows analytical signal, Stage 7 correctly did not broaden rollout into low-confidence manual-review cases

### Held Back: Harris `opportunity_only_tax_profile_incomplete`

Sampled accounts:

- `0420350030006`
- `0860950000017`
- `0860960000018`

Observed behavior:

- all rows stayed on `savings_translation_mode = current_public_formula`
- all rows kept the Stage 6 opportunity-only public messaging
- none received a translated tax-profile-based public savings number

Interpretation:

- the current tax profile is still too incomplete for Stage 7 numeric rollout

### Held Back Controls

- Harris `high_opportunity_low_cash` stayed unchanged on the old path
- Fort Bend `high_opportunity_low_cash` stayed unchanged on the old path
- Harris `missing_assessment_basis` control account `1372390030007` stayed explicitly blocked and did not enter the rollout path

## Stage 7 Decision

Stage 7 is acceptable because:

- the rollout is default-off
- the rollout is limited to an explicitly justified county/state cohort
- only the Fort Bend `total_exemption_low_cash` cohort received translated public savings output
- school-limited, opportunity-only, refined-review, and Harris data-quality cases remained on the old path
- the Stage 1 guardrail baseline remained intact outside enabled cohorts

No `near_total_exemption_low_cash` rows were present in the isolated 2026 Harris or Fort Bend validation set, so that enabled state remains implemented but not live-exercised in this run.
