# Instant Quote V5 Stage 6: Product-State Rollout

Date: 2026-04-22
Branch: `codex/instant-quote-v5-stage06-product-state-rollout`
Guardrail baseline artifact: `docs/architecture/instant-quote-v5-stage1-guardrail-20260422.json`
Stage 5 handoff artifact: `docs/architecture/instant-quote-v5-stage5-shadow-comparison-20260422.json`
Stage 6 rollout artifact: `docs/architecture/instant-quote-v5-stage6-product-state-rollout-20260422.json`

## Scope

Stage 6 adds state-aware quote presentation without replacing the public savings formula.

Public savings math remains:

- `reduction_estimate * effective_tax_rate`

Stage 6 uses the existing Stage 2 internal taxonomy plus Stage 5 shadow signals to change presentation only through:

- explanation summary
- limitation note
- estimate tax-protection note where relevant
- `next_step_cta`

No public response fields were added or removed in this stage.

## Additive Persistence

Migration `0062_stage24_instant_quote_product_state_rollout.sql` adds the following request-log fields:

- `public_rollout_state`
- `public_rollout_reason_code`
- `public_rollout_applied_flag`

These fields make Stage 6 behavior inspectable without changing the public savings model.

## Guardrail Result

The fresh Stage 6 guardrail rerun matched `instant-quote-v5-stage1-guardrail-20260422.json` exactly.

That means Stage 6 did not change:

- public savings math
- supportability behavior
- Harris `missing_assessment_basis` handling

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

## State Rollout Behavior

### Harris 2026

Target shadow-state counts remained explicit:

- `opportunity_only_tax_profile_incomplete = 88`
- `total_exemption_low_cash = 27402`
- `near_total_exemption_low_cash = 0`
- `school_limited_non_school_possible = 0`

Observed public rollout cohorts:

- `high_opportunity_low_cash`
  - sampled accounts:
    - `1097810000030`
    - `1216740030008`
    - `1163970050013`
  - rollout behavior:
    - public quote remains supported
    - public estimate stays unchanged
    - summary now says the parcel shows protest opportunity but likely modest current-year cash
    - `next_step_cta` now explicitly points to refined review
- `opportunity_only_tax_profile_incomplete`
  - sampled accounts:
    - `0420350030006`
    - `0860950000017`
    - `0860960000018`
  - rollout behavior:
    - public quote remains supported
    - public estimate stays at the existing value, including `$0` where applicable
    - summary now frames the parcel as an opportunity signal rather than a reliable cash-savings quote
    - limitation note explicitly says the current tax profile is too limited for a confident promise

Interpretation:

- Harris Stage 6 makes opportunity-only and low-cash posture more honest without changing supportability or weakening the explicit `missing_assessment_basis` blocker.

### Fort Bend 2026

Target shadow-state counts remained explicit:

- `total_exemption_low_cash = 8117`
- `school_limited_non_school_possible = 2442`
- `opportunity_only_tax_profile_incomplete = 11`
- `near_total_exemption_low_cash = 0`

Observed public rollout cohorts:

- `total_exemption_low_cash`
  - sampled accounts:
    - `3531-04-001-0410-914`
    - `5016-01-001-0170-914`
    - `6250-01-004-1600-907`
  - rollout behavior:
    - public quotes remain supported
    - public savings math remains unchanged
    - summary and note now explicitly say exemptions likely absorb most or all current-year cash savings
    - `6250-01-004-1600-907` is the clearest Stage 5 carry-forward example: public midpoint stayed `1050.0`, but Stage 6 now publicly frames it as a low-cash, exemption-limited opportunity instead of a clean cash quote
- `school_limited_non_school_possible`
  - sampled accounts:
    - `5683-04-001-0340-907`
    - `6250-07-004-0110-907`
    - `0057-00-860-0001-908`
  - rollout behavior:
    - parcels remain unsupported with `tax_limitation_uncertain`
    - summary now explains that school-tax protections may limit current-year cash savings more than non-school taxes
    - limitation note makes the directional-only nature explicit
- `shadow_quoteable_public_refined_review`
  - sampled accounts:
    - `2545-02-001-0300-907`
    - `0029-00-000-1186-901`
  - rollout behavior:
    - parcels remain unsupported with `low_confidence_refined_review`
    - public response now explicitly says there is protest signal, but a refined review is still required before showing a safe public range
    - no supportability promotion occurred
- `high_opportunity_low_cash`
  - sampled accounts:
    - `2869-01-001-0230-901`
    - `8700-04-001-0150-907`
    - `5741-10-002-0170-907`
  - rollout behavior:
    - public quotes remain supported
    - summary now says the parcel shows protest opportunity but likely modest current-year cash
    - `next_step_cta` now explicitly points to refined review

Interpretation:

- Fort Bend is where Stage 6 meaningfully improves honesty without switching models:
  - exemption-limited low-cash cases are now visibly different from standard quotes
  - school-limited cases explain why the quote is directional or unsupported
  - shadow-quoteable refined-review cases remain blocked, but the reason is now more informative

## Explicit Non-Changes

Stage 6 did not:

- replace the public savings formula
- expose the Stage 5 shadow savings number publicly
- relax county capability limitations
- weaken Harris over65 or tax-rate reliability limitations
- absorb Harris `missing_assessment_basis` into broader state labels

## Stage 6 Decision

Stage 6 is acceptable because:

- the Stage 1 guardrail baseline remained intact
- public behavior changes were explicit and state-specific
- changes favored honest messaging rather than hidden dollar-model replacement
- low-cash, opportunity-only, school-limited, and refined-review-shadow-signal cohorts are now inspectable in both public responses and internal logs

No `near_total_exemption_low_cash` rows were present in the isolated 2026 Harris or Fort Bend profile sets, so that state remains implemented and tested but not live-exercised in this validation run.
