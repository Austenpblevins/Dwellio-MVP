# Instant Quote V5 Rollout Readiness Closeout

Date: 2026-04-22
Closeout branch: `codex/instant-quote-v5-rollout-readiness-closeout`
Behavior baseline: `docs/architecture/instant-quote-v5-stage1-guardrail-20260422.json`
Final calibration artifact: `docs/architecture/instant-quote-v5-stage8-calibration-20260422.json`

## Scope

This closeout note freezes the implemented Stage 0 through Stage 8 V5 posture without broadening rollout or changing quote behavior.

All implementation stages are complete and merged:

- Stage 0 PR `#44`
- Stage 1 PR `#45`
- Stage 2 PR `#46`
- Stage 3 PR `#47`
- Stage 4 PR `#48`
- Stage 5 PR `#49`
- Stage 6 PR `#50`
- Stage 7 PR `#51`
- Stage 8 PR `#52`

## What Each Stage Delivered

### Stage 0

- froze the repo-grounded baseline
- recorded the isolated live-data baseline for Harris and Fort Bend 2026
- scaffolded the V5 feature flag without changing public quote logic

### Stage 1

- added typed companion metadata around `assessment_basis_value`
- preserved the public savings formula and response shape
- established the accepted corrected baseline at `instant-quote-v5-stage1-guardrail-20260422.json`

### Stage 2

- added internal warning taxonomy:
  - `suppress`
  - `constrain`
  - `disclose`
  - `QA_only`
- added internal product-state taxonomy
- kept public quote behavior unchanged

### Stage 3

- materialized the county capability matrix for Harris and Fort Bend 2026
- made county limitations explicit instead of burying them in runtime-only logic

### Stage 4

- materialized the summary-first `instant_quote_tax_profile`
- populated the required V5.1 fields only
- kept optional precision work out of launch-critical scope

### Stage 5

- added the internal shadow savings path
- persisted side-by-side current-versus-shadow comparison outputs
- identified Fort Bend exemption-limited cases as the clearest public rollout candidate

### Stage 6

- rolled out state-aware public presentation
- made low-cash, opportunity-only, school-limited, and refined-review postures more honest
- did not replace the public savings formula

### Stage 7

- added a feature-flagged savings-translation rollout path
- kept the public model as the default path
- validated the first justified translated cohort:
  - Fort Bend `total_exemption_low_cash`

### Stage 8

- calibrated the Stage 7 rollout to be more conservative
- reduced default enabled translation states to `total_exemption_low_cash`
- kept `near_total_exemption_low_cash` implemented but dormant
- blocked non-calibrated expansion in code

## Guardrails Preserved Across Stages

The implementation sequence preserved these non-negotiable guardrails:

- the public baseline remained anchored to `instant-quote-v5-stage1-guardrail-20260422.json`
- Harris `missing_assessment_basis` remained explicit and was not silently absorbed into broader states
- public savings math was not globally replaced
- public response shape was not broadened for V5 internals
- county capability limitations remained explicit
- school-limited and refined-review cohorts remained constrained
- fallback-driven 2026 tax-profile uncertainty remained visible

## Final Rollout Posture

### Enabled now

Production-ready V5 behavior, as currently scoped, is:

- typed basis metadata is live internally
- warning taxonomy and product-state taxonomy are live internally
- county capability matrix is live internally
- summary-first tax profile is materialized internally
- shadow savings comparison is live internally
- Stage 6 state-aware public messaging is live
- Stage 7/8 translated savings rollout code is live but controlled by feature flag

The only approved translated public savings cohort is:

- Fort Bend `total_exemption_low_cash`

That cohort remains:

- feature-flagged
- reversible
- paired with explicit low-cash / exemption-limited messaging

### Dormant but implemented

These paths exist in code but are not approved as default rollout:

- `near_total_exemption_low_cash`

Reason:

- no isolated 2026 Harris or Fort Bend live rows were observed

### Explicitly not approved for rollout

The following remain intentionally constrained and are not approved for public translated rollout:

- any Harris translated-savings cohort
- `school_limited_non_school_possible`
- `shadow_quoteable_public_refined_review`
- `opportunity_only_tax_profile_incomplete`
- broad `high_opportunity_low_cash` translation
- any broad county-wide switch from the current public formula to the V5 translated path

## What Is Production-Ready Today

Production-ready today, under current limits:

- the Stage 0 through Stage 8 implementation stack
- internal V5 metadata, taxonomy, capability, tax-profile, and shadow-comparison infrastructure
- Stage 6 honest state-aware public presentation
- feature-flagged translated savings for the evidence-backed Fort Bend `total_exemption_low_cash` cohort only

Operational recommendation:

- V5 is ready for production use as a constrained rollout layer, not as a broad replacement model

## What Remains Future Work

Future work remains for:

- any Harris public translated-savings rollout
- any school-limited public translated-savings rollout
- any refined-review-shadow-signal public rollout
- any broad numeric rollout for opportunity-only cohorts
- any expansion of `near_total_exemption_low_cash` beyond dormant implementation
- any work that depends on stronger non-fallback 2026 tax-profile certainty
- any exact school-ceiling truth, unit-level exemption allocation, or breakpoint-heavy tax logic

## Key Risks And Limitations

### Harris data quality

- Harris still carries explicit `missing_assessment_basis` blockers
- Harris over65 reliability remains limited
- Harris should remain constrained until live evidence supports a safer translated-savings expansion

### School-limited cohorts

- school-limited rows still need directional-only treatment
- public translated cash outputs are still too risky there

### Refined-review shadow-signal cohorts

- shadow quoteability alone is not enough for safe public rollout
- those rows still require manual review safeguards

### Fallback-driven 2026 tax profiles

- both counties still rely on fallback-driven 2026 tax-profile support
- that means V5 can improve honesty and targeted low-cash handling today, but it should not be treated as high-certainty exact tax truth

## Rules For Any Future Expansion

Any future rollout expansion should satisfy all of the following:

- cohort-first, never broad replacement by default
- isolated live-data validation must be rerun
- `instant-quote-v5-stage1-guardrail-20260422.json` must remain intact outside intentionally enabled cohorts
- the cohort must show clear public benefit, not just analytical signal
- fallback-driven uncertainty must remain explicit
- Harris data-quality constraints must not be weakened
- school-limited and refined-review safeguards must not be weakened without new evidence
- expansion must remain feature-flagged and reversible
- if no live rows exist for a cohort, it stays dormant rather than being treated as proven

## Final Recommendation

No more implementation is recommended right now.

Final rollout posture:

- keep the current public formula as the default behavior
- keep Stage 6 state-aware messaging active
- keep translated savings feature-flagged
- approve only Fort Bend `total_exemption_low_cash` for translated public savings rollout
- keep `near_total_exemption_low_cash` dormant
- keep Harris, school-limited, refined-review, and opportunity-only numeric expansion constrained until new live evidence justifies expansion

Under those limits, the current V5 package is production-ready.
