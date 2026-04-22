# Instant Quote V5 Stage 5: Shadow Savings Comparison

Date: 2026-04-22
Branch: `codex/instant-quote-v5-stage05-shadow-savings`
Baseline guardrail artifact: `docs/architecture/instant-quote-v5-stage1-guardrail-20260422.json`
Stage 4 tax-profile handoff artifact: `docs/architecture/instant-quote-v5-stage4-tax-profile-20260422.json`
Stage 5 comparison artifact: `docs/architecture/instant-quote-v5-stage5-shadow-comparison-20260422.json`

## Scope

Stage 5 adds an internal shadow savings path that reads the Stage 4 `instant_quote_tax_profile` and computes a side-by-side V5 savings estimate without changing the public instant-quote response, public savings math, or public supportability behavior.

Public behavior remains on:

- `reduction_estimate * effective_tax_rate`

The shadow path is internal only and is persisted through `instant_quote_request_logs` for analysis.

## Live Validation Setup

- isolated DB: `postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev`
- guardrail rerun:
  - `python3 -m infra.scripts.report_instant_quote_v5_stage0_baseline --county-ids harris fort_bend --tax-year 2026`
- shadow comparison rerun:
  - `python3 -m infra.scripts.report_instant_quote_v5_stage5_shadow_comparison --county-ids harris fort_bend --tax-year 2026 --sample-size 100`
- request-log smoke check:
  - Harris `1240900010010`
  - Fort Bend `2278-08-002-0080-914`

## Guardrail Result

The fresh Stage 5 guardrail rerun matched `instant-quote-v5-stage1-guardrail-20260422.json` exactly.

That means Stage 5 did not change:

- public savings math
- public supportability behavior
- Harris `missing_assessment_basis` handling

Confirmed live baseline values remained:

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

## Shadow Comparison Findings

### Harris 2026

- All `1171610` Stage 4 tax profiles still use `fallback_tax_profile_used_flag = true`.
- `missing_assessment_basis_warning_count = 1345` remained explicit in the tax profile and was not absorbed by the shadow path.
- Full profile status distribution:
  - `supported_with_disclosure = 1169209`
  - `opportunity_only = 239`
  - `unsupported = 2162`
- In the 100-row supportable sample:
  - shadow quoteability matched current public quoteability exactly
  - max shadow delta was `0.0`
  - mean shadow delta was `0.0`
  - current public display-based `$0` share was `0.47474747474747475`
  - current raw `$0` share was `0.45454545454545453`
  - shadow raw `$0` share was `0.45454545454545453`

Interpretation:

- Harris shadow savings is functionally flat versus the current public model in the sampled quoteable cohort.
- The apparent `$0` share difference disappears when comparing raw current savings to raw shadow savings; the gap comes from public display bucketing, not a new shadow-model regression.

### Fort Bend 2026

- All `278427` Stage 4 tax profiles still use `fallback_tax_profile_used_flag = true`.
- `school_ceiling_amount_unavailable_count = 2885` remained explicit.
- Full profile status distribution:
  - `supported_with_disclosure = 273999`
  - `constrained = 2480`
  - `opportunity_only = 1046`
  - `unsupported = 902`
- In the 100-row supportable sample:
  - `supported_public_quote_count = 94`
  - `shadow_quoteable_count = 96`
  - `public_unsupported_shadow_quoteable_count = 2`
  - `high_opportunity_low_cash_count = 3`
  - `max_abs_shadow_delta_raw = 1072.7895362146855`
  - `mean_shadow_delta_raw = -11.174891002236308`
  - current public display/raw `$0` share was `0.46808510638297873`
  - shadow raw `$0` share was `0.4791666666666667`

Important sampled cohorts:

- Fort Bend account `6250-01-004-1600-907` shifted from current savings `1072.7895362146855` to shadow savings `0.0` because the Stage 4 profile marked `total_exemption_likely`.
- Three sampled Fort Bend rows were classified as `total_exemption_low_cash`.
- Two sampled Fort Bend rows were still publicly unsupported with `low_confidence_refined_review` but were analytically shadow-quoteable:
  - `2545-02-001-0300-907`
  - `0029-00-000-1186-901`

Interpretation:

- Fort Bend is where Stage 5 starts surfacing real separation between public quote behavior and V5 shadow behavior.
- That separation is still shadow-only and appropriately constrained by explicit limitation codes:
  - `profile_support_level_summary_only`
  - `tax_rate_basis_fallback_applied`
  - `total_exemption_likely` where applicable

## Opportunity-Only Review

The 100-row supportable sample did not naturally produce positive-savings `opportunity_only` candidates.

To avoid hiding that cohort, the Stage 5 artifact now includes `opportunity_only_profile_examples` sampled from the full Stage 4 tax-profile population.

Observed examples showed:

- Harris opportunity-only rows that are still publicly supported but shadow-unquoteable with `opportunity_only_tax_profile_incomplete`
- Harris and Fort Bend opportunity-only rows that remain suppressed by current public blockers such as `missing_living_area`

This keeps opportunity-only behavior inspectable without changing public quote behavior.

## Stage 5 Decision

Stage 5 is acceptable as a shadow-only analytical stage because:

- public baseline behavior remained exactly unchanged
- side-by-side shadow outputs were persisted internally
- fallback-driven cohorts remained explicit
- Harris `missing_assessment_basis` remained explicit
- county capability limitations remained visible instead of being collapsed into fake precision

The stage does not yet justify public rollout of V5 savings translation. It provides the comparison surface needed for Stage 6 review and later rollout decisions.
