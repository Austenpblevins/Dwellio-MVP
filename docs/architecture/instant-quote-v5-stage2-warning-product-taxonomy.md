# Instant Quote V5 Stage 2 Validation

Date: 2026-04-22

Scope:
- add internal warning action classes for existing blockers, warnings, and runtime guardrails
- add internal product-state classification for opportunity-versus-savings posture
- preserve current public instant-quote savings math and public response shape
- validate against the isolated Stage 21 DB on `localhost:55442/stage21_dev`

Implementation summary:
- migration `0058_stage24_instant_quote_warning_product_taxonomy.sql` adds additive internal columns to `instant_quote_request_logs`:
  - `warning_action_classes`
  - `dominant_warning_action_class`
  - `warning_taxonomy_json`
  - `opportunity_vs_savings_state`
  - `product_state_reason_code`
- `app/services/instant_quote.py` now maps existing blocker and warning behavior into the internal action classes:
  - `suppress`
  - `constrain`
  - `disclose`
  - `QA_only`
- `app/services/instant_quote.py` now classifies served requests into internal product states without changing public savings math or `InstantQuoteResponse`

Public contract guardrails:
- the public savings model remains `reduction_estimate_times_effective_tax_rate`
- `app/models/quote.py` was not changed in Stage 2
- Stage 2 writes internal taxonomy only to telemetry and `instant_quote_request_logs`
- no public response fields were added, removed, or renamed

Test validation:
- targeted Stage 2 suite passed on the isolated DB target with `DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev`
- result: `62 passed`

Live-data validation:
- applied migration `0058` on the isolated Stage 21 DB
- reran the Stage 0/1 guardrail command against `harris` 2026 and `fort_bend` 2026 after Stage 2 changes
- the fresh Stage 2 guardrail output matched the accepted Stage 1 baseline artifact exactly:
  - baseline artifact: `instant-quote-v5-stage1-guardrail-20260422.json`
  - comparison result: exact match
- Harris `missing_assessment_basis` remained explicit and unchanged in the guardrail output:
  - `missing_assessment_basis: 1345`
  - `support_rate_all_sfr_flagged: 0.9980266470924625`
- Fort Bend guardrail output also remained unchanged from the accepted Stage 1 baseline

Internal taxonomy population:
- recent isolated live sample window: `2026-04-22 17:08:00+00` onward
- sampled request count with Stage 2 fields present: `194`
- population coverage in `instant_quote_request_logs`:
  - `warning_action_classes`: `194 / 194`
  - `dominant_warning_action_class`: `194 / 194`
  - `warning_taxonomy_json`: `194 / 194`
  - `opportunity_vs_savings_state`: `194 / 194`
  - `product_state_reason_code`: `194 / 194`

Observed internal product states in isolated live traffic:
- Harris supported requests:
  - `no_opportunity_detected`: `33`
  - `strong_opportunity_high_cash`: `20`
  - `tax_profile_low_quality`: `12`
  - `standard_quote`: `11`
  - `supported_opportunity_low_cash`: `11`
  - `strong_opportunity_low_cash`: `1`
- Harris unsupported requests:
  - `unsupported_value_signal`: `5`
  - `suppressed_data_quality`: `5`
- Fort Bend supported requests:
  - `no_opportunity_detected`: `29`
  - `tax_profile_low_quality`: `14`
  - `standard_quote`: `13`
  - `strong_opportunity_high_cash`: `8`
  - `supported_opportunity_low_cash`: `4`
- Fort Bend unsupported requests:
  - `unsupported_value_signal`: `20`
  - `suppressed_data_quality`: `4`
  - `manual_review_recommended`: `3`
  - `opportunity_only_tax_profile_incomplete`: `1`

Targeted Harris evidence:
- supported prior-year basis fallback request:
  - account `0021480000013`
  - `warning_action_classes = ["constrain", "disclose"]`
  - `opportunity_vs_savings_state = "no_opportunity_detected"`
  - `warning_taxonomy_json` includes `prior_year_assessment_basis_fallback`
- blocked missing assessment basis request:
  - account `0102060000364`
  - `support_blocker_code = "missing_assessment_basis"`
  - `warning_action_classes = ["suppress", "constrain"]`
  - `opportunity_vs_savings_state = "suppressed_data_quality"`
  - `product_state_reason_code = "support_blocker_missing_assessment_basis"`
  - `warning_taxonomy_json` preserves `missing_assessment_basis` explicitly

Interpretation:
- Stage 2 successfully made warning taxonomy and product-state behavior inspectable internally
- Stage 2 did not change public savings math
- Stage 2 did not change the accepted Stage 1 supportability baseline
- Stage 2 did not hide, rename, or silently redefine the Harris `missing_assessment_basis` population

Baseline handoff:
- use `instant-quote-v5-stage1-guardrail-20260422.json` as the baseline artifact for judging later stages
- use `instant-quote-v5-stage2-warning-product-taxonomy-20260422.json` as the structured Stage 2 validation artifact
