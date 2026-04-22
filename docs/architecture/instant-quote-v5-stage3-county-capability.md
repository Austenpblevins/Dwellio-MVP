# Instant Quote V5 Stage 3 Validation

Date: 2026-04-22

Scope:
- add the `instant_quote_county_tax_capability` county-year matrix required by V5
- materialize explicit Harris and Fort Bend 2026 capability rows
- wire the matrix into `InstantQuoteValidationReport`
- preserve current public instant-quote savings math and public response shape

Implementation summary:
- migration `0059_stage24_instant_quote_county_tax_capability.sql` adds the county-year matrix table:
  - `exemption_normalization_confidence`
  - `over65_reliability`
  - `disabled_reliability`
  - `disabled_veteran_reliability`
  - `freeze_reliability`
  - `tax_unit_assignment_reliability`
  - `tax_rate_reliability`
  - `school_ceiling_amount_available`
  - `unit_exemption_policy_available`
  - `local_option_policy_available`
  - `profile_support_level`
- `app/services/instant_quote_county_tax_capability.py` builds and upserts county-year capability rows from:
  - county config capability entries
  - current `instant_quote_subject_cache` signal observations
  - the latest completed `instant_quote_refresh_runs` record
- `app/services/instant_quote_validation.py` now includes the materialized county capability snapshot in `InstantQuoteValidationReport`

Public contract guardrails:
- public savings math remains `reduction_estimate_times_effective_tax_rate`
- no public instant-quote response fields were added, removed, or renamed
- Stage 3 does not start `instant_quote_tax_profile` materialization

Test validation:
- targeted Stage 3 suite passed on the isolated Stage 21 DB target with `DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev`
- result: `73 passed`

Live-data validation:
- applied migration `0059` on the isolated Stage 21 DB
- materialized county capability rows for:
  - `harris`, `2026`
  - `fort_bend`, `2026`
- reran the accepted Stage 1 guardrail command after Stage 3 changes
- the fresh Stage 3 guardrail output matched `instant-quote-v5-stage1-guardrail-20260422.json` exactly
- Harris supportability remained unchanged:
  - `support_rate_all_sfr_flagged = 0.9980266470924625`
  - `missing_assessment_basis = 1345`
- Fort Bend supportability remained unchanged:
  - `support_rate_all_sfr_flagged = 0.9930466513664263`

Materialized county capability rows:
- Harris 2026:
  - `exemption_normalization_confidence = limited`
  - `over65_reliability = limited`
  - `disabled_reliability = limited`
  - `disabled_veteran_reliability = supported`
  - `freeze_reliability = limited`
  - `tax_unit_assignment_reliability = supported`
  - `tax_rate_reliability = limited`
  - `school_ceiling_amount_available = false`
  - `local_option_policy_available = false`
  - `profile_support_level = summary_only`
- Fort Bend 2026:
  - `exemption_normalization_confidence = limited`
  - `over65_reliability = limited`
  - `disabled_reliability = supported`
  - `disabled_veteran_reliability = supported`
  - `freeze_reliability = limited`
  - `tax_unit_assignment_reliability = supported`
  - `tax_rate_reliability = limited`
  - `school_ceiling_amount_available = false`
  - `local_option_policy_available = false`
  - `profile_support_level = summary_only`

Notable county limitations made explicit:
- Harris over65 remains explicitly limited and the materialized notes preserve that county-specific caveat.
- Harris disabled-veteran support remains explicit and supported.
- Both counties currently materialize `tax_rate_reliability = limited` because 2026 instant quote support still falls back to prior-year adopted rates.
- Both counties currently materialize `exemption_normalization_confidence = limited` because missing exemption amounts remain common in the current-year isolated cache.
- Fort Bend adapter config supports over65 source mapping, but the Stage 3 materialization downgraded 2026 `over65_reliability` to `limited` because the isolated live cache currently shows `0` observed over65 rows.

Interpretation:
- Stage 3 successfully moved county tax-profile limitations into an explicit county-year matrix instead of burying them in runtime-only logic.
- Stage 3 did not change the accepted Stage 1 supportability baseline.
- Harris `missing_assessment_basis` remained visible and unchanged.
- The matrix is now available for later stages through both the materialized table and the internal validation report.

Baseline handoff:
- continue using `instant-quote-v5-stage1-guardrail-20260422.json` as the live-data guardrail baseline
- use `instant-quote-v5-stage3-county-capability-20260422.json` as the structured Stage 3 capability validation artifact
