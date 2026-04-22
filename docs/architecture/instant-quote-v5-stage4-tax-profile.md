# Instant Quote V5 Stage 4 Validation

Date: 2026-04-22

Scope:
- add the additive `instant_quote_tax_profile` materialized table required by V5
- materialize only the required V5.1 profile fields for Harris and Fort Bend 2026
- use the Stage 3 county capability matrix during profile quality and limitation classification
- preserve current public instant-quote savings math and public response shape

Implementation summary:
- migration `0060_stage24_instant_quote_tax_profile.sql` adds `instant_quote_tax_profile` with the required Stage 4 V5.1 fields only:
  - identifiers and version fields
  - typed assessment basis fields
  - core value fields
  - exemption flags and code arrays
  - tax-unit/tax-rate completeness flags
  - quality/status fields
  - cap-gap and total-exemption heuristics
  - marginal-rate summary fields
  - internal limitation, warning, and product-state fields
- `app/services/instant_quote_tax_profile.py` materializes the table from:
  - `instant_quote_subject_cache`
  - `parcel_summary_view`
  - basis-year `parcel_taxing_units` + `tax_rates`
  - `instant_quote_county_tax_capability`
  - the latest completed `instant_quote_refresh_runs` row
- `app/services/instant_quote_validation.py` now exposes a compact `tax_profile` summary inside `InstantQuoteValidationReport`

Public contract guardrails:
- public savings math remains `reduction_estimate_times_effective_tax_rate`
- no public instant-quote response fields were added, removed, or renamed
- Stage 4 does not enable runtime/public use of the tax profile yet
- Stage 4 does not change the accepted Stage 1 supportability baseline

Test validation:
- targeted Stage 4 suite passed on the isolated Stage 21 DB target with `DWELLIO_DATABASE_URL=postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev`
- result: `69 passed`

Live-data validation:
- applied migration `0060` on the isolated Stage 21 DB
- materialized `instant_quote_tax_profile` for:
  - `harris`, `2026`
  - `fort_bend`, `2026`
- reran the accepted Stage 1 guardrail command after Stage 4 changes
- the fresh Stage 4 guardrail output matched `instant-quote-v5-stage1-guardrail-20260422.json` exactly
- Harris supportability remained unchanged:
  - `support_rate_all_sfr_flagged = 0.9980266470924625`
  - `missing_assessment_basis = 1345`
- Fort Bend supportability remained unchanged:
  - `support_rate_all_sfr_flagged = 0.9930466513664263`

Materialized profile summaries:
- Harris 2026:
  - `row_count = 1171610`
  - `rows_with_assessment_basis_value = 1169515`
  - `rows_with_raw_exemption_codes = 827054`
  - `rows_with_complete_tax_rate = 1169635`
  - `fallback_tax_profile_count = 1171610`
  - `missing_assessment_basis_warning_count = 1345`
  - `prior_year_assessment_basis_fallback` remained explicit on `78569` rows
  - `tax_profile_status` distribution:
    - `supported_with_disclosure = 1169209`
    - `opportunity_only = 239`
    - `unsupported = 2162`
- Fort Bend 2026:
  - `row_count = 278427`
  - `rows_with_assessment_basis_value = 278427`
  - `rows_with_raw_exemption_codes = 210887`
  - `rows_with_complete_tax_rate = 277916`
  - `fallback_tax_profile_count = 278427`
  - `school_ceiling_amount_unavailable_count = 2885`
  - `prior_year_assessment_basis_fallback` remained explicit on `2` rows
  - `tax_profile_status` distribution:
    - `supported_with_disclosure = 273999`
    - `constrained = 2480`
    - `opportunity_only = 1046`
    - `unsupported = 902`

County capability limitations flowing into the profile:
- both counties still materialize `tax_rate_reliability = limited`, so every 2026 profile carries `fallback_tax_profile_used_flag = true` while the basis year remains 2025
- both counties still materialize `profile_support_level = summary_only`, and that limitation is carried into `profile_warning_codes`
- Harris `missing_assessment_basis` stayed explicit in the profile instead of being silently absorbed:
  - `1345` Harris profiles include `missing_assessment_basis`
  - all `1345` of those rows remain `tax_profile_status = unsupported`
- Fort Bend freeze/school-ceiling uncertainty stayed explicit:
  - `2885` Fort Bend profiles have `freeze_flag = true`
  - all `2885` of those rows carry `school_ceiling_amount_unavailable`
- Harris and Fort Bend over65 limitations remain explicit at the county-capability level, but the isolated 2026 parcel scope currently shows `0` over65-flagged profile rows in both counties, so there are no row-level `over65_reliability_limited` counts yet

Interpretation:
- Stage 4 successfully materialized the required V5.1 summary-first tax profile without changing public quote behavior
- the profile is populated from repo-supported data only and keeps weak county truth explicit instead of hiding it
- Harris `missing_assessment_basis` remains visible and unchanged
- Stage 4 still avoids exact school-ceiling math, unit-level exemption allocation, and breakpoint dependencies

Baseline handoff:
- continue using `instant-quote-v5-stage1-guardrail-20260422.json` as the live-data guardrail baseline
- use `instant-quote-v5-stage4-tax-profile-20260422.json` as the structured Stage 4 tax-profile validation artifact
