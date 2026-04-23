# Fort Bend Post-Remediation V5 Rollout Revalidation

Date: 2026-04-22  
Scope: Fort Bend only, isolated Stage 21 DB only  
Database: `postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev`

## Why this exists

The Fort Bend living-area denominator remediation changed quote magnitudes enough that the earlier Fort Bend portions of Stage 5 through Stage 8 could no longer be treated as current. This note reruns the Fort Bend-only late-stage validation chain on the corrected denominator and records the updated rollout posture.

## Baseline after correction

The Fort Bend county-level public baseline stayed stable after the denominator correction refresh:

- `support_rate_all_sfr_flagged = 0.9930466513664263`
- `supportable = 276491 / 278427`
- `monitored_zero_savings_quote_share = 0.5208333333333334`
- blocker distribution:
  - `supportable = 276491`
  - `missing_living_area = 1452`
  - `missing_effective_tax_rate = 484`

This means the correction materially changed quote magnitudes on individual Fort Bend parcels without creating a county-level supportability regression.

## Target parcel recheck

Target parcel:

- `1239 Carswell Grove DR, Missouri City, TX 77459`
- account `8118-49-006-0150-907`

After the denominator correction:

- corrected `living_area_sf = 2023`
- current public quote remains supported
- current public range: `$1,200 - $2,250` with midpoint `$1,700`
- public rollout state: `strong_opportunity_high_cash`
- shadow savings equals current savings for this parcel
- Stage 7 translated rollout does not apply to this parcel, even when the Fort Bend total-exemption rollout flag is enabled

Interpretation: the target parcel remains a standard public quote, not a translated-rollout candidate.

## Stage 5 rerun on corrected Fort Bend data

Old Fort Bend Stage 5 vs corrected Fort Bend Stage 5:

- `current_zero_share_supported`: `0.46808510638297873 -> 0.4787234042553192`
- `shadow_zero_share_quoteable`: `0.4791666666666667 -> 0.4895833333333333`
- `max_abs_shadow_delta_raw`: `1072.7895362146855 -> 840.6166369883666`
- `mean_shadow_delta_raw`: `-11.174891002236308 -> -11.265711501032996`
- `public_unsupported_shadow_quoteable_count`: unchanged at `2`

What changed materially inside the total-exemption cohort:

- `3531-04-001-0410-914`: old current display `$0`, new current display `$850`, shadow still `$0`
- `6250-01-004-1600-907`: old current display `$1,050`, new current display `$250`, shadow still `$0`
- `5016-01-001-0170-914`: stayed effectively `$0` on both paths

Interpretation: the corrected denominator did not eliminate the exemption-limited signal. It changed which Fort Bend rows are materially overstated by the current public formula.

## Stage 6 rerun on corrected Fort Bend data

Fort Bend target shadow-state counts stayed unchanged after remediation:

- `total_exemption_low_cash = 8117`
- `school_limited_non_school_possible = 2442`
- `opportunity_only_tax_profile_incomplete = 11`
- `near_total_exemption_low_cash = 0`

Interpretation: the denominator correction changed magnitudes, not the overall state taxonomy footprint. The county still needs the same honesty-oriented state handling, and there is still no live Fort Bend evidence for `near_total_exemption_low_cash`.

## Stage 7 rerun on corrected Fort Bend data

Old Fort Bend material translated rows:

- `6250-01-004-1600-907`: `$1,050 -> $0`

Corrected Fort Bend material translated rows:

- `3531-04-001-0410-914`: `$850 -> $0`
- `6250-01-004-1600-907`: `$250 -> $0`

Still unchanged:

- `5016-01-001-0170-914`: `$0 -> $0`

Interpretation: after the denominator correction, the evidence for a narrow translated rollout did not disappear. It became slightly broader within the same Fort Bend `total_exemption_low_cash` cohort.

## Stage 8 rerun on corrected Fort Bend data

The calibration decision did not change:

- keep `total_exemption_low_cash` as the only evidence-backed translated Fort Bend cohort
- keep `near_total_exemption_low_cash` implemented but dormant because observed live rows remain `0`
- do not expand to:
  - `high_opportunity_low_cash`
  - `opportunity_only_tax_profile_incomplete`
  - `school_limited_non_school_possible`
  - `manual_review_shadow_quoteable`

Why the non-exemption cohorts remain blocked:

- school-limited rows are still too uncertain for public translated cash output
- refined-review shadow-signal rows still need manual-review safeguards
- broader fallback-driven cohorts still do not show enough public benefit to justify rollout

## Rollout posture after Fort Bend correction

Fort Bend base quote path:

- safe again after the denominator correction
- materially more trustworthy than the pre-remediation denominator path

Fort Bend translated rollout:

- safe only as a narrow, feature-flagged cohort rollout for `total_exemption_low_cash`
- should remain disabled for every other Fort Bend cohort

Do **not** restore or broaden translated rollout for:

- `near_total_exemption_low_cash`
- `high_opportunity_low_cash`
- `opportunity_only_tax_profile_incomplete`
- `school_limited_non_school_possible`
- `manual_review_shadow_quoteable`

## Final recommendation

Use the corrected Fort Bend denominator for the base instant quote path.

If translated rollout is re-enabled for Fort Bend, keep it narrow and reversible:

- county: `fort_bend`
- state: `total_exemption_low_cash` only

Everything else should remain disabled until new corrected-data evidence says otherwise.
