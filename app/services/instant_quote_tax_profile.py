from __future__ import annotations

from typing import Any

from app.db.connection import get_connection
from app.services.instant_quote import MATERIAL_CAP_GAP_RATIO
from app.services.instant_quote_county_tax_capability import (
    InstantQuoteCountyTaxCapabilityService,
)

INSTANT_QUOTE_TAX_PROFILE_VERSION = "v5_summary_profile_v1"
NEAR_TOTAL_EXEMPTION_RATIO = 0.10
TOTAL_EXEMPTION_RATIO = 0.01


class InstantQuoteTaxProfileService:
    def __init__(
        self,
        *,
        capability_service: InstantQuoteCountyTaxCapabilityService | None = None,
    ) -> None:
        self.capability_service = capability_service or InstantQuoteCountyTaxCapabilityService()

    def materialize_profiles(
        self,
        *,
        county_id: str,
        tax_year: int,
        profile_version: str = INSTANT_QUOTE_TAX_PROFILE_VERSION,
    ) -> dict[str, Any]:
        capability = self.capability_service.fetch_materialized_capability(
            county_id=county_id,
            tax_year=tax_year,
        )
        if capability is None:
            capability = self.capability_service.materialize_capability(
                county_id=county_id,
                tax_year=tax_year,
            ).as_dict()

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                cursor.execute(
                    """
                    WITH latest_refresh AS (
                      SELECT
                        refresh_finished_at,
                        validated_at,
                        tax_rate_basis_warning_codes,
                        tax_rate_basis_fallback_applied
                      FROM instant_quote_refresh_runs
                      WHERE county_id = %s
                        AND tax_year = %s
                        AND refresh_status = 'completed'
                      ORDER BY refresh_finished_at DESC NULLS LAST, created_at DESC
                      LIMIT 1
                    ),
                    capability AS (
                      SELECT
                        county_id,
                        tax_year,
                        exemption_normalization_confidence,
                        over65_reliability,
                        disabled_reliability,
                        disabled_veteran_reliability,
                        freeze_reliability,
                        tax_unit_assignment_reliability,
                        tax_rate_reliability,
                        school_ceiling_amount_available,
                        local_option_policy_available,
                        profile_support_level
                      FROM instant_quote_county_tax_capability
                      WHERE county_id = %s
                        AND tax_year = %s
                    ),
                    basis_unit_rollup AS (
                      SELECT
                        sc.parcel_id,
                        sc.tax_year,
                        COUNT(*) FILTER (WHERE tu.unit_type_code = 'school')::integer AS school_unit_count,
                        COUNT(*) FILTER (WHERE tu.unit_type_code <> 'school')::integer AS non_school_unit_count,
                        COUNT(*)::integer AS tax_unit_count,
                        COALESCE(
                          SUM(COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)),
                          0::numeric
                        ) AS marginal_tax_rate_total_components,
                        COALESCE(
                          SUM(
                            CASE
                              WHEN tu.unit_type_code = 'school'
                                THEN COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)
                              ELSE 0::numeric
                            END
                          ),
                          0::numeric
                        ) AS marginal_tax_rate_school_components,
                        COALESCE(
                          SUM(
                            CASE
                              WHEN tu.unit_type_code <> 'school'
                                THEN COALESCE(tr.rate_value, tr.rate_per_100 / 100.0)
                              ELSE 0::numeric
                            END
                          ),
                          0::numeric
                        ) AS marginal_tax_rate_non_school_components
                      FROM instant_quote_subject_cache sc
                      LEFT JOIN parcel_taxing_units ptu
                        ON ptu.parcel_id = sc.parcel_id
                       AND ptu.tax_year = sc.effective_tax_rate_basis_year
                      LEFT JOIN taxing_units tu
                        ON tu.taxing_unit_id = ptu.taxing_unit_id
                      LEFT JOIN tax_rates tr
                        ON tr.taxing_unit_id = ptu.taxing_unit_id
                       AND tr.tax_year = ptu.tax_year
                       AND tr.is_current = true
                      WHERE sc.county_id = %s
                        AND sc.tax_year = %s
                      GROUP BY sc.parcel_id, sc.tax_year
                    ),
                    base_profile AS (
                      SELECT
                        sc.parcel_id,
                        sc.county_id,
                        sc.tax_year,
                        %s::text AS profile_version,
                        COALESCE(lr.refresh_finished_at, lr.validated_at) AS source_data_cutoff_at,
                        sc.assessment_basis_value,
                        sc.assessment_basis_source_value_type,
                        sc.assessment_basis_source_year,
                        sc.assessment_basis_source_reason,
                        ps.market_value,
                        ps.appraised_value,
                        COALESCE(ps.capped_value, sc.capped_value) AS capped_value,
                        ps.certified_value,
                        COALESCE(ps.notice_value, sc.notice_value) AS notice_value,
                        COALESCE(sc.homestead_flag, false) AS homestead_flag,
                        sc.over65_flag,
                        sc.disabled_flag,
                        sc.disabled_veteran_flag,
                        sc.freeze_flag,
                        COALESCE(ps.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
                        COALESCE(ps.exemption_type_codes, sc.exemption_type_codes, ARRAY[]::text[]) AS normalized_exemption_codes,
                        COALESCE(ps.exemption_value_total, 0::numeric) AS exemption_value_total,
                        COALESCE(sc.warning_codes, ARRAY[]::text[]) AS subject_warning_codes,
                        sc.support_blocker_code,
                        sc.effective_tax_rate,
                        sc.effective_tax_rate_source_method,
                        sc.effective_tax_rate_basis_fallback_applied,
                        sc.effective_tax_rate_basis_status,
                        COALESCE(lr.tax_rate_basis_warning_codes, ARRAY[]::text[]) AS refresh_warning_codes,
                        COALESCE(cap.exemption_normalization_confidence, 'unknown') AS exemption_normalization_confidence,
                        COALESCE(cap.over65_reliability, 'unknown') AS over65_reliability,
                        COALESCE(cap.disabled_reliability, 'unknown') AS disabled_reliability,
                        COALESCE(cap.disabled_veteran_reliability, 'unknown') AS disabled_veteran_reliability,
                        COALESCE(cap.freeze_reliability, 'unknown') AS freeze_reliability,
                        COALESCE(cap.tax_unit_assignment_reliability, 'unknown') AS tax_unit_assignment_reliability,
                        COALESCE(cap.tax_rate_reliability, 'unknown') AS tax_rate_reliability,
                        COALESCE(cap.school_ceiling_amount_available, false) AS school_ceiling_amount_available,
                        COALESCE(cap.local_option_policy_available, false) AS local_option_policy_available,
                        COALESCE(cap.profile_support_level, 'unknown') AS profile_support_level,
                        COALESCE(bur.tax_unit_count, 0) AS tax_unit_count,
                        COALESCE(bur.school_unit_count, 0) AS school_unit_count,
                        COALESCE(bur.non_school_unit_count, 0) AS non_school_unit_count,
                        COALESCE(bur.marginal_tax_rate_total_components, 0::numeric) AS marginal_tax_rate_total_components,
                        COALESCE(bur.marginal_tax_rate_school_components, 0::numeric) AS marginal_tax_rate_school_components,
                        COALESCE(bur.marginal_tax_rate_non_school_components, 0::numeric) AS marginal_tax_rate_non_school_components,
                        (
                          COALESCE(bur.school_unit_count, 0) > 0
                          AND COALESCE(bur.non_school_unit_count, 0) > 0
                        ) AS tax_unit_assignment_complete_flag,
                        (
                          COALESCE(bur.school_unit_count, 0) > 0
                          AND COALESCE(bur.non_school_unit_count, 0) > 0
                          AND COALESCE(bur.marginal_tax_rate_total_components, 0::numeric) > 0
                        ) AS tax_rate_complete_flag,
                        CASE
                          WHEN sc.assessment_basis_value IS NULL
                            OR sc.assessment_basis_value <= 0
                            OR COALESCE(ps.capped_value, sc.capped_value) IS NULL
                            OR COALESCE(ps.capped_value, sc.capped_value) <= 0
                            OR COALESCE(ps.capped_value, sc.capped_value) >= sc.assessment_basis_value
                            THEN NULL
                          ELSE ROUND(
                            (sc.assessment_basis_value - COALESCE(ps.capped_value, sc.capped_value))::numeric,
                            2
                          )
                        END AS cap_gap_value,
                        CASE
                          WHEN sc.assessment_basis_value IS NULL
                            OR sc.assessment_basis_value <= 0
                            OR COALESCE(ps.capped_value, sc.capped_value) IS NULL
                            OR COALESCE(ps.capped_value, sc.capped_value) <= 0
                            OR COALESCE(ps.capped_value, sc.capped_value) >= sc.assessment_basis_value
                            THEN NULL
                          ELSE ROUND(
                            (
                              (sc.assessment_basis_value - COALESCE(ps.capped_value, sc.capped_value))
                              / sc.assessment_basis_value
                            )::numeric,
                            6
                          )
                        END AS cap_gap_pct,
                        CASE
                          WHEN COALESCE(sc.homestead_flag, false) IS DISTINCT FROM true
                            OR sc.assessment_basis_value IS NULL
                            OR sc.assessment_basis_value <= 0
                            OR COALESCE(ps.capped_value, sc.capped_value) IS NULL
                            OR COALESCE(ps.capped_value, sc.capped_value) <= 0
                            OR COALESCE(ps.capped_value, sc.capped_value) >= sc.assessment_basis_value
                            THEN NULL
                          ELSE (
                            (
                              (sc.assessment_basis_value - COALESCE(ps.capped_value, sc.capped_value))
                              / sc.assessment_basis_value
                            ) >= %s
                          )
                        END AS homestead_cap_binding_flag,
                        CASE
                          WHEN sc.assessment_basis_value IS NULL OR sc.assessment_basis_value <= 0
                            THEN false
                          WHEN COALESCE(sc.disabled_veteran_flag, false)
                            THEN true
                          WHEN GREATEST(sc.assessment_basis_value - COALESCE(ps.exemption_value_total, 0::numeric), 0::numeric)
                               / sc.assessment_basis_value <= %s
                            AND (
                              cardinality(COALESCE(ps.exemption_type_codes, sc.exemption_type_codes, ARRAY[]::text[])) > 0
                              OR COALESCE(sc.homestead_flag, false)
                              OR COALESCE(sc.over65_flag, false)
                              OR COALESCE(sc.disabled_flag, false)
                              OR COALESCE(sc.disabled_veteran_flag, false)
                            )
                            THEN true
                          ELSE false
                        END AS total_exemption_flag,
                        CASE
                          WHEN sc.assessment_basis_value IS NULL OR sc.assessment_basis_value <= 0
                            THEN false
                          WHEN COALESCE(sc.disabled_veteran_flag, false)
                            THEN false
                          WHEN GREATEST(sc.assessment_basis_value - COALESCE(ps.exemption_value_total, 0::numeric), 0::numeric)
                               / sc.assessment_basis_value < %s
                            THEN true
                          ELSE false
                        END AS near_total_exemption_flag,
                        CASE
                          WHEN COALESCE(bur.school_unit_count, 0) > 0
                               AND COALESCE(bur.non_school_unit_count, 0) > 0
                            THEN 'school_and_non_school'
                          WHEN COALESCE(bur.school_unit_count, 0) > 0
                            THEN 'school_only'
                          WHEN COALESCE(bur.non_school_unit_count, 0) > 0
                            THEN 'non_school_only'
                          ELSE 'unknown'
                        END AS affected_unit_mask,
                        GREATEST(
                          0,
                          100
                          - CASE
                              WHEN COALESCE(cap.exemption_normalization_confidence, 'unknown') = 'unsupported'
                                THEN 35
                              WHEN COALESCE(cap.exemption_normalization_confidence, 'unknown') = 'limited'
                                THEN 20
                              ELSE 0
                            END
                          - CASE
                              WHEN cardinality(COALESCE(ps.raw_exemption_codes, ARRAY[]::text[])) = 0
                                   AND (
                                     cardinality(
                                       COALESCE(ps.exemption_type_codes, sc.exemption_type_codes, ARRAY[]::text[])
                                     ) > 0
                                     OR COALESCE(sc.homestead_flag, false)
                                     OR COALESCE(sc.over65_flag, false)
                                     OR COALESCE(sc.disabled_flag, false)
                                     OR COALESCE(sc.disabled_veteran_flag, false)
                                     OR COALESCE(sc.freeze_flag, false)
                                   )
                                THEN 15
                              ELSE 0
                            END
                          - CASE
                              WHEN 'missing_exemption_amount' = ANY(COALESCE(sc.warning_codes, ARRAY[]::text[]))
                                THEN 20
                              ELSE 0
                            END
                          - CASE
                              WHEN 'assessment_exemption_total_mismatch' = ANY(COALESCE(sc.warning_codes, ARRAY[]::text[]))
                                THEN 15
                              ELSE 0
                            END
                          - CASE
                              WHEN 'homestead_flag_mismatch' = ANY(COALESCE(sc.warning_codes, ARRAY[]::text[]))
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.over65_flag, false)
                                   AND COALESCE(cap.over65_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.disabled_flag, false)
                                   AND COALESCE(cap.disabled_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.freeze_flag, false)
                                   AND COALESCE(cap.freeze_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                        )::integer AS exemption_profile_quality_score,
                        GREATEST(
                          0,
                          100
                          - CASE
                              WHEN sc.support_blocker_code = 'missing_assessment_basis'
                                THEN 100
                              WHEN sc.support_blocker_code = 'missing_effective_tax_rate'
                                THEN 75
                              WHEN sc.support_blocker_code IS NOT NULL
                                THEN 40
                              ELSE 0
                            END
                          - CASE
                              WHEN (
                                COALESCE(bur.school_unit_count, 0) > 0
                                AND COALESCE(bur.non_school_unit_count, 0) > 0
                              )
                                THEN 0
                              ELSE 20
                            END
                          - CASE
                              WHEN (
                                COALESCE(bur.school_unit_count, 0) > 0
                                AND COALESCE(bur.non_school_unit_count, 0) > 0
                                AND COALESCE(bur.marginal_tax_rate_total_components, 0::numeric) > 0
                              )
                                THEN 0
                              ELSE 20
                            END
                          - CASE
                              WHEN COALESCE(sc.effective_tax_rate_basis_fallback_applied, false)
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN sc.assessment_basis_source_year IS NULL
                                   OR sc.assessment_basis_source_reason = 'missing'
                                THEN 50
                              WHEN sc.assessment_basis_source_year < sc.tax_year
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(cap.exemption_normalization_confidence, 'unknown') = 'unsupported'
                                THEN 20
                              WHEN COALESCE(cap.exemption_normalization_confidence, 'unknown') = 'limited'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN cardinality(COALESCE(ps.raw_exemption_codes, ARRAY[]::text[])) = 0
                                   AND cardinality(
                                     COALESCE(ps.exemption_type_codes, sc.exemption_type_codes, ARRAY[]::text[])
                                   ) > 0
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.over65_flag, false)
                                   AND COALESCE(cap.over65_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.disabled_flag, false)
                                   AND COALESCE(cap.disabled_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.disabled_veteran_flag, false)
                                   AND COALESCE(cap.disabled_veteran_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.freeze_flag, false)
                                   AND COALESCE(cap.freeze_reliability, 'unknown') <> 'supported'
                                THEN 10
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(sc.freeze_flag, false)
                                   AND COALESCE(cap.school_ceiling_amount_available, false) IS FALSE
                                THEN 15
                              ELSE 0
                            END
                          - CASE
                              WHEN COALESCE(cap.tax_rate_reliability, 'unknown') = 'limited'
                                THEN 5
                              WHEN COALESCE(cap.tax_rate_reliability, 'unknown') = 'unsupported'
                                THEN 15
                              ELSE 0
                            END
                        )::integer AS tax_profile_quality_score
                      FROM instant_quote_subject_cache sc
                      LEFT JOIN parcel_summary_view ps
                        ON ps.parcel_id = sc.parcel_id
                       AND ps.tax_year = sc.tax_year
                      LEFT JOIN latest_refresh lr
                        ON true
                      LEFT JOIN capability cap
                        ON cap.county_id = sc.county_id
                       AND cap.tax_year = sc.tax_year
                      LEFT JOIN basis_unit_rollup bur
                        ON bur.parcel_id = sc.parcel_id
                       AND bur.tax_year = sc.tax_year
                      WHERE sc.county_id = %s
                        AND sc.tax_year = %s
                    ),
                    finalized AS (
                      SELECT
                        bp.parcel_id,
                        bp.county_id,
                        bp.tax_year,
                        bp.profile_version,
                        now() AS generated_at,
                        bp.source_data_cutoff_at,
                        bp.assessment_basis_value,
                        bp.assessment_basis_source_value_type,
                        bp.assessment_basis_source_year,
                        bp.assessment_basis_source_reason,
                        bp.market_value,
                        bp.appraised_value,
                        bp.capped_value,
                        bp.certified_value,
                        bp.notice_value,
                        bp.homestead_flag,
                        bp.over65_flag,
                        bp.disabled_flag,
                        bp.disabled_veteran_flag,
                        bp.freeze_flag,
                        bp.raw_exemption_codes,
                        bp.normalized_exemption_codes,
                        bp.exemption_profile_quality_score,
                        bp.tax_unit_assignment_complete_flag,
                        bp.tax_rate_complete_flag,
                        CASE
                          WHEN bp.support_blocker_code = 'unsupported_property_type'
                            THEN 'unsupported'
                          WHEN bp.support_blocker_code = 'missing_assessment_basis'
                            THEN 'unsupported'
                          WHEN bp.tax_profile_quality_score >= 85
                            THEN 'supported'
                          WHEN bp.tax_profile_quality_score >= 65
                            THEN 'supported_with_disclosure'
                          WHEN bp.tax_profile_quality_score >= 45
                            THEN 'constrained'
                          WHEN bp.tax_profile_quality_score >= 25
                            THEN 'opportunity_only'
                          ELSE 'unsupported'
                        END AS tax_profile_status,
                        bp.tax_profile_quality_score,
                        bp.cap_gap_value,
                        bp.cap_gap_pct,
                        bp.homestead_cap_binding_flag,
                        bp.total_exemption_flag,
                        bp.near_total_exemption_flag,
                        CASE
                          WHEN bp.support_blocker_code = 'missing_assessment_basis'
                            THEN 'unsupported_tax_profile'
                          WHEN bp.total_exemption_flag
                            THEN 'limited_by_total_exemption'
                          WHEN bp.near_total_exemption_flag
                            THEN 'limited_by_near_total_exemption'
                          WHEN bp.freeze_flag IS TRUE AND bp.school_ceiling_amount_available IS FALSE
                            THEN 'limited_by_school_ceiling_unknown'
                          WHEN bp.tax_rate_complete_flag
                               AND bp.marginal_tax_rate_school_components > 0
                               AND bp.marginal_tax_rate_non_school_components > 0
                            THEN 'school_non_school_split'
                          WHEN bp.tax_rate_complete_flag
                               AND bp.marginal_tax_rate_total_components > 0
                            THEN 'modeled_total_marginal'
                          WHEN COALESCE(bp.effective_tax_rate, 0::numeric) > 0
                            THEN 'effective_rate_fallback'
                          WHEN bp.assessment_basis_value IS NOT NULL AND bp.assessment_basis_value > 0
                            THEN 'opportunity_only_no_reliable_tax_profile'
                          ELSE 'unsupported_tax_profile'
                        END AS marginal_model_type,
                        ROUND(
                          CASE
                            WHEN bp.tax_rate_complete_flag
                                 AND bp.marginal_tax_rate_total_components > 0
                              THEN bp.marginal_tax_rate_total_components
                            WHEN COALESCE(bp.effective_tax_rate, 0::numeric) > 0
                              THEN bp.effective_tax_rate
                            ELSE NULL
                          END,
                          8
                        ) AS marginal_tax_rate_total,
                        ROUND(
                          CASE
                            WHEN bp.tax_rate_complete_flag
                                 AND bp.marginal_tax_rate_school_components > 0
                              THEN bp.marginal_tax_rate_school_components
                            ELSE NULL
                          END,
                          8
                        ) AS marginal_tax_rate_school,
                        ROUND(
                          CASE
                            WHEN bp.tax_rate_complete_flag
                                 AND bp.marginal_tax_rate_non_school_components > 0
                              THEN bp.marginal_tax_rate_non_school_components
                            ELSE NULL
                          END,
                          8
                        ) AS marginal_tax_rate_non_school,
                        CASE
                          WHEN bp.tax_rate_complete_flag
                               AND bp.marginal_tax_rate_total_components > 0
                            THEN 'basis_year_component_rollup'
                          WHEN COALESCE(bp.effective_tax_rate, 0::numeric) > 0
                            THEN 'effective_tax_rate_fallback'
                          ELSE 'unsupported'
                        END AS marginal_rate_basis,
                        ARRAY(
                          SELECT DISTINCT code
                          FROM unnest(
                            array_remove(
                              ARRAY[
                                CASE
                                  WHEN bp.support_blocker_code IS NOT NULL THEN bp.support_blocker_code
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.assessment_basis_source_year IS NOT NULL
                                       AND bp.assessment_basis_source_year < bp.tax_year
                                    THEN 'prior_year_assessment_basis_fallback'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.effective_tax_rate_basis_fallback_applied
                                    THEN 'tax_rate_basis_fallback_applied'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.profile_support_level <> 'full'
                                    THEN 'profile_support_level_' || bp.profile_support_level
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN cardinality(bp.raw_exemption_codes) = 0
                                       AND cardinality(bp.normalized_exemption_codes) > 0
                                    THEN 'missing_raw_exemption_codes'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.over65_flag IS TRUE AND bp.over65_reliability <> 'supported'
                                    THEN 'over65_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.disabled_flag IS TRUE AND bp.disabled_reliability <> 'supported'
                                    THEN 'disabled_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.disabled_veteran_flag IS TRUE
                                       AND bp.disabled_veteran_reliability <> 'supported'
                                    THEN 'disabled_veteran_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.freeze_flag IS TRUE AND bp.freeze_reliability <> 'supported'
                                    THEN 'freeze_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.freeze_flag IS TRUE
                                       AND bp.school_ceiling_amount_available IS FALSE
                                    THEN 'school_ceiling_amount_unavailable'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.total_exemption_flag THEN 'total_exemption_likely'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.near_total_exemption_flag THEN 'near_total_exemption_likely'
                                  ELSE NULL
                                END
                              ],
                              NULL::text
                            )
                          ) AS code
                          WHERE btrim(code) <> ''
                          ORDER BY code
                        ) AS savings_limited_by_codes,
                        bp.affected_unit_mask,
                        CASE
                          WHEN bp.support_blocker_code = 'unsupported_property_type'
                            THEN 'unsupported_property_type'
                          WHEN bp.support_blocker_code IS NOT NULL
                            THEN 'suppressed_data_quality'
                          WHEN bp.total_exemption_flag
                            THEN 'total_exemption_low_cash'
                          WHEN bp.near_total_exemption_flag
                            THEN 'near_total_exemption_low_cash'
                          WHEN bp.freeze_flag IS TRUE AND bp.school_ceiling_amount_available IS FALSE
                            THEN 'school_limited_non_school_possible'
                          WHEN bp.tax_profile_quality_score < 45
                            THEN 'opportunity_only_tax_profile_incomplete'
                          WHEN bp.tax_profile_quality_score < 65
                            THEN 'tax_profile_low_quality'
                          ELSE 'standard_quote'
                        END AS opportunity_vs_savings_state,
                        ARRAY(
                          SELECT DISTINCT code
                          FROM unnest(
                            bp.subject_warning_codes
                            || bp.refresh_warning_codes
                            || array_remove(
                              ARRAY[
                                CASE
                                  WHEN bp.support_blocker_code IS NOT NULL THEN bp.support_blocker_code
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.assessment_basis_source_year IS NOT NULL
                                       AND bp.assessment_basis_source_year < bp.tax_year
                                    THEN 'prior_year_assessment_basis_fallback'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.effective_tax_rate_basis_fallback_applied
                                    THEN 'tax_rate_basis_fallback_applied'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.profile_support_level <> 'full'
                                    THEN 'profile_support_level_' || bp.profile_support_level
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN cardinality(bp.raw_exemption_codes) = 0
                                       AND cardinality(bp.normalized_exemption_codes) > 0
                                    THEN 'missing_raw_exemption_codes'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.over65_flag IS TRUE AND bp.over65_reliability <> 'supported'
                                    THEN 'over65_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.disabled_flag IS TRUE AND bp.disabled_reliability <> 'supported'
                                    THEN 'disabled_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.disabled_veteran_flag IS TRUE
                                       AND bp.disabled_veteran_reliability <> 'supported'
                                    THEN 'disabled_veteran_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.freeze_flag IS TRUE AND bp.freeze_reliability <> 'supported'
                                    THEN 'freeze_reliability_limited'
                                  ELSE NULL
                                END,
                                CASE
                                  WHEN bp.freeze_flag IS TRUE
                                       AND bp.school_ceiling_amount_available IS FALSE
                                    THEN 'school_ceiling_amount_unavailable'
                                  ELSE NULL
                                END
                              ],
                              NULL::text
                            )
                          ) AS code
                          WHERE btrim(code) <> ''
                          ORDER BY code
                        ) AS profile_warning_codes,
                        (
                          COALESCE(bp.effective_tax_rate_basis_fallback_applied, false)
                          OR (
                            bp.assessment_basis_source_year IS NOT NULL
                            AND bp.assessment_basis_source_year < bp.tax_year
                          )
                          OR (
                            NOT bp.tax_rate_complete_flag
                            AND COALESCE(bp.effective_tax_rate, 0::numeric) > 0
                          )
                        ) AS fallback_tax_profile_used_flag
                      FROM base_profile bp
                    )
                    INSERT INTO instant_quote_tax_profile (
                      parcel_id,
                      county_id,
                      tax_year,
                      profile_version,
                      generated_at,
                      source_data_cutoff_at,
                      assessment_basis_value,
                      assessment_basis_source_value_type,
                      assessment_basis_source_year,
                      assessment_basis_source_reason,
                      market_value,
                      appraised_value,
                      capped_value,
                      certified_value,
                      notice_value,
                      homestead_flag,
                      over65_flag,
                      disabled_flag,
                      disabled_veteran_flag,
                      freeze_flag,
                      raw_exemption_codes,
                      normalized_exemption_codes,
                      exemption_profile_quality_score,
                      tax_unit_assignment_complete_flag,
                      tax_rate_complete_flag,
                      tax_profile_status,
                      tax_profile_quality_score,
                      cap_gap_value,
                      cap_gap_pct,
                      homestead_cap_binding_flag,
                      total_exemption_flag,
                      near_total_exemption_flag,
                      marginal_model_type,
                      marginal_tax_rate_total,
                      marginal_tax_rate_school,
                      marginal_tax_rate_non_school,
                      marginal_rate_basis,
                      savings_limited_by_codes,
                      affected_unit_mask,
                      opportunity_vs_savings_state,
                      profile_warning_codes,
                      fallback_tax_profile_used_flag
                    )
                    SELECT
                      parcel_id,
                      county_id,
                      tax_year,
                      profile_version,
                      generated_at,
                      source_data_cutoff_at,
                      assessment_basis_value,
                      assessment_basis_source_value_type,
                      assessment_basis_source_year,
                      assessment_basis_source_reason,
                      market_value,
                      appraised_value,
                      capped_value,
                      certified_value,
                      notice_value,
                      homestead_flag,
                      over65_flag,
                      disabled_flag,
                      disabled_veteran_flag,
                      freeze_flag,
                      raw_exemption_codes,
                      normalized_exemption_codes,
                      exemption_profile_quality_score,
                      tax_unit_assignment_complete_flag,
                      tax_rate_complete_flag,
                      tax_profile_status,
                      tax_profile_quality_score,
                      cap_gap_value,
                      cap_gap_pct,
                      homestead_cap_binding_flag,
                      total_exemption_flag,
                      near_total_exemption_flag,
                      marginal_model_type,
                      marginal_tax_rate_total,
                      marginal_tax_rate_school,
                      marginal_tax_rate_non_school,
                      marginal_rate_basis,
                      savings_limited_by_codes,
                      affected_unit_mask,
                      opportunity_vs_savings_state,
                      profile_warning_codes,
                      fallback_tax_profile_used_flag
                    FROM finalized
                    ON CONFLICT (parcel_id, tax_year, profile_version) DO UPDATE
                    SET county_id = EXCLUDED.county_id,
                        generated_at = EXCLUDED.generated_at,
                        source_data_cutoff_at = EXCLUDED.source_data_cutoff_at,
                        assessment_basis_value = EXCLUDED.assessment_basis_value,
                        assessment_basis_source_value_type = EXCLUDED.assessment_basis_source_value_type,
                        assessment_basis_source_year = EXCLUDED.assessment_basis_source_year,
                        assessment_basis_source_reason = EXCLUDED.assessment_basis_source_reason,
                        market_value = EXCLUDED.market_value,
                        appraised_value = EXCLUDED.appraised_value,
                        capped_value = EXCLUDED.capped_value,
                        certified_value = EXCLUDED.certified_value,
                        notice_value = EXCLUDED.notice_value,
                        homestead_flag = EXCLUDED.homestead_flag,
                        over65_flag = EXCLUDED.over65_flag,
                        disabled_flag = EXCLUDED.disabled_flag,
                        disabled_veteran_flag = EXCLUDED.disabled_veteran_flag,
                        freeze_flag = EXCLUDED.freeze_flag,
                        raw_exemption_codes = EXCLUDED.raw_exemption_codes,
                        normalized_exemption_codes = EXCLUDED.normalized_exemption_codes,
                        exemption_profile_quality_score = EXCLUDED.exemption_profile_quality_score,
                        tax_unit_assignment_complete_flag = EXCLUDED.tax_unit_assignment_complete_flag,
                        tax_rate_complete_flag = EXCLUDED.tax_rate_complete_flag,
                        tax_profile_status = EXCLUDED.tax_profile_status,
                        tax_profile_quality_score = EXCLUDED.tax_profile_quality_score,
                        cap_gap_value = EXCLUDED.cap_gap_value,
                        cap_gap_pct = EXCLUDED.cap_gap_pct,
                        homestead_cap_binding_flag = EXCLUDED.homestead_cap_binding_flag,
                        total_exemption_flag = EXCLUDED.total_exemption_flag,
                        near_total_exemption_flag = EXCLUDED.near_total_exemption_flag,
                        marginal_model_type = EXCLUDED.marginal_model_type,
                        marginal_tax_rate_total = EXCLUDED.marginal_tax_rate_total,
                        marginal_tax_rate_school = EXCLUDED.marginal_tax_rate_school,
                        marginal_tax_rate_non_school = EXCLUDED.marginal_tax_rate_non_school,
                        marginal_rate_basis = EXCLUDED.marginal_rate_basis,
                        savings_limited_by_codes = EXCLUDED.savings_limited_by_codes,
                        affected_unit_mask = EXCLUDED.affected_unit_mask,
                        opportunity_vs_savings_state = EXCLUDED.opportunity_vs_savings_state,
                        profile_warning_codes = EXCLUDED.profile_warning_codes,
                        fallback_tax_profile_used_flag = EXCLUDED.fallback_tax_profile_used_flag
                    """,
                    (
                        county_id,
                        tax_year,
                        county_id,
                        tax_year,
                        county_id,
                        tax_year,
                        profile_version,
                        MATERIAL_CAP_GAP_RATIO,
                        TOTAL_EXEMPTION_RATIO,
                        NEAR_TOTAL_EXEMPTION_RATIO,
                        county_id,
                        tax_year,
                    ),
                )
            connection.commit()

        return self.fetch_materialized_summary(
            county_id=county_id,
            tax_year=tax_year,
            profile_version=profile_version,
        )

    def fetch_materialized_summary(
        self,
        *,
        county_id: str,
        tax_year: int,
        profile_version: str = INSTANT_QUOTE_TAX_PROFILE_VERSION,
    ) -> dict[str, Any]:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      profile_version,
                      COUNT(*)::integer AS row_count,
                      MAX(source_data_cutoff_at) AS source_data_cutoff_at,
                      COUNT(*) FILTER (
                        WHERE assessment_basis_value IS NOT NULL
                          AND assessment_basis_value > 0
                      )::integer AS rows_with_assessment_basis_value,
                      COUNT(*) FILTER (
                        WHERE cardinality(raw_exemption_codes) > 0
                      )::integer AS rows_with_raw_exemption_codes,
                      COUNT(*) FILTER (
                        WHERE cardinality(normalized_exemption_codes) > 0
                      )::integer AS rows_with_normalized_exemption_codes,
                      COUNT(*) FILTER (
                        WHERE tax_unit_assignment_complete_flag
                      )::integer AS rows_with_complete_tax_unit_assignment,
                      COUNT(*) FILTER (
                        WHERE tax_rate_complete_flag
                      )::integer AS rows_with_complete_tax_rate,
                      COUNT(*) FILTER (
                        WHERE fallback_tax_profile_used_flag
                      )::integer AS fallback_tax_profile_count,
                      COUNT(*) FILTER (
                        WHERE 'missing_assessment_basis' = ANY(profile_warning_codes)
                      )::integer AS missing_assessment_basis_warning_count,
                      COUNT(*) FILTER (
                        WHERE over65_flag IS TRUE
                          AND 'over65_reliability_limited' = ANY(profile_warning_codes)
                      )::integer AS over65_reliability_limited_count,
                      COUNT(*) FILTER (
                        WHERE freeze_flag IS TRUE
                          AND 'school_ceiling_amount_unavailable' = ANY(profile_warning_codes)
                      )::integer AS school_ceiling_amount_unavailable_count
                    FROM instant_quote_tax_profile
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND profile_version = %s
                    GROUP BY profile_version
                    """,
                    (county_id, tax_year, profile_version),
                )
                summary_row = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT
                      tax_profile_status,
                      COUNT(*)::integer AS count
                    FROM instant_quote_tax_profile
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND profile_version = %s
                    GROUP BY tax_profile_status
                    ORDER BY count DESC, tax_profile_status ASC
                    """,
                    (county_id, tax_year, profile_version),
                )
                status_distribution = {
                    str(row["tax_profile_status"]): int(row["count"]) for row in cursor.fetchall()
                }

        if summary_row is None:
            return {}
        payload = dict(summary_row)
        payload["status_distribution"] = status_distribution
        return payload
