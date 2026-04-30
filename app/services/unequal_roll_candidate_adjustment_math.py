from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import median
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.unequal_roll_candidate_adjustment_support import (
    ADJUSTMENT_BURDEN_EXCLUDE_CHANNEL_COUNT,
    ADJUSTMENT_BURDEN_EXCLUDE_PCT_THRESHOLD,
    ADJUSTMENT_BURDEN_REVIEW_CHANNEL_COUNT,
    ADJUSTMENT_BURDEN_REVIEW_PCT_THRESHOLD,
)
from app.services.unequal_roll_candidate_normalization import (
    condition_rank,
    quality_rank,
)

ADJUSTMENT_MATH_VERSION = "unequal_roll_adjustment_math_v2"
ADJUSTMENT_MATH_CONFIG_VERSION = "unequal_roll_adjustment_math_v2"

GLA_RATE_SCALE = 0.55
AGE_RATE_PCT_OF_SUBJECT_RAW_VALUE_PER_YEAR = 0.0015
FULL_BATH_RATE_PCT_OF_SUBJECT_RAW_VALUE = 0.03
HALF_BATH_RATE_PCT_OF_SUBJECT_RAW_VALUE = 0.01625
STORY_RATE_PCT_OF_SUBJECT_RAW_VALUE = 0.01
POOL_RATE_PCT_OF_SUBJECT_RAW_VALUE = 0.02
QUALITY_STEP_RATE_PCT_OF_SUBJECT_RAW_VALUE = 0.02
CONDITION_STEP_RATE_PCT_OF_SUBJECT_RAW_VALUE = 0.015

MATERIAL_ADJUSTMENT_MIN_PCT_OF_RAW_VALUE = 0.01
NONTRIVIAL_ADJUSTMENT_MIN_PCT_OF_RAW_VALUE = 0.005
DISPERSION_DIVERGENCE_RANK_SHIFT_THRESHOLD = 2
STRONG_CONFLICT_DIVERGENCE_RANK_SHIFT_THRESHOLD = 4
UNRESOLVED_CHANNEL_EXCLUDE_COUNT_THRESHOLD = 3

CHANNEL_DETAIL_KEY_BY_ADJUSTMENT_TYPE = {
    "age": "age_or_effective_age",
}

UNRESOLVED_CHANNEL_SEVERITY_BY_TYPE = {
    "gla": "high",
    "age": "moderate",
    "full_bath": "high",
    "half_bath": "moderate",
    "bedroom": "low",
    "land_site": "moderate",
    "story": "moderate",
    "pool": "moderate",
    "quality": "high",
    "condition": "high",
}

ADJUSTMENT_TYPES_IN_ORDER = [
    "gla",
    "age",
    "full_bath",
    "half_bath",
    "bedroom",
    "land_site",
    "story",
    "pool",
    "quality",
    "condition",
]


@dataclass(frozen=True)
class UnequalRollCandidateAdjustmentMathResult:
    unequal_roll_run_id: str
    total_candidates: int
    adjusted_count: int
    review_adjusted_count: int
    adjusted_limited_count: int
    review_adjusted_limited_count: int
    excluded_from_adjustment_math_count: int
    adjustment_line_count: int


class UnequalRollCandidateAdjustmentMathService:
    def build_adjustment_math_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateAdjustmentMathResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                run_context = self._fetch_run_context(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if run_context is None:
                    raise LookupError(
                        "Unequal-roll run context not found for adjustment math "
                        f"{unequal_roll_run_id}."
                    )

                candidates = self._fetch_candidates(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if not candidates:
                    raise LookupError(
                        "Unequal-roll adjustment-support candidates not found for run "
                        f"{unequal_roll_run_id}."
                    )

                self._delete_existing_adjustments(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                adjustment_plan, dispersion_support = self._build_adjustment_math_plan(
                    candidates=candidates,
                    run_context=run_context,
                )
                adjustment_log_json = self._build_adjustment_log_json(
                    run_context=run_context,
                    adjustment_plan=adjustment_plan,
                    dispersion_support=dispersion_support,
                )
                inserted_adjustment_line_count = 0
                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    assignment = adjustment_plan[candidate_id]
                    self._persist_candidate_adjustment_math(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        adjustment_math_assignment=assignment,
                    )
                    for line_item in assignment["line_items"]:
                        self._insert_adjustment_line_item(
                            cursor,
                            unequal_roll_candidate_id=candidate_id,
                            line_item=line_item,
                            candidate=candidate,
                            unequal_roll_run_id=unequal_roll_run_id,
                        )
                        inserted_adjustment_line_count += 1
                self._persist_run_adjustment_log(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                    adjustment_log_json=adjustment_log_json,
                )
            connection.commit()

        return UnequalRollCandidateAdjustmentMathResult(
            unequal_roll_run_id=unequal_roll_run_id,
            total_candidates=len(candidates),
            adjusted_count=sum(
                1
                for assignment in adjustment_plan.values()
                if assignment["adjustment_math_status"] == "adjusted"
            ),
            review_adjusted_count=sum(
                1
                for assignment in adjustment_plan.values()
                if assignment["adjustment_math_status"] == "adjusted_with_review"
            ),
            adjusted_limited_count=sum(
                1
                for assignment in adjustment_plan.values()
                if assignment["adjustment_math_status"] == "adjusted_limited"
            ),
            review_adjusted_limited_count=sum(
                1
                for assignment in adjustment_plan.values()
                if assignment["adjustment_math_status"] == "adjusted_limited_with_review"
            ),
            excluded_from_adjustment_math_count=sum(
                1
                for assignment in adjustment_plan.values()
                if assignment["adjustment_math_status"] == "excluded_from_adjustment_math"
            ),
            adjustment_line_count=inserted_adjustment_line_count,
        )

    def _fetch_run_context(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT
              urr.unequal_roll_run_id,
              urr.final_comp_count_status,
              urr.selection_governance_status,
              urr.selection_log_json,
              urss.county_id,
              urss.tax_year,
              urss.living_area_sf,
              urss.year_built,
              urss.effective_age,
              urss.bedrooms,
              urss.full_baths,
              urss.half_baths,
              urss.stories,
              urss.quality_code,
              urss.condition_code,
              urss.pool_flag,
              urss.land_sf,
              urss.land_acres,
              urss.appraised_value
            FROM unequal_roll_runs AS urr
            JOIN unequal_roll_subject_snapshots AS urss
              ON urss.unequal_roll_run_id = urr.unequal_roll_run_id
            WHERE urr.unequal_roll_run_id = %s
            LIMIT 1
            """,
            (unequal_roll_run_id,),
        )
        return cursor.fetchone()

    def _fetch_candidates(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> list[dict[str, Any]]:
        cursor.execute(
            """
            SELECT
              unequal_roll_candidate_id,
              unequal_roll_run_id,
              candidate_parcel_id,
              county_id,
              tax_year,
              living_area_sf,
              year_built,
              effective_age,
              bedrooms,
              full_baths,
              half_baths,
              stories,
              quality_code,
              condition_code,
              pool_flag,
              land_sf,
              land_acres,
              appraised_value,
              chosen_comp_status,
              chosen_comp_position,
              chosen_comp_detail_json,
              eligibility_reason_code,
              normalized_similarity_score,
              adjustment_support_position,
              adjustment_support_status,
              adjustment_support_version,
              adjustment_support_config_version,
              adjustment_support_detail_json,
              candidate_snapshot_json
            FROM unequal_roll_candidates
            WHERE unequal_roll_run_id = %s
            ORDER BY adjustment_support_position NULLS LAST, chosen_comp_position NULLS LAST, candidate_parcel_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _delete_existing_adjustments(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> None:
        cursor.execute(
            "DELETE FROM unequal_roll_adjustments WHERE unequal_roll_run_id = %s",
            (unequal_roll_run_id,),
        )

    def _build_adjustment_math_plan(
        self,
        *,
        candidates: list[dict[str, Any]],
        run_context: dict[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        base_assignments: dict[str, dict[str, Any]] = {}
        adjusted_candidates: list[dict[str, Any]] = []

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if not self._is_adjustment_math_eligible(candidate):
                base_assignments[candidate_id] = {
                    "adjustment_math_status": "excluded_from_adjustment_math",
                    "adjusted_appraised_value": None,
                    "total_signed_adjustment": None,
                    "total_absolute_adjustment": None,
                    "adjustment_pct_of_raw_value": None,
                    "material_adjustment_count": None,
                    "nontrivial_adjustment_sources_count": None,
                    "adjustment_summary_json": self._excluded_adjustment_summary_json(
                        candidate=candidate,
                        run_context=run_context,
                    ),
                    "line_items": [],
                }
                continue

            line_items = self._build_line_items(candidate=candidate, run_context=run_context)
            summary = self._build_preliminary_adjustment_summary(
                candidate=candidate,
                run_context=run_context,
                line_items=line_items,
            )
            base_assignments[candidate_id] = {
                "adjustment_math_status": self._adjustment_math_status_from_support(candidate),
                "adjusted_appraised_value": summary["adjusted_appraised_value"],
                "total_signed_adjustment": summary["total_signed_adjustment"],
                "total_absolute_adjustment": summary["total_absolute_adjustment"],
                "adjustment_pct_of_raw_value": summary["adjustment_pct_of_raw_value"],
                "material_adjustment_count": summary["material_adjustment_count"],
                "nontrivial_adjustment_sources_count": (
                    summary["nontrivial_adjustment_sources_count"]
                ),
                "adjustment_summary_json": summary["adjustment_summary_json"],
                "line_items": line_items,
            }
            adjusted_candidates.append(
                {
                    "candidate_id": candidate_id,
                    "raw_appraised_value": _as_float(candidate.get("appraised_value")),
                    "raw_appraised_value_per_sf": _value_per_sf(
                        _as_float(candidate.get("appraised_value")),
                        _as_float(candidate.get("living_area_sf")),
                    ),
                    "adjusted_appraised_value": summary["adjusted_appraised_value"],
                    "adjusted_appraised_value_per_sf": _value_per_sf(
                        summary["adjusted_appraised_value"],
                        _as_float(candidate.get("living_area_sf")),
                    ),
                }
            )

        dispersion_support = self._build_dispersion_support(adjusted_candidates)
        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            assignment = base_assignments[candidate_id]
            if assignment["adjustment_math_status"] == "excluded_from_adjustment_math":
                continue
            comp_dispersion = dispersion_support["candidate_flags"].get(candidate_id, {})
            summary_json = dict(assignment["adjustment_summary_json"])
            summary_json["dispersion_scaffolding"] = {
                "status": "evaluated_iqr_scaffold",
                **comp_dispersion,
            }
            burden_summary = dict(summary_json.get("burden_summary") or {})
            summary_json["burden_governance"] = _burden_governance_summary(
                burden_status=str(burden_summary.get("burden_status") or "within_thresholds"),
                adjustment_pct_of_raw_value=_as_float(
                    burden_summary.get("adjustment_pct_of_raw_value")
                ),
                material_adjustment_count=_as_int(
                    burden_summary.get("material_adjustment_count")
                )
                or 0,
                nontrivial_adjustment_sources_count=_as_int(
                    burden_summary.get("nontrivial_adjustment_sources_count")
                )
                or 0,
                unresolved_material_difference_details=list(
                    burden_summary.get("unresolved_material_difference_details") or []
                ),
                review_carry_forward_flag=bool(
                    summary_json.get("review_carry_forward_flag")
                ),
                adjusted_conflict_indicator_flag=bool(
                    comp_dispersion.get("adjusted_conflict_indicator_flag")
                ),
            )
            summary_json["adjustment_conflict_support"] = {
                **dict(summary_json.get("adjustment_conflict_support") or {}),
                "raw_adjusted_divergence_flag": comp_dispersion.get(
                    "raw_adjusted_divergence_flag",
                    False,
                ),
                "adjusted_conflict_indicator_flag": comp_dispersion.get(
                    "adjusted_conflict_indicator_flag",
                    False,
                ),
                "high_adjustment_driver_flag": (
                    summary_json["burden_governance"]["status"]
                    in {"manual_review_recommended", "exclude_recommended"}
                ),
            }
            summary_json["adjusted_set_governance"] = _adjusted_set_governance_summary(
                burden_governance=dict(summary_json.get("burden_governance") or {}),
                source_governance=dict(summary_json.get("source_governance") or {}),
                review_carry_forward_flag=bool(summary_json.get("review_carry_forward_flag")),
                acceptable_zone_admitted_flag=bool(
                    (summary_json.get("acceptable_zone_governance") or {}).get(
                        "acceptable_zone_admitted_flag"
                    )
                ),
                adjusted_conflict_indicator_flag=bool(
                    comp_dispersion.get("adjusted_conflict_indicator_flag")
                ),
                raw_adjusted_divergence_flag=bool(
                    comp_dispersion.get("raw_adjusted_divergence_flag")
                ),
                raw_adjusted_rank_shift=_as_int(
                    comp_dispersion.get("raw_adjusted_rank_shift")
                ),
                adjusted_value_outlier_flag=bool(
                    comp_dispersion.get("adjusted_value_outlier_flag")
                ),
                adjusted_value_per_sf_outlier_flag=bool(
                    comp_dispersion.get("adjusted_value_per_sf_outlier_flag")
                ),
            )
            assignment["adjustment_summary_json"] = summary_json

        return base_assignments, dispersion_support

    def _build_line_items(
        self,
        *,
        candidate: dict[str, Any],
        run_context: dict[str, Any],
    ) -> list[dict[str, Any]]:
        adjustment_support_detail_json = dict(
            candidate.get("adjustment_support_detail_json") or {}
        )
        adjustment_channels = dict(
            adjustment_support_detail_json.get("adjustment_channels") or {}
        )
        line_items: list[dict[str, Any]] = []
        for line_order, adjustment_type in enumerate(ADJUSTMENT_TYPES_IN_ORDER, start=1):
            channel_key = CHANNEL_DETAIL_KEY_BY_ADJUSTMENT_TYPE.get(
                adjustment_type,
                adjustment_type,
            )
            channel_detail = dict(adjustment_channels.get(channel_key) or {})
            line_items.append(
                self._line_item_for_channel(
                    adjustment_type=adjustment_type,
                    line_order=line_order,
                    channel_detail=channel_detail,
                    candidate=candidate,
                    run_context=run_context,
                )
            )
        return line_items

    def _line_item_for_channel(
        self,
        *,
        adjustment_type: str,
        line_order: int,
        channel_detail: dict[str, Any],
        candidate: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        subject_raw_value = _as_float(run_context.get("appraised_value"))
        raw_value_per_sf = _value_per_sf(
            subject_raw_value,
            _as_float(run_context.get("living_area_sf")),
        )
        chosen_comp_detail_json = dict(candidate.get("chosen_comp_detail_json") or {})
        acceptable_zone_evaluation = dict(
            (chosen_comp_detail_json.get("score_context") or {}).get(
                "acceptable_zone_evaluation"
            )
            or {}
        )
        governance_context = {
            "review_carry_forward_flag": candidate.get("chosen_comp_status")
            == "review_chosen_comp",
            "acceptable_zone_admitted_flag": (
                (chosen_comp_detail_json.get("chosen_comp_context") or {}).get(
                    "acceptable_zone_admitted_flag"
                )
            ),
            "acceptable_zone_evaluation": acceptable_zone_evaluation,
        }

        if adjustment_type == "gla":
            rate = round((raw_value_per_sf or 0.0) * GLA_RATE_SCALE, 2) if raw_value_per_sf else None
            diff = _as_float(run_context.get("living_area_sf")) - _as_float(
                candidate.get("living_area_sf")
            )
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="gla",
                source_method_code="subject_raw_value_per_sf_scaled_fallback",
                subject_value_json={"living_area_sf": _as_float(run_context.get("living_area_sf"))},
                candidate_value_json={"living_area_sf": _as_float(candidate.get("living_area_sf"))},
                difference_value_json={"living_area_sf_delta": diff},
                rate_or_basis_json={
                    "formula_code": "subject_minus_candidate_times_rate",
                    "rate_per_sf": rate,
                    "subject_raw_value_per_sf": raw_value_per_sf,
                    "scale_factor": GLA_RATE_SCALE,
                    "source_priority": 4,
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="GLA scaffold adjustment using a transparent raw-value-per-SF fallback scale.",
                raw_value=subject_raw_value,
            )

        if adjustment_type == "age":
            basis_field = channel_detail.get("basis_field")
            rate = (
                round(subject_raw_value * AGE_RATE_PCT_OF_SUBJECT_RAW_VALUE_PER_YEAR, 2)
                if subject_raw_value
                else None
            )
            if basis_field == "effective_age":
                diff = _as_float(candidate.get("effective_age")) - _as_float(
                    run_context.get("effective_age")
                )
            else:
                diff = _as_float(run_context.get("year_built")) - _as_float(
                    candidate.get("year_built")
                )
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="age",
                source_method_code="subject_raw_value_pct_per_year_fallback",
                subject_value_json={
                    basis_field or "effective_age": channel_detail.get("subject_value")
                },
                candidate_value_json={
                    basis_field or "effective_age": channel_detail.get("candidate_value")
                },
                difference_value_json={"delta": diff, "basis_field": basis_field},
                rate_or_basis_json={
                    "formula_code": "age_difference_times_fallback_year_rate",
                    "rate_per_year": rate,
                    "rate_pct_of_subject_raw_value": (
                        AGE_RATE_PCT_OF_SUBJECT_RAW_VALUE_PER_YEAR
                    ),
                    "basis_field": basis_field,
                    "source_priority": 4,
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Age/effective-age scaffold adjustment using a conservative per-year fallback.",
                raw_value=subject_raw_value,
            )

        if adjustment_type == "full_bath":
            diff = _difference(
                _as_float(channel_detail.get("subject_value")),
                _as_float(channel_detail.get("candidate_value")),
            )
            rate = (
                round(subject_raw_value * FULL_BATH_RATE_PCT_OF_SUBJECT_RAW_VALUE, 2)
                if subject_raw_value
                else None
            )
            source_method_code = "subject_raw_value_pct_fallback"
            source_precedence_override = None
            if channel_detail.get("basis_source_code") == "fort_bend_valuation_bathroom_features_exact":
                source_method_code = "fort_bend_exact_bath_basis_with_fallback_rate"
                source_precedence_override = {
                    "rank": 3,
                    "label": "county_supported_secondary_basis_with_fallback_rate",
                    "quality_tier": "county_supported_basis_with_fallback_rate",
                    "resolution_status": "monetized_basis_supported_fallback_rate",
                }
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="full_bath",
                source_method_code=source_method_code,
                subject_value_json={
                    "full_baths": _as_float(channel_detail.get("subject_value")),
                },
                candidate_value_json={
                    "full_baths": _as_float(channel_detail.get("candidate_value")),
                },
                difference_value_json={
                    "full_bath_delta": diff,
                    "basis_source_code": channel_detail.get("basis_source_code"),
                },
                rate_or_basis_json={
                    "formula_code": "subject_minus_candidate_times_rate",
                    "rate_per_full_bath": rate,
                    "rate_pct_of_subject_raw_value": FULL_BATH_RATE_PCT_OF_SUBJECT_RAW_VALUE,
                    "source_priority": 4,
                    "basis_source_code": channel_detail.get("basis_source_code"),
                    "basis_source_reason_code": channel_detail.get("basis_source_reason_code"),
                    "basis_source_support": {
                        "secondary_source_used_flag": channel_detail.get(
                            "secondary_source_used_flag"
                        ),
                        "valuation_support_basis_field": channel_detail.get(
                            "valuation_support_basis_field"
                        ),
                        "valuation_support_basis_value": channel_detail.get(
                            "valuation_support_basis_value"
                        ),
                        "valuation_support_basis_status": channel_detail.get(
                            "valuation_support_basis_status"
                        ),
                        "valuation_support_basis_confidence": channel_detail.get(
                            "valuation_support_basis_confidence"
                        ),
                    },
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Full-bath scaffold adjustment using a conservative raw-value percentage fallback.",
                raw_value=subject_raw_value,
                source_precedence_override=source_precedence_override,
            )

        if adjustment_type == "half_bath":
            diff = _difference(
                _as_float(channel_detail.get("subject_value")),
                _as_float(channel_detail.get("candidate_value")),
            )
            rate = (
                round(subject_raw_value * HALF_BATH_RATE_PCT_OF_SUBJECT_RAW_VALUE, 2)
                if subject_raw_value
                else None
            )
            source_method_code = "subject_raw_value_pct_fallback"
            source_precedence_override = None
            if channel_detail.get("basis_source_code") == "fort_bend_valuation_bathroom_features_exact":
                source_method_code = "fort_bend_exact_bath_basis_with_fallback_rate"
                source_precedence_override = {
                    "rank": 3,
                    "label": "county_supported_secondary_basis_with_fallback_rate",
                    "quality_tier": "county_supported_basis_with_fallback_rate",
                    "resolution_status": "monetized_basis_supported_fallback_rate",
                }
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="half_bath",
                source_method_code=source_method_code,
                subject_value_json={
                    "half_baths": _as_float(channel_detail.get("subject_value")),
                },
                candidate_value_json={
                    "half_baths": _as_float(channel_detail.get("candidate_value")),
                },
                difference_value_json={
                    "half_bath_delta": diff,
                    "basis_source_code": channel_detail.get("basis_source_code"),
                },
                rate_or_basis_json={
                    "formula_code": "subject_minus_candidate_times_rate",
                    "rate_per_half_bath": rate,
                    "rate_pct_of_subject_raw_value": HALF_BATH_RATE_PCT_OF_SUBJECT_RAW_VALUE,
                    "source_priority": 4,
                    "basis_source_code": channel_detail.get("basis_source_code"),
                    "basis_source_reason_code": channel_detail.get("basis_source_reason_code"),
                    "basis_source_support": {
                        "secondary_source_used_flag": channel_detail.get(
                            "secondary_source_used_flag"
                        ),
                        "valuation_support_basis_field": channel_detail.get(
                            "valuation_support_basis_field"
                        ),
                        "valuation_support_basis_value": channel_detail.get(
                            "valuation_support_basis_value"
                        ),
                        "valuation_support_basis_status": channel_detail.get(
                            "valuation_support_basis_status"
                        ),
                        "valuation_support_basis_confidence": channel_detail.get(
                            "valuation_support_basis_confidence"
                        ),
                    },
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Half-bath scaffold adjustment using a conservative raw-value percentage fallback.",
                raw_value=subject_raw_value,
                source_precedence_override=source_precedence_override,
            )

        if adjustment_type == "bedroom":
            diff = _as_float(run_context.get("bedrooms")) - _as_float(candidate.get("bedrooms"))
            return self._non_monetized_line_item(
                line_order=line_order,
                adjustment_type="bedroom",
                source_method_code="non_monetized_guardrail",
                subject_value_json={"bedrooms": _as_int(run_context.get("bedrooms"))},
                candidate_value_json={"bedrooms": _as_int(candidate.get("bedrooms"))},
                difference_value_json={"bedroom_delta": diff},
                rate_or_basis_json={
                    "formula_code": "not_monetized_by_default",
                    "policy_basis": "bedroom_guardrail_only_unless_future_config_enables_monetization",
                    "governance_context": governance_context,
                },
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Bedroom difference is persisted as a guardrail channel but not monetized in this scaffold phase.",
            )

        if adjustment_type == "land_site":
            return self._non_monetized_line_item(
                line_order=line_order,
                adjustment_type="land_site",
                source_method_code="non_monetized_land_reasonableness_scaffold",
                subject_value_json={
                    "land_sf": _as_float(run_context.get("land_sf")),
                    "land_acres": _as_float(run_context.get("land_acres")),
                },
                candidate_value_json={
                    "land_sf": _as_float(candidate.get("land_sf")),
                    "land_acres": _as_float(candidate.get("land_acres")),
                },
                difference_value_json={
                    "land_sf_delta": _difference(
                        _as_float(run_context.get("land_sf")),
                        _as_float(candidate.get("land_sf")),
                    ),
                    "land_acres_delta": _difference(
                        _as_float(run_context.get("land_acres")),
                        _as_float(candidate.get("land_acres")),
                    ),
                },
                rate_or_basis_json={
                    "formula_code": "await_land_value_or_site_schedule",
                    "policy_basis": "persist_site_difference_without_forcing_monetized_land_adjustment",
                    "governance_context": governance_context,
                },
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Site/land difference is preserved for later governed land adjustment logic but not monetized yet.",
            )

        if adjustment_type == "story":
            diff = _as_float(run_context.get("stories")) - _as_float(candidate.get("stories"))
            rate = (
                round(subject_raw_value * STORY_RATE_PCT_OF_SUBJECT_RAW_VALUE, 2)
                if subject_raw_value
                else None
            )
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="story",
                source_method_code="subject_raw_value_pct_fallback",
                subject_value_json={"stories": _as_float(run_context.get("stories"))},
                candidate_value_json={"stories": _as_float(candidate.get("stories"))},
                difference_value_json={"story_delta": diff},
                rate_or_basis_json={
                    "formula_code": "subject_minus_candidate_times_rate",
                    "rate_per_story": rate,
                    "rate_pct_of_subject_raw_value": STORY_RATE_PCT_OF_SUBJECT_RAW_VALUE,
                    "source_priority": 4,
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Story-count scaffold adjustment using a conservative raw-value percentage fallback.",
                raw_value=subject_raw_value,
            )

        if adjustment_type == "pool":
            subject_pool = _bool_to_int(run_context.get("pool_flag"))
            candidate_pool = _bool_to_int(candidate.get("pool_flag"))
            diff = None
            if subject_pool is not None and candidate_pool is not None:
                diff = subject_pool - candidate_pool
            rate = (
                round(subject_raw_value * POOL_RATE_PCT_OF_SUBJECT_RAW_VALUE, 2)
                if subject_raw_value
                else None
            )
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="pool",
                source_method_code="subject_raw_value_pct_fallback",
                subject_value_json={"pool_flag": _as_bool(run_context.get("pool_flag"))},
                candidate_value_json={"pool_flag": _as_bool(candidate.get("pool_flag"))},
                difference_value_json={"pool_delta": diff},
                rate_or_basis_json={
                    "formula_code": "boolean_presence_delta_times_rate",
                    "rate_per_pool_mismatch": rate,
                    "rate_pct_of_subject_raw_value": POOL_RATE_PCT_OF_SUBJECT_RAW_VALUE,
                    "source_priority": 4,
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Pool scaffold adjustment using a conservative raw-value percentage fallback.",
                raw_value=subject_raw_value,
            )

        if adjustment_type == "quality":
            subject_rank = quality_rank(run_context.get("county_id"), run_context.get("quality_code"))
            candidate_rank = quality_rank(run_context.get("county_id"), candidate.get("quality_code"))
            diff = None
            if subject_rank is not None and candidate_rank is not None:
                diff = subject_rank - candidate_rank
            rate = (
                round(subject_raw_value * QUALITY_STEP_RATE_PCT_OF_SUBJECT_RAW_VALUE, 2)
                if subject_raw_value
                else None
            )
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="quality",
                source_method_code="subject_raw_value_pct_fallback",
                subject_value_json={
                    "quality_code": run_context.get("quality_code"),
                    "quality_rank": subject_rank,
                },
                candidate_value_json={
                    "quality_code": candidate.get("quality_code"),
                    "quality_rank": candidate_rank,
                },
                difference_value_json={"quality_rank_delta": diff},
                rate_or_basis_json={
                    "formula_code": "subject_minus_candidate_rank_times_rate",
                    "rate_per_quality_step": rate,
                    "rate_pct_of_subject_raw_value": QUALITY_STEP_RATE_PCT_OF_SUBJECT_RAW_VALUE,
                    "source_priority": 4,
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Quality scaffold adjustment using county-aware ordinal ranks and a conservative raw-value percentage fallback.",
                raw_value=subject_raw_value,
            )

        if adjustment_type == "condition":
            subject_rank = condition_rank(
                run_context.get("county_id"), run_context.get("condition_code")
            )
            candidate_rank = condition_rank(
                run_context.get("county_id"), candidate.get("condition_code")
            )
            diff = None
            if subject_rank is not None and candidate_rank is not None:
                diff = subject_rank - candidate_rank
            rate = (
                round(subject_raw_value * CONDITION_STEP_RATE_PCT_OF_SUBJECT_RAW_VALUE, 2)
                if subject_raw_value
                else None
            )
            return self._monetized_line_item(
                line_order=line_order,
                adjustment_type="condition",
                source_method_code="subject_raw_value_pct_fallback",
                subject_value_json={
                    "condition_code": run_context.get("condition_code"),
                    "condition_rank": subject_rank,
                },
                candidate_value_json={
                    "condition_code": candidate.get("condition_code"),
                    "condition_rank": candidate_rank,
                },
                difference_value_json={"condition_rank_delta": diff},
                rate_or_basis_json={
                    "formula_code": "subject_minus_candidate_rank_times_rate",
                    "rate_per_condition_step": rate,
                    "rate_pct_of_subject_raw_value": CONDITION_STEP_RATE_PCT_OF_SUBJECT_RAW_VALUE,
                    "source_priority": 4,
                    "governance_context": governance_context,
                },
                signed_adjustment_amount=_signed_product(diff, rate),
                readiness_status=channel_detail.get("readiness_status"),
                potential_adjustment_flag=channel_detail.get("potential_adjustment_flag"),
                notes="Condition scaffold adjustment using county-aware ordinal ranks and a conservative raw-value percentage fallback.",
                raw_value=subject_raw_value,
            )

        raise ValueError(f"Unsupported adjustment type {adjustment_type}.")

    def _monetized_line_item(
        self,
        *,
        line_order: int,
        adjustment_type: str,
        source_method_code: str,
        subject_value_json: dict[str, Any],
        candidate_value_json: dict[str, Any],
        difference_value_json: dict[str, Any],
        rate_or_basis_json: dict[str, Any],
        signed_adjustment_amount: float | None,
        readiness_status: Any,
        potential_adjustment_flag: Any,
        notes: str,
        raw_value: float | None,
        source_precedence_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        source_precedence = source_precedence_override or {
            "rank": 4,
            "label": "county_configured_fallback_schedule",
            "quality_tier": "fallback_scaffold",
            "resolution_status": "monetized_fallback",
        }
        if readiness_status != "ready":
            return {
                "adjustment_line_order": line_order,
                "adjustment_type": adjustment_type,
                "source_method_code": "missing_basis_review_required",
                "rate_or_basis_json": {
                    **rate_or_basis_json,
                    "readiness_status": readiness_status,
                    "fallback_amount_suppressed": True,
                    "potential_adjustment_flag": bool(potential_adjustment_flag),
                    "source_precedence": {
                        "rank": 5,
                        "label": "exclude_instead_of_guessing",
                        "quality_tier": "unresolved_review_only",
                        "resolution_status": "review_only_unresolved",
                    },
                },
                "subject_value_json": subject_value_json,
                "candidate_value_json": candidate_value_json,
                "difference_value_json": difference_value_json,
                "signed_adjustment_amount": None,
                "adjustment_reliability_flag": "scaffold_review",
                "material_flag": False,
                "notes": f"{notes} Basis missing or review-gated; amount not monetized in this phase.",
            }

        material_flag = _is_material_amount(
            signed_adjustment_amount=signed_adjustment_amount,
            raw_value=raw_value,
        )
        return {
            "adjustment_line_order": line_order,
            "adjustment_type": adjustment_type,
            "source_method_code": source_method_code,
            "rate_or_basis_json": {
                **rate_or_basis_json,
                "readiness_status": readiness_status,
                "potential_adjustment_flag": bool(potential_adjustment_flag),
                "source_precedence": source_precedence,
            },
            "subject_value_json": subject_value_json,
            "candidate_value_json": candidate_value_json,
            "difference_value_json": difference_value_json,
            "signed_adjustment_amount": signed_adjustment_amount,
            "adjustment_reliability_flag": "scaffold",
            "material_flag": bool(material_flag and potential_adjustment_flag),
            "notes": notes,
        }

    def _non_monetized_line_item(
        self,
        *,
        line_order: int,
        adjustment_type: str,
        source_method_code: str,
        subject_value_json: dict[str, Any],
        candidate_value_json: dict[str, Any],
        difference_value_json: dict[str, Any],
        rate_or_basis_json: dict[str, Any],
        readiness_status: Any,
        potential_adjustment_flag: Any,
        notes: str,
    ) -> dict[str, Any]:
        reliability_flag = (
            "scaffold_review"
            if readiness_status != "ready" and potential_adjustment_flag
            else "not_monetized"
        )
        return {
            "adjustment_line_order": line_order,
            "adjustment_type": adjustment_type,
            "source_method_code": source_method_code,
            "rate_or_basis_json": {
                **rate_or_basis_json,
                "readiness_status": readiness_status,
                "potential_adjustment_flag": bool(potential_adjustment_flag),
                "source_precedence": {
                    "rank": None if reliability_flag == "not_monetized" else 5,
                    "label": (
                        "policy_non_monetized_channel"
                        if reliability_flag == "not_monetized"
                        else "exclude_instead_of_guessing"
                    ),
                    "quality_tier": (
                        "policy_non_monetized"
                        if reliability_flag == "not_monetized"
                        else "unresolved_review_only"
                    ),
                    "resolution_status": (
                        "non_monetized_policy_channel"
                        if reliability_flag == "not_monetized"
                        else "review_only_unresolved"
                    ),
                },
            },
            "subject_value_json": subject_value_json,
            "candidate_value_json": candidate_value_json,
            "difference_value_json": difference_value_json,
            "signed_adjustment_amount": None,
            "adjustment_reliability_flag": reliability_flag,
            "material_flag": False,
            "notes": notes,
        }

    def _build_preliminary_adjustment_summary(
        self,
        *,
        candidate: dict[str, Any],
        run_context: dict[str, Any],
        line_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        monetized_line_items = [
            line_item
            for line_item in line_items
            if line_item.get("signed_adjustment_amount") is not None
        ]
        raw_value = _as_float(candidate.get("appraised_value"))
        total_signed_adjustment = round(
            sum(_as_float(line_item.get("signed_adjustment_amount")) or 0.0 for line_item in monetized_line_items),
            2,
        )
        total_absolute_adjustment = round(
            sum(abs(_as_float(line_item.get("signed_adjustment_amount")) or 0.0) for line_item in monetized_line_items),
            2,
        )
        adjustment_pct_of_raw_value = (
            round(total_absolute_adjustment / raw_value, 4)
            if raw_value not in {None, 0.0}
            else None
        )
        material_adjustment_count = sum(
            1 for line_item in monetized_line_items if line_item.get("material_flag")
        )
        nontrivial_adjustment_sources_count = sum(
            1
            for line_item in monetized_line_items
            if _is_nontrivial_amount(
                signed_adjustment_amount=_as_float(line_item.get("signed_adjustment_amount")),
                raw_value=raw_value,
            )
        )
        adjusted_appraised_value = (
            round(raw_value + total_signed_adjustment, 2) if raw_value is not None else None
        )
        unresolved_material_difference_details = _unresolved_material_difference_details(
            line_items
        )
        unresolved_material_difference_channels = [
            detail["adjustment_type"] for detail in unresolved_material_difference_details
        ]
        burden_status = _burden_status(
            adjustment_pct_of_raw_value=adjustment_pct_of_raw_value,
            material_adjustment_count=material_adjustment_count,
        )
        source_governance = _source_governance_summary(line_items)
        review_carry_forward_flag = candidate.get("chosen_comp_status") == "review_chosen_comp"
        burden_governance = _burden_governance_summary(
            burden_status=burden_status,
            adjustment_pct_of_raw_value=adjustment_pct_of_raw_value,
            material_adjustment_count=material_adjustment_count,
            nontrivial_adjustment_sources_count=nontrivial_adjustment_sources_count,
            unresolved_material_difference_details=unresolved_material_difference_details,
            review_carry_forward_flag=review_carry_forward_flag,
            adjusted_conflict_indicator_flag=False,
        )
        dominant_adjustment_channel = _dominant_adjustment_channel(line_items)
        chosen_comp_detail_json = dict(candidate.get("chosen_comp_detail_json") or {})
        acceptable_zone_context = dict(
            (chosen_comp_detail_json.get("chosen_comp_context") or {})
        )
        adjusted_set_governance = _adjusted_set_governance_summary(
            burden_governance=burden_governance,
            source_governance=source_governance,
            review_carry_forward_flag=review_carry_forward_flag,
            acceptable_zone_admitted_flag=bool(
                acceptable_zone_context.get("acceptable_zone_admitted_flag")
            ),
            adjusted_conflict_indicator_flag=False,
            raw_adjusted_divergence_flag=False,
            raw_adjusted_rank_shift=None,
            adjusted_value_outlier_flag=False,
            adjusted_value_per_sf_outlier_flag=False,
        )
        return {
            "adjusted_appraised_value": adjusted_appraised_value,
            "total_signed_adjustment": total_signed_adjustment,
            "total_absolute_adjustment": total_absolute_adjustment,
            "adjustment_pct_of_raw_value": adjustment_pct_of_raw_value,
            "material_adjustment_count": material_adjustment_count,
            "nontrivial_adjustment_sources_count": nontrivial_adjustment_sources_count,
            "adjustment_summary_json": {
                "adjustment_math_version": ADJUSTMENT_MATH_VERSION,
                "adjustment_math_config_version": ADJUSTMENT_MATH_CONFIG_VERSION,
                "adjustment_math_status": self._adjustment_math_status_from_support(candidate),
                "review_carry_forward_flag": review_carry_forward_flag,
                "acceptable_zone_governance": {
                    "acceptable_zone_admitted_flag": acceptable_zone_context.get(
                        "acceptable_zone_admitted_flag"
                    ),
                    "acceptable_zone_candidate_flag": acceptable_zone_context.get(
                        "acceptable_zone_candidate_flag"
                    ),
                    "acceptable_zone_exclusion_reason_code": acceptable_zone_context.get(
                        "acceptable_zone_exclusion_reason_code"
                    ),
                    "acceptable_zone_evaluation": dict(
                        (chosen_comp_detail_json.get("score_context") or {}).get(
                            "acceptable_zone_evaluation"
                        )
                        or {}
                    ),
                },
                "line_item_summary": {
                    "line_item_count": len(line_items),
                    "monetized_line_item_count": len(monetized_line_items),
                    "non_monetized_line_item_count": len(line_items) - len(monetized_line_items),
                },
                "burden_summary": {
                    "total_signed_adjustment": total_signed_adjustment,
                    "total_absolute_adjustment": total_absolute_adjustment,
                    "adjustment_pct_of_raw_value": adjustment_pct_of_raw_value,
                    "material_adjustment_count": material_adjustment_count,
                    "nontrivial_adjustment_sources_count": (
                        nontrivial_adjustment_sources_count
                    ),
                    "unresolved_material_difference_channels": (
                        unresolved_material_difference_channels
                    ),
                    "unresolved_material_difference_details": (
                        unresolved_material_difference_details
                    ),
                    "burden_status": burden_status,
                    "review_threshold_pct_of_raw_value": (
                        ADJUSTMENT_BURDEN_REVIEW_PCT_THRESHOLD
                    ),
                    "exclude_threshold_pct_of_raw_value": (
                        ADJUSTMENT_BURDEN_EXCLUDE_PCT_THRESHOLD
                    ),
                    "review_threshold_material_channel_count": (
                        ADJUSTMENT_BURDEN_REVIEW_CHANNEL_COUNT
                    ),
                    "exclude_threshold_material_channel_count": (
                        ADJUSTMENT_BURDEN_EXCLUDE_CHANNEL_COUNT
                    ),
                },
                "source_governance": source_governance,
                "burden_governance": burden_governance,
                "adjusted_set_governance": adjusted_set_governance,
                "dispersion_scaffolding": {
                    "status": "pending_run_level_evaluation",
                },
                "adjustment_conflict_support": {
                    "dominant_adjustment_channel": dominant_adjustment_channel,
                    "raw_adjusted_divergence_flag": False,
                    "high_adjustment_driver_flag": burden_governance["status"]
                    in {"manual_review_recommended", "exclude_recommended"},
                },
                "bathroom_boundary_context": dict(
                    (candidate.get("adjustment_support_detail_json") or {}).get(
                        "bathroom_boundary_context"
                    )
                    or {}
                ),
            },
        }

    def _excluded_adjustment_summary_json(
        self,
        *,
        candidate: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "adjustment_math_version": ADJUSTMENT_MATH_VERSION,
            "adjustment_math_config_version": ADJUSTMENT_MATH_CONFIG_VERSION,
            "adjustment_math_status": "excluded_from_adjustment_math",
            "adjustment_math_context": {
                "exclusion_reason_code": self._adjustment_math_exclusion_reason_code(candidate),
                "selection_governance_status": run_context.get("selection_governance_status"),
            },
        }

    def _build_dispersion_support(
        self,
        adjusted_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        raw_values = [
            candidate["raw_appraised_value"]
            for candidate in adjusted_candidates
            if candidate["raw_appraised_value"] is not None
        ]
        raw_values_per_sf = [
            candidate["raw_appraised_value_per_sf"]
            for candidate in adjusted_candidates
            if candidate["raw_appraised_value_per_sf"] is not None
        ]
        adjusted_values = [
            candidate["adjusted_appraised_value"]
            for candidate in adjusted_candidates
            if candidate["adjusted_appraised_value"] is not None
        ]
        adjusted_values_per_sf = [
            candidate["adjusted_appraised_value_per_sf"]
            for candidate in adjusted_candidates
            if candidate["adjusted_appraised_value_per_sf"] is not None
        ]
        raw_value_stats = _iqr_stats(raw_values)
        raw_value_per_sf_stats = _iqr_stats(raw_values_per_sf)
        adjusted_value_stats = _iqr_stats(adjusted_values)
        adjusted_value_per_sf_stats = _iqr_stats(adjusted_values_per_sf)
        adjusted_median_all = adjusted_value_stats.get("median")
        trimmed_adjusted_values = sorted(adjusted_values)[1:-1] if len(adjusted_values) > 2 else list(adjusted_values)
        trimmed_median_adjusted_value = (
            round(median(trimmed_adjusted_values), 2)
            if trimmed_adjusted_values
            else adjusted_median_all
        )
        max_leave_one_out_delta = _max_leave_one_out_delta(adjusted_values)
        median_absolute_deviation_adjusted_values = _median_absolute_deviation(
            adjusted_values
        )
        raw_rank_map = _rank_map(
            adjusted_candidates,
            value_key="raw_appraised_value_per_sf",
        )
        adjusted_rank_map = _rank_map(
            adjusted_candidates,
            value_key="adjusted_appraised_value_per_sf",
        )

        candidate_flags: dict[str, dict[str, Any]] = {}
        for candidate in adjusted_candidates:
            candidate_id = candidate["candidate_id"]
            raw_rank = raw_rank_map.get(candidate_id)
            adjusted_rank = adjusted_rank_map.get(candidate_id)
            raw_adjusted_rank_shift = (
                abs(raw_rank - adjusted_rank)
                if raw_rank is not None and adjusted_rank is not None
                else None
            )
            raw_adjusted_divergence_flag = bool(
                raw_adjusted_rank_shift is not None
                and raw_adjusted_rank_shift >= DISPERSION_DIVERGENCE_RANK_SHIFT_THRESHOLD
            )
            candidate_flags[candidate["candidate_id"]] = {
                "raw_appraised_value": candidate["raw_appraised_value"],
                "raw_appraised_value_per_sf": candidate["raw_appraised_value_per_sf"],
                "adjusted_appraised_value": candidate["adjusted_appraised_value"],
                "adjusted_appraised_value_per_sf": candidate["adjusted_appraised_value_per_sf"],
                "raw_value_outlier_flag": _outside_fences(
                    candidate["raw_appraised_value"], raw_value_stats
                ),
                "raw_value_per_sf_outlier_flag": _outside_fences(
                    candidate["raw_appraised_value_per_sf"], raw_value_per_sf_stats
                ),
                "adjusted_value_outlier_flag": _outside_fences(
                    candidate["adjusted_appraised_value"], adjusted_value_stats
                ),
                "adjusted_value_per_sf_outlier_flag": _outside_fences(
                    candidate["adjusted_appraised_value_per_sf"],
                    adjusted_value_per_sf_stats,
                ),
                "raw_adjusted_rank_shift": raw_adjusted_rank_shift,
                "raw_adjusted_divergence_flag": raw_adjusted_divergence_flag,
                "adjusted_conflict_indicator_flag": bool(
                    raw_adjusted_divergence_flag
                    or _outside_fences(candidate["adjusted_appraised_value"], adjusted_value_stats)
                    or _outside_fences(
                        candidate["adjusted_appraised_value_per_sf"],
                        adjusted_value_per_sf_stats,
                    )
                ),
            }

        return {
            "candidate_flags": candidate_flags,
            "run_summary": {
                "status": "evaluated_iqr_scaffold",
                "median_all": adjusted_median_all,
                "median_minus_high_low": trimmed_median_adjusted_value,
                "trimmed_median_adjusted_value": trimmed_median_adjusted_value,
                "max_leave_one_out_delta": max_leave_one_out_delta,
                "median_absolute_deviation_adjusted_values": (
                    median_absolute_deviation_adjusted_values
                ),
                "adjusted_value_iqr": adjusted_value_stats.get("iqr"),
                "raw_value_stats": raw_value_stats,
                "raw_value_per_sf_stats": raw_value_per_sf_stats,
                "adjusted_value_stats": adjusted_value_stats,
                "adjusted_value_per_sf_stats": adjusted_value_per_sf_stats,
                "raw_adjusted_divergence_summary": {
                    "median_rank_shift": _median_or_none(
                        [
                            flags["raw_adjusted_rank_shift"]
                            for flags in candidate_flags.values()
                            if flags["raw_adjusted_rank_shift"] is not None
                        ]
                    ),
                    "max_rank_shift": _max_or_none(
                        [
                            flags["raw_adjusted_rank_shift"]
                            for flags in candidate_flags.values()
                            if flags["raw_adjusted_rank_shift"] is not None
                        ]
                    ),
                    "divergence_flag": any(
                        flags["raw_adjusted_divergence_flag"]
                        for flags in candidate_flags.values()
                    ),
                },
                "raw_value_outlier_count": sum(
                    1
                    for flags in candidate_flags.values()
                    if flags["raw_value_outlier_flag"]
                ),
                "raw_value_per_sf_outlier_count": sum(
                    1
                    for flags in candidate_flags.values()
                    if flags["raw_value_per_sf_outlier_flag"]
                ),
                "adjusted_value_outlier_count": sum(
                    1
                    for flags in candidate_flags.values()
                    if flags["adjusted_value_outlier_flag"]
                ),
                "adjusted_value_per_sf_outlier_count": sum(
                    1
                    for flags in candidate_flags.values()
                    if flags["adjusted_value_per_sf_outlier_flag"]
                ),
                "adjusted_conflict_indicator_count": sum(
                    1
                    for flags in candidate_flags.values()
                    if flags["adjusted_conflict_indicator_flag"]
                ),
            },
        }

    def _persist_candidate_adjustment_math(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        adjustment_math_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET adjustment_math_status = %s,
                adjustment_math_version = %s,
                adjustment_math_config_version = %s,
                adjustment_summary_json = %s,
                adjusted_appraised_value = %s,
                total_signed_adjustment = %s,
                total_absolute_adjustment = %s,
                adjustment_pct_of_raw_value = %s,
                material_adjustment_count = %s,
                nontrivial_adjustment_sources_count = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                adjustment_math_assignment["adjustment_math_status"],
                ADJUSTMENT_MATH_VERSION,
                ADJUSTMENT_MATH_CONFIG_VERSION,
                Jsonb(adjustment_math_assignment["adjustment_summary_json"]),
                adjustment_math_assignment["adjusted_appraised_value"],
                adjustment_math_assignment["total_signed_adjustment"],
                adjustment_math_assignment["total_absolute_adjustment"],
                adjustment_math_assignment["adjustment_pct_of_raw_value"],
                adjustment_math_assignment["material_adjustment_count"],
                adjustment_math_assignment["nontrivial_adjustment_sources_count"],
                unequal_roll_candidate_id,
            ),
        )

    def _insert_adjustment_line_item(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        line_item: dict[str, Any],
        candidate: dict[str, Any],
        unequal_roll_run_id: str,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO unequal_roll_adjustments (
              unequal_roll_run_id,
              unequal_roll_candidate_id,
              candidate_parcel_id,
              adjustment_line_order,
              adjustment_type,
              source_method_code,
              rate_or_basis_json,
              subject_value_json,
              candidate_value_json,
              difference_value_json,
              signed_adjustment_amount,
              adjustment_reliability_flag,
              material_flag,
              notes
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                unequal_roll_run_id,
                unequal_roll_candidate_id,
                candidate["candidate_parcel_id"],
                line_item["adjustment_line_order"],
                line_item["adjustment_type"],
                line_item["source_method_code"],
                Jsonb(line_item["rate_or_basis_json"]),
                Jsonb(line_item["subject_value_json"]),
                Jsonb(line_item["candidate_value_json"]),
                Jsonb(line_item["difference_value_json"]),
                line_item["signed_adjustment_amount"],
                line_item["adjustment_reliability_flag"],
                line_item["material_flag"],
                line_item["notes"],
            ),
        )

    def _persist_run_adjustment_log(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        adjustment_log_json: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_runs
            SET selection_log_json = %s,
                updated_at = now()
            WHERE unequal_roll_run_id = %s
            """,
            (Jsonb(adjustment_log_json), unequal_roll_run_id),
        )

    def _build_adjustment_log_json(
        self,
        *,
        run_context: dict[str, Any],
        adjustment_plan: dict[str, dict[str, Any]],
        dispersion_support: dict[str, Any],
    ) -> dict[str, Any]:
        selection_log_json = dict(run_context.get("selection_log_json") or {})
        assignments = list(adjustment_plan.values())
        run_dispersion_summary = selection_log_json.get("adjustment_math", {}).get(
            "dispersion_scaffolding"
        )
        non_excluded_assignments = [
            assignment
            for assignment in assignments
            if assignment["adjustment_math_status"] != "excluded_from_adjustment_math"
        ]
        line_items = [
            line_item
            for assignment in non_excluded_assignments
            for line_item in assignment["line_items"]
        ]
        source_governance = _source_governance_summary(line_items)
        burden_governance_counts = _count_by_key(
            non_excluded_assignments,
            lambda assignment: (
                assignment["adjustment_summary_json"]
                .get("burden_governance", {})
                .get("status")
            ),
        )
        adjusted_set_governance_counts = _count_by_key(
            non_excluded_assignments,
            lambda assignment: (
                assignment["adjustment_summary_json"]
                .get("adjusted_set_governance", {})
                .get("status")
            ),
        )
        average_adjustment_pct = _avg(
            [
                assignment["adjustment_pct_of_raw_value"]
                for assignment in non_excluded_assignments
                if assignment["adjustment_pct_of_raw_value"] is not None
            ]
        )
        max_adjustment_pct = _max_or_none(
            [
                assignment["adjustment_pct_of_raw_value"]
                for assignment in non_excluded_assignments
                if assignment["adjustment_pct_of_raw_value"] is not None
            ]
        )
        dominant_adjustment_channel = _dominant_adjustment_channel(line_items)
        run_dispersion_governance = {
            **(run_dispersion_summary or dispersion_support["run_summary"]),
            "average_adjustment_pct": average_adjustment_pct,
            "max_adjustment_pct": max_adjustment_pct,
            "dominant_adjustment_channel": dominant_adjustment_channel,
            "heavily_adjusted_candidate_count": sum(
                1
                for assignment in non_excluded_assignments
                if (
                    assignment["adjustment_summary_json"]
                    .get("burden_governance", {})
                    .get("status")
                    in {"manual_review_recommended", "exclude_recommended"}
                )
            ),
            "raw_sf_divergence_check_flag": (
                (run_dispersion_summary or dispersion_support["run_summary"])
                .get("raw_adjusted_divergence_summary", {})
                .get("divergence_flag", False)
            ),
        }
        selection_log_json["adjustment_math"] = {
            "adjustment_math_version": ADJUSTMENT_MATH_VERSION,
            "adjustment_math_config_version": ADJUSTMENT_MATH_CONFIG_VERSION,
            "status_counts": _count_by_key(
                assignments, lambda assignment: assignment["adjustment_math_status"]
            ),
            "source_governance": source_governance,
            "burden_summary": {
                "avg_adjustment_pct_of_raw_value": average_adjustment_pct,
                "max_adjustment_pct_of_raw_value": max_adjustment_pct,
                "material_adjustment_count_max": _max_or_none(
                    [
                        assignment["material_adjustment_count"]
                        for assignment in non_excluded_assignments
                        if assignment["material_adjustment_count"] is not None
                    ]
                ),
                "review_threshold_pct_of_raw_value": (
                    ADJUSTMENT_BURDEN_REVIEW_PCT_THRESHOLD
                ),
                "exclude_threshold_pct_of_raw_value": (
                    ADJUSTMENT_BURDEN_EXCLUDE_PCT_THRESHOLD
                ),
                "review_threshold_material_channel_count": (
                    ADJUSTMENT_BURDEN_REVIEW_CHANNEL_COUNT
                ),
                "exclude_threshold_material_channel_count": (
                    ADJUSTMENT_BURDEN_EXCLUDE_CHANNEL_COUNT
                ),
            },
            "burden_governance": {
                "status_counts": burden_governance_counts,
                "manual_review_recommended_count": burden_governance_counts.get(
                    "manual_review_recommended",
                    0,
                ),
                "exclude_recommended_count": burden_governance_counts.get(
                    "exclude_recommended",
                    0,
                ),
                "warning_count": burden_governance_counts.get("warning", 0),
                "within_thresholds_count": burden_governance_counts.get(
                    "within_thresholds",
                    0,
                ),
                "unresolved_material_difference_count": sum(
                    1
                    for assignment in non_excluded_assignments
                    if (
                        assignment["adjustment_summary_json"]
                        .get("burden_governance", {})
                        .get("unresolved_material_difference_count", 0)
                        > 0
                    )
                ),
            },
            "adjusted_set_governance": {
                "status_counts": adjusted_set_governance_counts,
                "usable_adjusted_comp_count": adjusted_set_governance_counts.get(
                    "usable_adjusted_comp",
                    0,
                ),
                "usable_with_review_adjusted_comp_count": (
                    adjusted_set_governance_counts.get(
                        "usable_with_review_adjusted_comp",
                        0,
                    )
                ),
                "review_heavy_adjusted_comp_count": adjusted_set_governance_counts.get(
                    "review_heavy_adjusted_comp",
                    0,
                ),
                "likely_exclude_adjusted_comp_count": (
                    adjusted_set_governance_counts.get(
                        "likely_exclude_adjusted_comp",
                        0,
                    )
                ),
            },
            "dispersion_scaffolding": run_dispersion_governance,
            "review_carry_forward_count": sum(
                1
                for assignment in assignments
                if assignment["adjustment_math_status"]
                in {"adjusted_with_review", "adjusted_limited_with_review"}
            ),
            "acceptable_zone_tail_count": sum(
                1
                for assignment in assignments
                if (
                    assignment["adjustment_summary_json"]
                    .get("acceptable_zone_governance", {})
                    .get("acceptable_zone_admitted_flag")
                )
            ),
        }
        return selection_log_json

    def _is_adjustment_math_eligible(self, candidate: dict[str, Any]) -> bool:
        return candidate.get("adjustment_support_status") in {
            "adjustment_ready",
            "adjustment_ready_with_review",
            "adjustment_limited",
            "adjustment_limited_with_review",
        }

    def _adjustment_math_status_from_support(self, candidate: dict[str, Any]) -> str:
        support_status = str(candidate.get("adjustment_support_status") or "")
        mapping = {
            "adjustment_ready": "adjusted",
            "adjustment_ready_with_review": "adjusted_with_review",
            "adjustment_limited": "adjusted_limited",
            "adjustment_limited_with_review": "adjusted_limited_with_review",
        }
        return mapping.get(support_status, "excluded_from_adjustment_math")

    def _adjustment_math_exclusion_reason_code(self, candidate: dict[str, Any]) -> str:
        support_status = str(candidate.get("adjustment_support_status") or "")
        if support_status == "excluded_from_adjustment_support":
            return "excluded_from_adjustment_support"
        return "adjustment_support_status_not_eligible"


def _source_governance_summary(line_items: list[dict[str, Any]]) -> dict[str, Any]:
    precedence_counts = _count_by_key(
        line_items,
        lambda line_item: (
            (line_item.get("rate_or_basis_json") or {})
            .get("source_precedence", {})
            .get("label")
        ),
    )
    quality_tier_counts = _count_by_key(
        line_items,
        lambda line_item: (
            (line_item.get("rate_or_basis_json") or {})
            .get("source_precedence", {})
            .get("quality_tier")
        ),
    )
    resolution_status_counts = _count_by_key(
        line_items,
        lambda line_item: (
            (line_item.get("rate_or_basis_json") or {})
            .get("source_precedence", {})
            .get("resolution_status")
        ),
    )
    unresolved_review_only_channels = [
        line_item["adjustment_type"]
        for line_item in line_items
        if (
            (line_item.get("rate_or_basis_json") or {})
            .get("source_precedence", {})
            .get("quality_tier")
            == "unresolved_review_only"
        )
    ]
    preferred_source_available_flag = any(
        (
            (line_item.get("rate_or_basis_json") or {})
            .get("source_precedence", {})
            .get("rank")
        )
        in {1, 2, 3}
        for line_item in line_items
    )
    if unresolved_review_only_channels and preferred_source_available_flag:
        source_governance_status = "hybrid_supported_with_unresolved_review_only"
    elif unresolved_review_only_channels:
        source_governance_status = "mixed_with_unresolved_review_only"
    elif preferred_source_available_flag:
        source_governance_status = "hybrid_supported_with_fallback"
    else:
        source_governance_status = "fallback_only"
    return {
        "source_precedence_policy": {
            "1": "canonical_cad_roll_or_direct_component_value",
            "2": "county_schedule_or_depreciation_table",
            "3": "county_supported_secondary_basis_or_local_roll_only_calibration",
            "4": "county_configured_fallback_schedule",
            "5": "exclude_instead_of_guessing",
        },
        "source_precedence_counts": precedence_counts,
        "source_quality_tier_counts": quality_tier_counts,
        "source_resolution_status_counts": resolution_status_counts,
        "monetized_fallback_channel_count": quality_tier_counts.get("fallback_scaffold", 0),
        "unresolved_review_only_channel_count": quality_tier_counts.get(
            "unresolved_review_only",
            0,
        ),
        "policy_non_monetized_channel_count": quality_tier_counts.get(
            "policy_non_monetized",
            0,
        ),
        "unresolved_review_only_channels": unresolved_review_only_channels,
        "preferred_source_available_flag": preferred_source_available_flag,
        "source_governance_status": source_governance_status,
    }


def _unresolved_material_difference_details(
    line_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for line_item in line_items:
        if line_item.get("signed_adjustment_amount") is not None:
            continue
        if line_item.get("adjustment_reliability_flag") != "scaffold_review":
            continue
        rate_or_basis_json = dict(line_item.get("rate_or_basis_json") or {})
        if not rate_or_basis_json.get("potential_adjustment_flag"):
            continue
        adjustment_type = str(line_item.get("adjustment_type") or "")
        details.append(
            {
                "adjustment_type": adjustment_type,
                "severity": UNRESOLVED_CHANNEL_SEVERITY_BY_TYPE.get(
                    adjustment_type,
                    "moderate",
                ),
                "source_method_code": line_item.get("source_method_code"),
                "readiness_status": rate_or_basis_json.get("readiness_status"),
                "source_precedence_label": (
                    rate_or_basis_json.get("source_precedence") or {}
                ).get("label"),
            }
        )
    return details


def _burden_governance_summary(
    *,
    burden_status: str,
    adjustment_pct_of_raw_value: float | None,
    material_adjustment_count: int,
    nontrivial_adjustment_sources_count: int,
    unresolved_material_difference_details: list[dict[str, Any]],
    review_carry_forward_flag: bool,
    adjusted_conflict_indicator_flag: bool,
) -> dict[str, Any]:
    reason_codes: list[str] = []
    status = "within_thresholds"
    unresolved_channel_impact = "none"
    unresolved_material_difference_channels = [
        detail["adjustment_type"] for detail in unresolved_material_difference_details
    ]
    severity_counts = Counter(
        str(detail.get("severity") or "moderate")
        for detail in unresolved_material_difference_details
    )
    if (
        adjustment_pct_of_raw_value is not None
        and adjustment_pct_of_raw_value > ADJUSTMENT_BURDEN_EXCLUDE_PCT_THRESHOLD
    ):
        status = "exclude_recommended"
        reason_codes.append("absolute_adjustment_pct_exceeds_exclude_threshold")
    elif (
        adjustment_pct_of_raw_value is not None
        and adjustment_pct_of_raw_value > ADJUSTMENT_BURDEN_REVIEW_PCT_THRESHOLD
        and status != "exclude_recommended"
    ):
        status = "manual_review_recommended"
        reason_codes.append("absolute_adjustment_pct_exceeds_review_threshold")
    if material_adjustment_count > ADJUSTMENT_BURDEN_EXCLUDE_CHANNEL_COUNT:
        status = "exclude_recommended"
        reason_codes.append("material_adjustment_channel_count_exceeds_exclude_threshold")
    elif (
        material_adjustment_count > ADJUSTMENT_BURDEN_REVIEW_CHANNEL_COUNT
        and status not in {"exclude_recommended"}
    ):
        status = "manual_review_recommended"
        reason_codes.append("material_adjustment_channel_count_exceeds_review_threshold")
    if (
        nontrivial_adjustment_sources_count >= 3
        and status == "within_thresholds"
    ):
        status = "warning"
        reason_codes.append("multiple_nontrivial_adjustment_sources_present")

    unresolved_count = len(unresolved_material_difference_details)
    high_unresolved_count = severity_counts.get("high", 0)
    moderate_unresolved_count = severity_counts.get("moderate", 0)

    if unresolved_count:
        unresolved_channel_impact = "warning"
        if (
            high_unresolved_count >= 2
            or unresolved_count >= UNRESOLVED_CHANNEL_EXCLUDE_COUNT_THRESHOLD
            or (
                high_unresolved_count >= 1
                and adjusted_conflict_indicator_flag
            )
            or (
                high_unresolved_count >= 1
                and review_carry_forward_flag
            )
        ):
            status = "exclude_recommended"
            unresolved_channel_impact = "exclude_recommended"
            if high_unresolved_count >= 2:
                reason_codes.append("multiple_high_severity_unresolved_channels_present")
            elif unresolved_count >= UNRESOLVED_CHANNEL_EXCLUDE_COUNT_THRESHOLD:
                reason_codes.append("unresolved_channel_count_exceeds_exclude_threshold")
            elif adjusted_conflict_indicator_flag:
                reason_codes.append("unresolved_channels_with_adjusted_conflict_indicator")
            elif review_carry_forward_flag:
                reason_codes.append("review_carry_forward_with_high_severity_unresolved_channel")
        elif (
            high_unresolved_count >= 1
            or moderate_unresolved_count >= 2
            or review_carry_forward_flag
            or burden_status == "review_threshold_exceeded"
            or adjusted_conflict_indicator_flag
        ):
            if status != "exclude_recommended":
                status = "manual_review_recommended"
            unresolved_channel_impact = "manual_review_recommended"
            if high_unresolved_count >= 1:
                reason_codes.append("high_severity_unresolved_channel_present")
            elif moderate_unresolved_count >= 2:
                reason_codes.append("multiple_moderate_unresolved_channels_present")
            elif review_carry_forward_flag:
                reason_codes.append("review_carry_forward_with_unresolved_channel_present")
            elif adjusted_conflict_indicator_flag:
                reason_codes.append("unresolved_channels_with_adjusted_conflict_indicator")
            else:
                reason_codes.append("unresolved_channels_require_manual_review")
        else:
            if status == "within_thresholds":
                status = "warning"
            unresolved_channel_impact = "warning"
            reason_codes.append("limited_unresolved_channels_present")

    return {
        "status": status,
        "reason_codes": reason_codes,
        "unresolved_material_difference_channels": unresolved_material_difference_channels,
        "unresolved_material_difference_count": len(unresolved_material_difference_channels),
        "unresolved_material_difference_details": unresolved_material_difference_details,
        "unresolved_channel_severity_counts": dict(severity_counts),
        "unresolved_channel_impact": unresolved_channel_impact,
        "review_carry_forward_flag": review_carry_forward_flag,
        "adjusted_conflict_indicator_flag": adjusted_conflict_indicator_flag,
        "review_recommended_flag": status == "manual_review_recommended",
        "exclude_recommended_flag": status == "exclude_recommended",
    }


def _adjusted_set_governance_summary(
    *,
    burden_governance: dict[str, Any],
    source_governance: dict[str, Any],
    review_carry_forward_flag: bool,
    acceptable_zone_admitted_flag: bool,
    adjusted_conflict_indicator_flag: bool,
    raw_adjusted_divergence_flag: bool,
    raw_adjusted_rank_shift: int | None = None,
    adjusted_value_outlier_flag: bool = False,
    adjusted_value_per_sf_outlier_flag: bool = False,
) -> dict[str, Any]:
    burden_status = str(burden_governance.get("status") or "within_thresholds")
    source_status = str(source_governance.get("source_governance_status") or "fallback_only")
    unresolved_review_only_count = (
        _as_int(source_governance.get("unresolved_review_only_channel_count")) or 0
    )
    preferred_source_available_flag = bool(
        source_governance.get("preferred_source_available_flag")
    )

    status = "usable_adjusted_comp"
    reason_codes: list[str] = []
    adjusted_outlier_conflict_flag = bool(
        adjusted_value_outlier_flag or adjusted_value_per_sf_outlier_flag
    )
    strong_divergence_flag = bool(
        raw_adjusted_rank_shift is not None
        and raw_adjusted_rank_shift >= STRONG_CONFLICT_DIVERGENCE_RANK_SHIFT_THRESHOLD
    )
    clean_chosen_flag = not review_carry_forward_flag
    low_burden_clean_chosen_flag = (
        burden_status == "within_thresholds"
        and clean_chosen_flag
        and unresolved_review_only_count == 0
        and not acceptable_zone_admitted_flag
        and not source_status.startswith("hybrid_supported_")
    )
    divergence_requires_review_flag = bool(
        raw_adjusted_divergence_flag
        and (
            strong_divergence_flag
            or not low_burden_clean_chosen_flag
            or adjusted_outlier_conflict_flag
        )
    )
    mild_divergence_only_flag = bool(
        raw_adjusted_divergence_flag
        and not divergence_requires_review_flag
        and not adjusted_outlier_conflict_flag
    )
    unresolved_review_only_conflict_escalation_flag = bool(
        unresolved_review_only_count > 0 and adjusted_outlier_conflict_flag
    )
    review_carry_forward_unresolved_escalation_flag = bool(
        unresolved_review_only_count > 0
        and review_carry_forward_flag
        and (
            burden_status != "within_thresholds"
            or adjusted_outlier_conflict_flag
            or strong_divergence_flag
        )
    )

    if burden_status == "exclude_recommended":
        status = "likely_exclude_adjusted_comp"
        reason_codes.append("burden_governance_exclude_recommended")
    elif (
        burden_status == "manual_review_recommended"
        or unresolved_review_only_conflict_escalation_flag
        or review_carry_forward_unresolved_escalation_flag
    ):
        status = "review_heavy_adjusted_comp"
        if burden_status == "manual_review_recommended":
            reason_codes.append("burden_governance_manual_review_recommended")
        if unresolved_review_only_conflict_escalation_flag:
            reason_codes.append("unresolved_review_only_with_adjusted_conflict_indicator")
        if review_carry_forward_unresolved_escalation_flag:
            reason_codes.append("review_carry_forward_with_unresolved_review_only_channel")
    elif (
        burden_status == "warning"
        or acceptable_zone_admitted_flag
        or review_carry_forward_flag
        or adjusted_outlier_conflict_flag
        or divergence_requires_review_flag
        or unresolved_review_only_count > 0
        or source_status.startswith("hybrid_supported_")
    ):
        status = "usable_with_review_adjusted_comp"
        if burden_status == "warning":
            reason_codes.append("burden_governance_warning")
        if acceptable_zone_admitted_flag:
            reason_codes.append("acceptable_zone_tail_requires_review_visibility")
        if review_carry_forward_flag:
            reason_codes.append("review_carry_forward_requires_review_visibility")
        if adjusted_outlier_conflict_flag:
            reason_codes.append("adjusted_conflict_indicator_requires_review")
        if divergence_requires_review_flag:
            reason_codes.append("raw_adjusted_divergence_requires_review")
        if unresolved_review_only_count > 0:
            reason_codes.append("unresolved_review_only_channels_present")
        elif source_status.startswith("hybrid_supported_"):
            reason_codes.append("hybrid_supported_source_requires_review_visibility")

    return {
        "status": status,
        "reason_codes": reason_codes,
        "conflict_divergence_governance": {
            "raw_adjusted_rank_shift": raw_adjusted_rank_shift,
            "raw_adjusted_divergence_flag": raw_adjusted_divergence_flag,
            "strong_divergence_flag": strong_divergence_flag,
            "adjusted_conflict_indicator_flag": adjusted_conflict_indicator_flag,
            "adjusted_value_outlier_flag": adjusted_value_outlier_flag,
            "adjusted_value_per_sf_outlier_flag": adjusted_value_per_sf_outlier_flag,
            "adjusted_outlier_conflict_flag": adjusted_outlier_conflict_flag,
            "divergence_requires_review_flag": divergence_requires_review_flag,
            "mild_divergence_only_flag": mild_divergence_only_flag,
            "low_burden_clean_chosen_flag": low_burden_clean_chosen_flag,
            "unresolved_review_only_conflict_escalation_flag": (
                unresolved_review_only_conflict_escalation_flag
            ),
            "review_carry_forward_unresolved_escalation_flag": (
                review_carry_forward_unresolved_escalation_flag
            ),
        },
        "source_governance_status": source_status,
        "burden_governance_status": burden_status,
        "preferred_source_available_flag": preferred_source_available_flag,
        "unresolved_review_only_channel_count": unresolved_review_only_count,
        "review_carry_forward_flag": review_carry_forward_flag,
        "acceptable_zone_admitted_flag": acceptable_zone_admitted_flag,
        "adjusted_conflict_indicator_flag": adjusted_conflict_indicator_flag,
        "raw_adjusted_divergence_flag": raw_adjusted_divergence_flag,
        "hybrid_supported_source_flag": source_status.startswith("hybrid_supported_"),
        "review_required_flag": status != "usable_adjusted_comp",
        "likely_exclude_flag": status == "likely_exclude_adjusted_comp",
    }


def _dominant_adjustment_channel(line_items: list[dict[str, Any]]) -> str | None:
    totals: dict[str, float] = {}
    for line_item in line_items:
        amount = _as_float(line_item.get("signed_adjustment_amount"))
        if amount is None:
            continue
        adjustment_type = str(line_item.get("adjustment_type") or "")
        totals[adjustment_type] = totals.get(adjustment_type, 0.0) + abs(amount)
    if not totals:
        return None
    return max(totals.items(), key=lambda item: item[1])[0]


def _max_leave_one_out_delta(values: list[float]) -> float | None:
    if len(values) <= 1:
        return None
    overall_median = median(values)
    deltas = []
    for index in range(len(values)):
        subset = values[:index] + values[index + 1 :]
        if not subset:
            continue
        deltas.append(abs(median(subset) - overall_median))
    if not deltas:
        return None
    return round(max(deltas), 2)


def _median_absolute_deviation(values: list[float]) -> float | None:
    if not values:
        return None
    center = median(values)
    deviations = [abs(value - center) for value in values]
    return round(median(deviations), 2)


def _rank_map(
    candidates: list[dict[str, Any]],
    *,
    value_key: str,
) -> dict[str, int]:
    present = [
        candidate
        for candidate in candidates
        if candidate.get(value_key) is not None
    ]
    present.sort(
        key=lambda candidate: (
            candidate.get(value_key),
            candidate["candidate_id"],
        )
    )
    return {
        candidate["candidate_id"]: index
        for index, candidate in enumerate(present, start=1)
    }


def _count_by_key(rows: list[Any], key_fn: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = key_fn(row)
        if key in {None, ""}:
            continue
        normalized_key = str(key)
        counts[normalized_key] = counts.get(normalized_key, 0) + 1
    return counts


def _as_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _bool_to_int(value: Any) -> int | None:
    bool_value = _as_bool(value)
    if bool_value is None:
        return None
    return 1 if bool_value else 0


def _signed_product(diff: float | None, rate: float | None) -> float | None:
    if diff is None or rate is None:
        return None
    return round(diff * rate, 2)


def _difference(subject_value: float | None, candidate_value: float | None) -> float | None:
    if subject_value is None or candidate_value is None:
        return None
    return round(subject_value - candidate_value, 4)


def _value_per_sf(value: float | None, living_area_sf: float | None) -> float | None:
    if value in {None, 0.0} or living_area_sf in {None, 0.0}:
        return None
    return round(value / living_area_sf, 2)


def _is_material_amount(*, signed_adjustment_amount: float | None, raw_value: float | None) -> bool:
    if signed_adjustment_amount is None or raw_value in {None, 0.0}:
        return False
    return abs(signed_adjustment_amount) >= raw_value * MATERIAL_ADJUSTMENT_MIN_PCT_OF_RAW_VALUE


def _is_nontrivial_amount(
    *,
    signed_adjustment_amount: float | None,
    raw_value: float | None,
) -> bool:
    if signed_adjustment_amount is None or raw_value in {None, 0.0}:
        return False
    return abs(signed_adjustment_amount) >= raw_value * NONTRIVIAL_ADJUSTMENT_MIN_PCT_OF_RAW_VALUE


def _burden_status(
    *,
    adjustment_pct_of_raw_value: float | None,
    material_adjustment_count: int,
) -> str:
    if (
        adjustment_pct_of_raw_value is not None
        and adjustment_pct_of_raw_value > ADJUSTMENT_BURDEN_EXCLUDE_PCT_THRESHOLD
    ) or material_adjustment_count > ADJUSTMENT_BURDEN_EXCLUDE_CHANNEL_COUNT:
        return "exclude_threshold_exceeded"
    if (
        adjustment_pct_of_raw_value is not None
        and adjustment_pct_of_raw_value > ADJUSTMENT_BURDEN_REVIEW_PCT_THRESHOLD
    ) or material_adjustment_count > ADJUSTMENT_BURDEN_REVIEW_CHANNEL_COUNT:
        return "review_threshold_exceeded"
    return "within_thresholds"


def _iqr_stats(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "count": 0,
            "median": None,
            "q1": None,
            "q3": None,
            "iqr": None,
            "lower_fence": None,
            "upper_fence": None,
        }
    sorted_values = sorted(values)
    q1 = _percentile(sorted_values, 0.25)
    q3 = _percentile(sorted_values, 0.75)
    iqr = round(q3 - q1, 2)
    return {
        "count": len(sorted_values),
        "median": round(median(sorted_values), 2),
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower_fence": round(q1 - 1.5 * iqr, 2),
        "upper_fence": round(q3 + 1.5 * iqr, 2),
    }


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if len(sorted_values) == 1:
        return round(sorted_values[0], 2)
    index = (len(sorted_values) - 1) * percentile
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    weight = index - lower_index
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return round(lower_value + (upper_value - lower_value) * weight, 2)


def _outside_fences(value: float | None, stats: dict[str, Any]) -> bool:
    if value is None:
        return False
    lower_fence = stats.get("lower_fence")
    upper_fence = stats.get("upper_fence")
    if lower_fence is None or upper_fence is None:
        return False
    return value < lower_fence or value > upper_fence


def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _median_or_none(values: list[float | int]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 2)


def _max_or_none(values: list[float | int]) -> float | int | None:
    if not values:
        return None
    return max(values)
