from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.unequal_roll_candidate_normalization import ordinal_gap

ADJUSTMENT_SUPPORT_VERSION = "unequal_roll_adjustment_support_v1"
ADJUSTMENT_SUPPORT_CONFIG_VERSION = "unequal_roll_adjustment_support_v1"

FORT_BEND_EXACT_BATHROOM_SUPPORT_STATUSES = {"exact_supported"}
FORT_BEND_EXACT_BATHROOM_SUPPORT_CONFIDENCES = {"high"}

CRITICAL_ADJUSTMENT_CHANNELS = (
    "gla",
    "age_or_effective_age",
    "quality",
    "condition",
)

ADJUSTMENT_BURDEN_REVIEW_PCT_THRESHOLD = 0.20
ADJUSTMENT_BURDEN_EXCLUDE_PCT_THRESHOLD = 0.25
ADJUSTMENT_BURDEN_REVIEW_CHANNEL_COUNT = 4
ADJUSTMENT_BURDEN_EXCLUDE_CHANNEL_COUNT = 5


@dataclass(frozen=True)
class UnequalRollCandidateAdjustmentSupportResult:
    unequal_roll_run_id: str
    total_candidates: int
    adjustment_ready_count: int
    review_adjustment_ready_count: int
    adjustment_limited_count: int
    review_adjustment_limited_count: int
    excluded_from_adjustment_support_count: int


class UnequalRollCandidateAdjustmentSupportService:
    def build_adjustment_support_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateAdjustmentSupportResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                run_context = self._fetch_run_context(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if run_context is None:
                    raise LookupError(
                        "Unequal-roll run context not found for adjustment support "
                        f"{unequal_roll_run_id}."
                    )

                candidates = self._fetch_candidates(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if not candidates:
                    raise LookupError(
                        "Unequal-roll chosen-comp candidates not found for run "
                        f"{unequal_roll_run_id}."
                    )

                adjustment_support_plan = self._build_adjustment_support_plan(
                    candidates=candidates,
                    run_context=run_context,
                )
                adjustment_log_json = self._build_adjustment_log_json(
                    candidates=candidates,
                    run_context=run_context,
                    adjustment_support_plan=adjustment_support_plan,
                )
                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    self._persist_candidate_adjustment_support(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        adjustment_support_assignment=adjustment_support_plan[candidate_id],
                    )
                self._persist_run_adjustment_log(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                    adjustment_log_json=adjustment_log_json,
                )
            connection.commit()

        return UnequalRollCandidateAdjustmentSupportResult(
            unequal_roll_run_id=unequal_roll_run_id,
            total_candidates=len(candidates),
            adjustment_ready_count=sum(
                1
                for assignment in adjustment_support_plan.values()
                if assignment["adjustment_support_status"] == "adjustment_ready"
            ),
            review_adjustment_ready_count=sum(
                1
                for assignment in adjustment_support_plan.values()
                if assignment["adjustment_support_status"] == "adjustment_ready_with_review"
            ),
            adjustment_limited_count=sum(
                1
                for assignment in adjustment_support_plan.values()
                if assignment["adjustment_support_status"] == "adjustment_limited"
            ),
            review_adjustment_limited_count=sum(
                1
                for assignment in adjustment_support_plan.values()
                if assignment["adjustment_support_status"]
                == "adjustment_limited_with_review"
            ),
            excluded_from_adjustment_support_count=sum(
                1
                for assignment in adjustment_support_plan.values()
                if assignment["adjustment_support_status"]
                == "excluded_from_adjustment_support"
            ),
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
              urss.parcel_id AS subject_parcel_id,
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
              address,
              discovery_tier,
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
              source_provenance_json,
              candidate_snapshot_json,
              chosen_comp_position,
              chosen_comp_status,
              chosen_comp_version,
              chosen_comp_config_version,
              chosen_comp_detail_json,
              final_selection_support_status,
              shortlist_status,
              ranking_status,
              eligibility_status,
              eligibility_reason_code,
              normalized_similarity_score,
              raw_similarity_score,
              scoring_version,
              scoring_config_version,
              similarity_score_detail_json
            FROM unequal_roll_candidates
            WHERE unequal_roll_run_id = %s
            ORDER BY
              chosen_comp_position NULLS LAST,
              candidate_parcel_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _build_adjustment_support_plan(
        self,
        *,
        candidates: list[dict[str, Any]],
        run_context: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        plan: dict[str, dict[str, Any]] = {}
        chosen_candidates = [
            candidate
            for candidate in candidates
            if candidate.get("chosen_comp_status") in {"chosen_comp", "review_chosen_comp"}
        ]
        chosen_candidates.sort(key=self._adjustment_support_sort_key)

        for adjustment_support_position, candidate in enumerate(chosen_candidates, start=1):
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            detail_json = self._build_adjustment_support_detail_json(
                candidate=candidate,
                run_context=run_context,
            )
            readiness_status = detail_json["adjustment_readiness"]["overall_readiness_status"]
            chosen_comp_status = str(candidate.get("chosen_comp_status") or "")
            if readiness_status == "ready":
                adjustment_support_status = (
                    "adjustment_ready_with_review"
                    if chosen_comp_status == "review_chosen_comp"
                    else "adjustment_ready"
                )
            else:
                adjustment_support_status = (
                    "adjustment_limited_with_review"
                    if chosen_comp_status == "review_chosen_comp"
                    else "adjustment_limited"
                )
            detail_json["adjustment_support_status"] = adjustment_support_status
            detail_json["adjustment_support_position"] = adjustment_support_position
            plan[candidate_id] = {
                "adjustment_support_position": adjustment_support_position,
                "adjustment_support_status": adjustment_support_status,
                "adjustment_support_detail_json": detail_json,
            }

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in plan:
                continue
            plan[candidate_id] = {
                "adjustment_support_position": None,
                "adjustment_support_status": "excluded_from_adjustment_support",
                "adjustment_support_detail_json": self._excluded_adjustment_support_detail_json(
                    candidate=candidate,
                    run_context=run_context,
                ),
            }

        return plan

    def _persist_candidate_adjustment_support(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        adjustment_support_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET adjustment_support_position = %s,
                adjustment_support_status = %s,
                adjustment_support_version = %s,
                adjustment_support_config_version = %s,
                adjustment_support_detail_json = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                adjustment_support_assignment["adjustment_support_position"],
                adjustment_support_assignment["adjustment_support_status"],
                ADJUSTMENT_SUPPORT_VERSION,
                ADJUSTMENT_SUPPORT_CONFIG_VERSION,
                Jsonb(adjustment_support_assignment["adjustment_support_detail_json"]),
                unequal_roll_candidate_id,
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
            (
                Jsonb(adjustment_log_json),
                unequal_roll_run_id,
            ),
        )

    def _build_adjustment_support_detail_json(
        self,
        *,
        candidate: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        chosen_comp_detail_json = dict(candidate.get("chosen_comp_detail_json") or {})
        chosen_comp_context = dict(chosen_comp_detail_json.get("chosen_comp_context") or {})
        score_context = dict(chosen_comp_detail_json.get("score_context") or {})

        adjustment_channels = self._build_adjustment_channels(
            candidate=candidate,
            run_context=run_context,
        )
        critical_missing_channels = [
            channel_name
            for channel_name in CRITICAL_ADJUSTMENT_CHANNELS
            if adjustment_channels[channel_name]["readiness_status"] != "ready"
        ]
        optional_missing_channels = [
            channel_name
            for channel_name, channel_detail in adjustment_channels.items()
            if channel_name not in CRITICAL_ADJUSTMENT_CHANNELS
            and channel_detail["readiness_status"] != "ready"
        ]
        potential_adjustment_channels = [
            channel_name
            for channel_name, channel_detail in adjustment_channels.items()
            if channel_detail["readiness_status"] == "ready"
            and channel_detail["potential_adjustment_flag"]
        ]
        potential_unsupported_difference_channels = [
            channel_name
            for channel_name, channel_detail in adjustment_channels.items()
            if channel_detail["readiness_status"] != "ready"
            and channel_detail["potential_adjustment_flag"]
        ]
        raw_appraised_value = _as_float(candidate.get("appraised_value"))
        living_area_sf = _as_float(candidate.get("living_area_sf"))
        raw_appraised_value_per_sf = (
            round(raw_appraised_value / living_area_sf, 2)
            if raw_appraised_value not in {None, 0.0}
            and living_area_sf not in {None, 0.0}
            else None
        )
        overall_readiness_status = "ready" if not critical_missing_channels else "limited"

        valuation_bathroom_features = dict(
            (candidate.get("candidate_snapshot_json") or {}).get("valuation_bathroom_features")
            or {}
        )
        fort_bend_bathroom_modifier = dict(
            (candidate.get("similarity_score_detail_json") or {}).get(
                "fort_bend_bathroom_modifier"
            )
            or {}
        )

        return {
            "adjustment_support_version": ADJUSTMENT_SUPPORT_VERSION,
            "adjustment_support_config_version": ADJUSTMENT_SUPPORT_CONFIG_VERSION,
            "governance_carry_forward": {
                "chosen_comp_status": candidate.get("chosen_comp_status"),
                "chosen_comp_position": candidate.get("chosen_comp_position"),
                "review_carry_forward_flag": (
                    candidate.get("chosen_comp_status") == "review_chosen_comp"
                ),
                "review_carry_forward_reason_code": candidate.get("eligibility_reason_code"),
                "chosen_comp_count_status": run_context.get("final_comp_count_status"),
                "selection_governance_status": run_context.get("selection_governance_status"),
                "acceptable_zone_admitted_flag": chosen_comp_context.get(
                    "acceptable_zone_admitted_flag"
                ),
                "acceptable_zone_candidate_flag": chosen_comp_context.get(
                    "acceptable_zone_candidate_flag"
                ),
                "acceptable_zone_exclusion_reason_code": chosen_comp_context.get(
                    "acceptable_zone_exclusion_reason_code"
                ),
                "acceptable_zone_evaluation": dict(
                    score_context.get("acceptable_zone_evaluation") or {}
                ),
            },
            "adjustment_readiness": {
                "overall_readiness_status": overall_readiness_status,
                "critical_channels": list(CRITICAL_ADJUSTMENT_CHANNELS),
                "missing_critical_channels": critical_missing_channels,
                "missing_optional_channels": optional_missing_channels,
                "raw_appraised_value_available_flag": raw_appraised_value is not None,
                "raw_appraised_value_per_sf": raw_appraised_value_per_sf,
                "channel_ready_count": sum(
                    1
                    for channel_detail in adjustment_channels.values()
                    if channel_detail["readiness_status"] == "ready"
                ),
                "channel_review_required_count": sum(
                    1
                    for channel_detail in adjustment_channels.values()
                    if channel_detail["readiness_status"] != "ready"
                ),
            },
            "adjustment_burden_scaffolding": {
                "status": "initialized_not_evaluated",
                "absolute_adjustment_total": None,
                "absolute_adjustment_pct_of_raw_value": None,
                "material_adjustment_count": None,
                "nontrivial_adjustment_sources_count": None,
                "potential_adjustment_channel_count": len(potential_adjustment_channels),
                "potential_adjustment_channels": potential_adjustment_channels,
                "potential_unsupported_difference_channels": (
                    potential_unsupported_difference_channels
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
            "dispersion_scaffolding": {
                "status": "initialized_not_evaluated",
                "raw_appraised_value": raw_appraised_value,
                "raw_appraised_value_per_sf": raw_appraised_value_per_sf,
                "adjusted_appraised_value": None,
                "adjusted_appraised_value_per_sf": None,
                "raw_outlier_check_status": "not_evaluated",
                "adjusted_outlier_check_status": "not_evaluated",
                "selected_for_future_dispersion_set_flag": True,
            },
            "adjustment_channels": adjustment_channels,
            "bathroom_boundary_context": {
                "canonical_fields": {
                    "full_baths": _as_float(candidate.get("full_baths")),
                    "half_baths": _as_float(candidate.get("half_baths")),
                },
                "valuation_bathroom_features": {
                    "attachment_status": valuation_bathroom_features.get("attachment_status"),
                    "bathroom_count_status": valuation_bathroom_features.get(
                        "bathroom_count_status"
                    ),
                    "bathroom_count_confidence": valuation_bathroom_features.get(
                        "bathroom_count_confidence"
                    ),
                    "bathroom_equivalent_derived": valuation_bathroom_features.get(
                        "bathroom_equivalent_derived"
                    ),
                },
                "fort_bend_bathroom_modifier": fort_bend_bathroom_modifier,
                "canonical_fields_replaced_by_valuation_only_features_flag": False,
                "future_adjustment_guidance": (
                    "Preserve canonical full_baths/half_baths as the primary bathroom "
                    "count fields. Fort Bend valuation-only bathroom features remain "
                    "supporting context for future governed adjustment channels and must "
                    "not silently replace canonical bathroom counts."
                ),
            },
            "prior_pipeline_context": {
                "ranking_status": candidate.get("ranking_status"),
                "shortlist_status": candidate.get("shortlist_status"),
                "final_selection_support_status": candidate.get(
                    "final_selection_support_status"
                ),
                "chosen_comp_status": candidate.get("chosen_comp_status"),
                "normalized_similarity_score": _as_float(
                    candidate.get("normalized_similarity_score")
                ),
                "raw_similarity_score": _as_float(candidate.get("raw_similarity_score")),
            },
        }

    def _excluded_adjustment_support_detail_json(
        self,
        *,
        candidate: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "adjustment_support_version": ADJUSTMENT_SUPPORT_VERSION,
            "adjustment_support_config_version": ADJUSTMENT_SUPPORT_CONFIG_VERSION,
            "adjustment_support_status": "excluded_from_adjustment_support",
            "adjustment_support_position": None,
            "adjustment_support_context": {
                "eligible_flag": False,
                "exclusion_reason_code": self._adjustment_support_exclusion_reason_code(
                    candidate
                ),
            },
            "governance_carry_forward": {
                "chosen_comp_status": candidate.get("chosen_comp_status"),
                "chosen_comp_count_status": run_context.get("final_comp_count_status"),
                "selection_governance_status": run_context.get("selection_governance_status"),
            },
        }

    def _build_adjustment_log_json(
        self,
        *,
        candidates: list[dict[str, Any]],
        run_context: dict[str, Any],
        adjustment_support_plan: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        selection_log_json = dict(run_context.get("selection_log_json") or {})
        status_counts = _count_by_key(
            adjustment_support_plan.values(),
            lambda assignment: assignment["adjustment_support_status"],
        )
        adjustment_ready_candidates = [
            candidate
            for candidate in candidates
            if adjustment_support_plan[str(candidate["unequal_roll_candidate_id"])][
                "adjustment_support_status"
            ]
            in {
                "adjustment_ready",
                "adjustment_ready_with_review",
                "adjustment_limited",
                "adjustment_limited_with_review",
            }
        ]
        acceptable_zone_tail_count = sum(
            1
            for candidate in adjustment_ready_candidates
            if (
                adjustment_support_plan[str(candidate["unequal_roll_candidate_id"])][
                    "adjustment_support_detail_json"
                ]["governance_carry_forward"].get("acceptable_zone_admitted_flag")
            )
        )
        selection_log_json["adjustment_scaffolding"] = {
            "adjustment_support_version": ADJUSTMENT_SUPPORT_VERSION,
            "adjustment_support_config_version": ADJUSTMENT_SUPPORT_CONFIG_VERSION,
            "status_counts": status_counts,
            "review_carry_forward_count": sum(
                1
                for candidate in adjustment_ready_candidates
                if candidate.get("chosen_comp_status") == "review_chosen_comp"
            ),
            "acceptable_zone_tail_count": acceptable_zone_tail_count,
            "burden_scaffolding": {
                "status": "initialized_not_evaluated",
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
            "dispersion_scaffolding": {
                "status": "initialized_not_evaluated",
                "selected_comp_count": len(adjustment_ready_candidates),
                "raw_value_per_sf_available_count": sum(
                    1
                    for candidate in adjustment_ready_candidates
                    if _as_float(candidate.get("appraised_value")) not in {None, 0.0}
                    and _as_float(candidate.get("living_area_sf")) not in {None, 0.0}
                ),
                "adjusted_value_dispersion_status": "not_evaluated",
            },
            "governance_carry_forward": {
                "selection_governance_status": run_context.get("selection_governance_status"),
                "final_comp_count_status": run_context.get("final_comp_count_status"),
            },
        }
        return selection_log_json

    def _build_adjustment_channels(
        self,
        *,
        candidate: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        county_id = str(run_context.get("county_id") or candidate.get("county_id") or "")
        return {
            "gla": self._numeric_channel(
                subject_value=run_context.get("living_area_sf"),
                candidate_value=candidate.get("living_area_sf"),
                difference_method="pct_and_abs",
                materiality_floor=0.03,
            ),
            "age_or_effective_age": self._age_channel(
                subject_effective_age=run_context.get("effective_age"),
                candidate_effective_age=candidate.get("effective_age"),
                subject_year_built=run_context.get("year_built"),
                candidate_year_built=candidate.get("year_built"),
            ),
            "full_bath": self._bath_channel(
                bath_field_name="full_bath",
                subject_value=run_context.get("full_baths"),
                candidate_value=candidate.get("full_baths"),
                valuation_bathroom_features=(
                    (candidate.get("candidate_snapshot_json") or {}).get(
                        "valuation_bathroom_features"
                    )
                ),
            ),
            "half_bath": self._bath_channel(
                bath_field_name="half_bath",
                subject_value=run_context.get("half_baths"),
                candidate_value=candidate.get("half_baths"),
                valuation_bathroom_features=(
                    (candidate.get("candidate_snapshot_json") or {}).get(
                        "valuation_bathroom_features"
                    )
                ),
            ),
            "bedroom": self._numeric_channel(
                subject_value=run_context.get("bedrooms"),
                candidate_value=candidate.get("bedrooms"),
                difference_method="abs_only",
                materiality_floor=1.0,
            ),
            "story": self._numeric_channel(
                subject_value=run_context.get("stories"),
                candidate_value=candidate.get("stories"),
                difference_method="abs_only",
                materiality_floor=0.5,
            ),
            "quality": self._ordinal_channel(
                county_id=county_id,
                field_name="quality",
                subject_value=run_context.get("quality_code"),
                candidate_value=candidate.get("quality_code"),
            ),
            "condition": self._ordinal_channel(
                county_id=county_id,
                field_name="condition",
                subject_value=run_context.get("condition_code"),
                candidate_value=candidate.get("condition_code"),
            ),
            "pool": self._boolean_channel(
                subject_value=run_context.get("pool_flag"),
                candidate_value=candidate.get("pool_flag"),
            ),
            "land_site": self._site_channel(
                subject_land_sf=run_context.get("land_sf"),
                candidate_land_sf=candidate.get("land_sf"),
                subject_land_acres=run_context.get("land_acres"),
                candidate_land_acres=candidate.get("land_acres"),
            ),
        }

    def _numeric_channel(
        self,
        *,
        subject_value: Any,
        candidate_value: Any,
        difference_method: str,
        materiality_floor: float,
    ) -> dict[str, Any]:
        subject_number = _as_float(subject_value)
        candidate_number = _as_float(candidate_value)
        if subject_number is None or candidate_number is None:
            return {
                "readiness_status": "review_required",
                "subject_value": subject_number,
                "candidate_value": candidate_number,
                "difference_value": None,
                "difference_pct": None,
                "potential_adjustment_flag": False,
                "adjustment_basis_status": "rate_not_evaluated",
            }
        difference_value = round(subject_number - candidate_number, 4)
        difference_pct = _pct_diff(subject_number, candidate_number)
        if difference_method == "pct_and_abs":
            potential_adjustment_flag = (
                difference_pct is not None and difference_pct >= materiality_floor
            )
        else:
            potential_adjustment_flag = abs(difference_value) >= materiality_floor
        return {
            "readiness_status": "ready",
            "subject_value": subject_number,
            "candidate_value": candidate_number,
            "difference_value": difference_value,
            "difference_pct": difference_pct,
            "potential_adjustment_flag": potential_adjustment_flag,
            "adjustment_basis_status": "rate_not_evaluated",
        }

    def _age_channel(
        self,
        *,
        subject_effective_age: Any,
        candidate_effective_age: Any,
        subject_year_built: Any,
        candidate_year_built: Any,
    ) -> dict[str, Any]:
        subject_effective_age_value = _as_float(subject_effective_age)
        candidate_effective_age_value = _as_float(candidate_effective_age)
        if subject_effective_age_value is not None and candidate_effective_age_value is not None:
            difference_value = round(
                subject_effective_age_value - candidate_effective_age_value,
                4,
            )
            return {
                "readiness_status": "ready",
                "basis_field": "effective_age",
                "subject_value": subject_effective_age_value,
                "candidate_value": candidate_effective_age_value,
                "difference_value": difference_value,
                "potential_adjustment_flag": abs(difference_value) >= 3.0,
                "adjustment_basis_status": "rate_not_evaluated",
            }

        subject_year_value = _as_int(subject_year_built)
        candidate_year_value = _as_int(candidate_year_built)
        if subject_year_value is not None and candidate_year_value is not None:
            difference_value = subject_year_value - candidate_year_value
            return {
                "readiness_status": "ready",
                "basis_field": "year_built",
                "subject_value": subject_year_value,
                "candidate_value": candidate_year_value,
                "difference_value": difference_value,
                "potential_adjustment_flag": abs(difference_value) >= 5,
                "adjustment_basis_status": "rate_not_evaluated",
            }

        return {
            "readiness_status": "review_required",
            "basis_field": None,
            "subject_value": subject_effective_age_value or subject_year_value,
            "candidate_value": candidate_effective_age_value or candidate_year_value,
            "difference_value": None,
            "potential_adjustment_flag": False,
            "adjustment_basis_status": "rate_not_evaluated",
        }

    def _bath_channel(
        self,
        *,
        bath_field_name: str,
        subject_value: Any,
        candidate_value: Any,
        valuation_bathroom_features: Any,
    ) -> dict[str, Any]:
        subject_number = _as_float(subject_value)
        candidate_number = _as_float(candidate_value)
        valuation_bathroom_features_json = dict(valuation_bathroom_features or {})
        derived_basis = _fort_bend_supported_bath_basis(
            bath_field_name=bath_field_name,
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        if subject_number is not None and candidate_number is None and derived_basis is not None:
            difference_value = round(subject_number - derived_basis["candidate_value"], 4)
            return {
                "readiness_status": "ready",
                "subject_value": subject_number,
                "candidate_value": derived_basis["candidate_value"],
                "difference_value": difference_value,
                "potential_adjustment_flag": difference_value != 0.0,
                "adjustment_basis_status": "county_secondary_basis_supported",
                "basis_source_code": "fort_bend_valuation_bathroom_features_exact",
                "basis_source_reason_code": "canonical_candidate_missing_county_exact_support_used",
                "secondary_source_used_flag": True,
                "canonical_candidate_missing_flag": True,
                "valuation_support_present_flag": bool(valuation_bathroom_features_json),
                "valuation_support_attachment_status": valuation_bathroom_features_json.get(
                    "attachment_status"
                ),
                "valuation_support_auto_usable_flag": True,
                "valuation_support_basis_field": derived_basis["basis_field"],
                "valuation_support_basis_value": derived_basis["candidate_value"],
                "valuation_support_basis_status": derived_basis["bathroom_count_status"],
                "valuation_support_basis_confidence": derived_basis[
                    "bathroom_count_confidence"
                ],
            }
        if subject_number is None or candidate_number is None:
            return {
                "readiness_status": "review_required",
                "subject_value": subject_number,
                "candidate_value": candidate_number,
                "difference_value": None,
                "potential_adjustment_flag": False,
                "adjustment_basis_status": "rate_not_evaluated",
                "basis_source_code": "canonical_roll",
                "basis_source_reason_code": "canonical_bathroom_count_missing",
                "secondary_source_used_flag": False,
                "canonical_candidate_missing_flag": candidate_number is None,
                "valuation_support_present_flag": bool(valuation_bathroom_features_json),
                "valuation_support_attachment_status": valuation_bathroom_features_json.get(
                    "attachment_status"
                ),
                "valuation_support_auto_usable_flag": False,
                "valuation_support_basis_field": (
                    derived_basis["basis_field"] if derived_basis is not None else None
                ),
                "valuation_support_basis_value": (
                    derived_basis["candidate_value"] if derived_basis is not None else None
                ),
                "valuation_support_basis_status": valuation_bathroom_features_json.get(
                    "bathroom_count_status"
                ),
                "valuation_support_basis_confidence": valuation_bathroom_features_json.get(
                    "bathroom_count_confidence"
                ),
            }
        difference_value = round(subject_number - candidate_number, 4)
        return {
            "readiness_status": "ready",
            "subject_value": subject_number,
            "candidate_value": candidate_number,
            "difference_value": difference_value,
            "potential_adjustment_flag": difference_value != 0.0,
            "adjustment_basis_status": "rate_not_evaluated",
            "basis_source_code": "canonical_roll",
            "basis_source_reason_code": "canonical_bathroom_count_available",
            "secondary_source_used_flag": False,
            "canonical_candidate_missing_flag": False,
            "valuation_support_present_flag": bool(valuation_bathroom_features_json),
            "valuation_support_attachment_status": valuation_bathroom_features_json.get(
                "attachment_status"
            ),
            "valuation_support_auto_usable_flag": False,
            "valuation_support_basis_field": None,
            "valuation_support_basis_value": None,
            "valuation_support_basis_status": valuation_bathroom_features_json.get(
                "bathroom_count_status"
            ),
            "valuation_support_basis_confidence": valuation_bathroom_features_json.get(
                "bathroom_count_confidence"
            ),
        }

    def _ordinal_channel(
        self,
        *,
        county_id: str,
        field_name: str,
        subject_value: Any,
        candidate_value: Any,
    ) -> dict[str, Any]:
        subject_text = str(subject_value or "").strip()
        candidate_text = str(candidate_value or "").strip()
        if not subject_text or not candidate_text:
            return {
                "readiness_status": "review_required",
                "subject_value": subject_value,
                "candidate_value": candidate_value,
                "difference_value": None,
                "potential_adjustment_flag": False,
                "adjustment_basis_status": "rate_not_evaluated",
            }
        gap_steps = ordinal_gap(
            county_id=county_id,
            field_name=field_name,
            subject_value=subject_value,
            candidate_value=candidate_value,
        )
        return {
            "readiness_status": "ready",
            "subject_value": subject_value,
            "candidate_value": candidate_value,
            "difference_value": gap_steps,
            "potential_adjustment_flag": bool(gap_steps),
            "adjustment_basis_status": "rate_not_evaluated",
        }

    def _boolean_channel(self, *, subject_value: Any, candidate_value: Any) -> dict[str, Any]:
        subject_bool = _as_bool(subject_value)
        candidate_bool = _as_bool(candidate_value)
        if subject_bool is None or candidate_bool is None:
            return {
                "readiness_status": "review_required",
                "subject_value": subject_bool,
                "candidate_value": candidate_bool,
                "difference_value": None,
                "potential_adjustment_flag": False,
                "adjustment_basis_status": "rate_not_evaluated",
            }
        difference_value = subject_bool != candidate_bool
        return {
            "readiness_status": "ready",
            "subject_value": subject_bool,
            "candidate_value": candidate_bool,
            "difference_value": difference_value,
            "potential_adjustment_flag": difference_value,
            "adjustment_basis_status": "rate_not_evaluated",
        }

    def _site_channel(
        self,
        *,
        subject_land_sf: Any,
        candidate_land_sf: Any,
        subject_land_acres: Any,
        candidate_land_acres: Any,
    ) -> dict[str, Any]:
        subject_land_sf_value = _as_float(subject_land_sf)
        candidate_land_sf_value = _as_float(candidate_land_sf)
        if subject_land_sf_value is not None and candidate_land_sf_value is not None:
            difference_value = round(subject_land_sf_value - candidate_land_sf_value, 4)
            difference_pct = _pct_diff(subject_land_sf_value, candidate_land_sf_value)
            return {
                "readiness_status": "ready",
                "basis_field": "land_sf",
                "subject_value": subject_land_sf_value,
                "candidate_value": candidate_land_sf_value,
                "difference_value": difference_value,
                "difference_pct": difference_pct,
                "potential_adjustment_flag": (
                    difference_pct is not None and difference_pct >= 0.10
                ),
                "adjustment_basis_status": "rate_not_evaluated",
            }

        subject_land_acres_value = _as_float(subject_land_acres)
        candidate_land_acres_value = _as_float(candidate_land_acres)
        if subject_land_acres_value is not None and candidate_land_acres_value is not None:
            difference_value = round(
                subject_land_acres_value - candidate_land_acres_value,
                4,
            )
            difference_pct = _pct_diff(
                subject_land_acres_value, candidate_land_acres_value
            )
            return {
                "readiness_status": "ready",
                "basis_field": "land_acres",
                "subject_value": subject_land_acres_value,
                "candidate_value": candidate_land_acres_value,
                "difference_value": difference_value,
                "difference_pct": difference_pct,
                "potential_adjustment_flag": (
                    difference_pct is not None and difference_pct >= 0.10
                ),
                "adjustment_basis_status": "rate_not_evaluated",
            }

        return {
            "readiness_status": "review_required",
            "basis_field": None,
            "subject_value": None,
            "candidate_value": None,
            "difference_value": None,
            "difference_pct": None,
            "potential_adjustment_flag": False,
            "adjustment_basis_status": "rate_not_evaluated",
        }

    def _adjustment_support_sort_key(
        self,
        candidate: dict[str, Any],
    ) -> tuple[int, str]:
        return (
            _as_int(candidate.get("chosen_comp_position")) or 10_000_000,
            str(candidate.get("candidate_parcel_id") or ""),
        )

    def _adjustment_support_exclusion_reason_code(self, candidate: dict[str, Any]) -> str:
        chosen_comp_status = str(candidate.get("chosen_comp_status") or "")
        if chosen_comp_status == "not_chosen_comp":
            return "not_in_chosen_comp_set"
        if chosen_comp_status == "excluded_from_chosen_comp":
            return "chosen_comp_gate_excluded"
        return "chosen_comp_status_not_adjustment_eligible"


def _count_by_key(
    rows: Any,
    key_fn: Any,
) -> dict[str, int]:
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


def _pct_diff(subject_value: float | None, candidate_value: float | None) -> float | None:
    if subject_value in {None, 0.0} or candidate_value is None:
        return None
    return round(abs(subject_value - candidate_value) / abs(subject_value), 4)


def _fort_bend_supported_bath_basis(
    *,
    bath_field_name: str,
    valuation_bathroom_features_json: dict[str, Any],
) -> dict[str, Any] | None:
    if valuation_bathroom_features_json.get("attachment_status") != "attached":
        return None
    if (
        valuation_bathroom_features_json.get("bathroom_count_status")
        not in FORT_BEND_EXACT_BATHROOM_SUPPORT_STATUSES
    ):
        return None
    if (
        valuation_bathroom_features_json.get("bathroom_count_confidence")
        not in FORT_BEND_EXACT_BATHROOM_SUPPORT_CONFIDENCES
    ):
        return None
    basis_field = {
        "full_bath": "full_baths_derived",
        "half_bath": "half_baths_derived",
    }.get(bath_field_name)
    if basis_field is None:
        return None
    candidate_value = _as_float(valuation_bathroom_features_json.get(basis_field))
    if candidate_value is None:
        return None
    return {
        "basis_field": basis_field,
        "candidate_value": candidate_value,
        "bathroom_count_status": valuation_bathroom_features_json.get(
            "bathroom_count_status"
        ),
        "bathroom_count_confidence": valuation_bathroom_features_json.get(
            "bathroom_count_confidence"
        ),
    }
