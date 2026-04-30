from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection

CHOSEN_COMP_VERSION = "unequal_roll_chosen_comp_v5"
CHOSEN_COMP_CONFIG_VERSION = "unequal_roll_chosen_comp_v5"

FINAL_COMP_PREFERRED_TARGET_MIN = 12
FINAL_COMP_PREFERRED_TARGET_MAX = 18
FINAL_COMP_ACCEPTABLE_TARGET_MIN = 10
FINAL_COMP_ACCEPTABLE_TARGET_MAX = 20
FINAL_COMP_AUTO_SUPPORTED_MINIMUM = 7
FINAL_COMP_MANUAL_REVIEW_MIN = 6
FINAL_COMP_MANUAL_REVIEW_MAX = 6

LOCAL_SHARE_THRESHOLD = 0.70
MICRO_STREET_CONCENTRATION_WARNING_THRESHOLD = 0.50
MIN_UNIQUE_STREET_TARGET = 3
ACCEPTABLE_ZONE_MAX_ADDITIONAL_COMPS = (
    FINAL_COMP_ACCEPTABLE_TARGET_MAX - FINAL_COMP_PREFERRED_TARGET_MAX
)
ACCEPTABLE_ZONE_MAX_TAIL_SCORE_GAP = 0.025
ACCEPTABLE_ZONE_MIN_NORMALIZED_SCORE = 0.92
ACCEPTABLE_ZONE_MAX_CORE_MEAN_GAP = 0.02


@dataclass(frozen=True)
class UnequalRollCandidateChosenCompResult:
    unequal_roll_run_id: str
    total_candidates: int
    chosen_comp_eligible_count: int
    chosen_comp_count: int
    clean_chosen_comp_count: int
    review_chosen_comp_count: int
    excluded_from_chosen_comp_count: int
    final_comp_count_status: str
    selection_governance_status: str


class UnequalRollCandidateChosenCompService:
    def build_chosen_comp_set_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateChosenCompResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                run_context = self._fetch_run_context(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if run_context is None:
                    raise LookupError(
                        "Unequal-roll run context not found for chosen-comp build "
                        f"{unequal_roll_run_id}."
                    )

                candidates = self._fetch_candidates(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if not candidates:
                    raise LookupError(
                        "Unequal-roll final-selection-support candidates not found for run "
                        f"{unequal_roll_run_id}."
                    )

                chosen_comp_plan = self._build_chosen_comp_plan(candidates, run_context=run_context)
                governance = self._build_selection_governance(
                    candidates=candidates,
                    chosen_comp_plan=chosen_comp_plan,
                    run_context=run_context,
                )
                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    chosen_comp_assignment = chosen_comp_plan[candidate_id]
                    self._persist_candidate_chosen_comp(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        chosen_comp_assignment=chosen_comp_assignment,
                    )
                self._persist_run_selection_governance(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                    governance=governance,
                )
            connection.commit()

        chosen_comp_eligible_count = sum(
            1
            for chosen_comp_assignment in chosen_comp_plan.values()
            if chosen_comp_assignment["chosen_comp_eligible_flag"]
        )
        chosen_comp_count = governance["final_comp_count"]
        clean_chosen_comp_count = governance["clean_chosen_comp_count"]
        review_chosen_comp_count = governance["review_chosen_comp_count"]
        excluded_from_chosen_comp_count = sum(
            1
            for chosen_comp_assignment in chosen_comp_plan.values()
            if chosen_comp_assignment["chosen_comp_status"] == "excluded_from_chosen_comp"
        )
        return UnequalRollCandidateChosenCompResult(
            unequal_roll_run_id=unequal_roll_run_id,
            total_candidates=len(candidates),
            chosen_comp_eligible_count=chosen_comp_eligible_count,
            chosen_comp_count=chosen_comp_count,
            clean_chosen_comp_count=clean_chosen_comp_count,
            review_chosen_comp_count=review_chosen_comp_count,
            excluded_from_chosen_comp_count=excluded_from_chosen_comp_count,
            final_comp_count_status=governance["final_comp_count_status"],
            selection_governance_status=governance["selection_governance_status"],
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
              urr.summary_json,
              urss.parcel_id AS subject_parcel_id,
              urss.county_id,
              urss.tax_year,
              urss.neighborhood_code AS subject_neighborhood_code,
              urss.subdivision_name AS subject_subdivision_name
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
              address,
              neighborhood_code,
              subdivision_name,
              final_selection_support_position,
              final_selection_support_status,
              final_selection_support_version,
              final_selection_support_config_version,
              final_selection_support_detail_json,
              shortlist_position,
              shortlist_status,
              shortlist_version,
              shortlist_config_version,
              shortlist_detail_json,
              ranking_position,
              ranking_status,
              ranking_version,
              ranking_config_version,
              ranking_detail_json,
              discovery_tier,
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
              final_selection_support_position NULLS LAST,
              shortlist_position NULLS LAST,
              ranking_position NULLS LAST,
              candidate_parcel_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _build_chosen_comp_plan(
        self,
        candidates: list[dict[str, Any]],
        *,
        run_context: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        plan: dict[str, dict[str, Any]] = {}
        clean_support_candidates = sorted(
            [
                candidate
                for candidate in candidates
                if self._is_chosen_comp_eligible(candidate)
                and candidate.get("final_selection_support_status") == "selected_support"
            ],
            key=self._chosen_comp_sort_key,
        )
        review_support_candidates = sorted(
            [
                candidate
                for candidate in candidates
                if self._is_chosen_comp_eligible(candidate)
                and candidate.get("final_selection_support_status") == "review_selected_support"
            ],
            key=self._chosen_comp_sort_key,
        )
        clean_support_available_count = len(clean_support_candidates)
        review_support_available_count = len(review_support_candidates)
        chosen_clean_target = min(
            clean_support_available_count,
            FINAL_COMP_PREFERRED_TARGET_MAX,
        )
        review_fill_allowed = clean_support_available_count < FINAL_COMP_PREFERRED_TARGET_MIN
        review_fill_target = (
            min(
                review_support_available_count,
                max(0, FINAL_COMP_PREFERRED_TARGET_MAX - chosen_clean_target),
            )
            if review_fill_allowed
            else 0
        )

        chosen_clean_candidates = clean_support_candidates[:chosen_clean_target]
        chosen_review_candidates = review_support_candidates[:review_fill_target]
        chosen_comp_candidates = list(chosen_clean_candidates) + list(chosen_review_candidates)
        acceptable_zone_evaluations: dict[str, dict[str, Any]] = {}
        acceptable_zone_admitted_candidates = self._admit_acceptable_zone_candidates(
            remaining_clean_candidates=clean_support_candidates[chosen_clean_target:],
            current_chosen_candidates=chosen_comp_candidates,
            support_eligible_candidates=clean_support_candidates + review_support_candidates,
            run_context=run_context,
            acceptable_zone_evaluations=acceptable_zone_evaluations,
        )
        chosen_comp_candidates.extend(acceptable_zone_admitted_candidates)
        chosen_comp_candidate_ids = {
            str(candidate["unequal_roll_candidate_id"]) for candidate in chosen_comp_candidates
        }
        clean_support_insufficient_flag = review_fill_allowed
        review_carry_forward_required = len(chosen_review_candidates) > 0

        for chosen_comp_position, candidate in enumerate(chosen_comp_candidates, start=1):
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            chosen_comp_status = (
                "review_chosen_comp"
                if candidate.get("final_selection_support_status") == "review_selected_support"
                else "chosen_comp"
            )
            plan[candidate_id] = {
                "chosen_comp_position": chosen_comp_position,
                "chosen_comp_status": chosen_comp_status,
                "chosen_comp_eligible_flag": True,
                "chosen_comp_detail_json": self._build_chosen_comp_detail_json(
                    candidate=candidate,
                    chosen_comp_status=chosen_comp_status,
                    chosen_comp_position=chosen_comp_position,
                    chosen_comp_eligible_flag=True,
                    chosen_comp_flag=True,
                    exclusion_reason_code=None,
                    clean_support_available_count=clean_support_available_count,
                    review_support_available_count=review_support_available_count,
                    clean_support_insufficient_flag=clean_support_insufficient_flag,
                    review_carry_forward_required=review_carry_forward_required,
                    chosen_clean_target=chosen_clean_target,
                    chosen_review_target=review_fill_target,
                    acceptable_zone_detail=acceptable_zone_evaluations.get(candidate_id),
                ),
            }

        for candidate in clean_support_candidates + review_support_candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in chosen_comp_candidate_ids:
                continue
            exclusion_reason_code = "chosen_comp_target_not_met"
            if (
                candidate.get("final_selection_support_status") == "review_selected_support"
                and clean_support_available_count >= FINAL_COMP_PREFERRED_TARGET_MIN
            ):
                exclusion_reason_code = "clean_support_preferred_before_review_carry_forward"
            acceptable_zone_detail = acceptable_zone_evaluations.get(candidate_id)
            if acceptable_zone_detail and acceptable_zone_detail.get("candidate_flag"):
                exclusion_reason_code = (
                    acceptable_zone_detail.get("exclusion_reason_code")
                    or "acceptable_zone_admission_not_met"
                )
            plan[candidate_id] = {
                "chosen_comp_position": None,
                "chosen_comp_status": "not_chosen_comp",
                "chosen_comp_eligible_flag": True,
                "chosen_comp_detail_json": self._build_chosen_comp_detail_json(
                    candidate=candidate,
                    chosen_comp_status="not_chosen_comp",
                    chosen_comp_position=None,
                    chosen_comp_eligible_flag=True,
                    chosen_comp_flag=False,
                    exclusion_reason_code=exclusion_reason_code,
                    clean_support_available_count=clean_support_available_count,
                    review_support_available_count=review_support_available_count,
                    clean_support_insufficient_flag=clean_support_insufficient_flag,
                    review_carry_forward_required=review_carry_forward_required,
                    chosen_clean_target=chosen_clean_target,
                    chosen_review_target=review_fill_target,
                    acceptable_zone_detail=acceptable_zone_detail,
                ),
            }

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in plan:
                continue
            plan[candidate_id] = {
                "chosen_comp_position": None,
                "chosen_comp_status": "excluded_from_chosen_comp",
                "chosen_comp_eligible_flag": False,
                "chosen_comp_detail_json": self._build_chosen_comp_detail_json(
                    candidate=candidate,
                    chosen_comp_status="excluded_from_chosen_comp",
                    chosen_comp_position=None,
                    chosen_comp_eligible_flag=False,
                    chosen_comp_flag=False,
                    exclusion_reason_code=self._chosen_comp_exclusion_reason_code(candidate),
                    clean_support_available_count=clean_support_available_count,
                    review_support_available_count=review_support_available_count,
                    clean_support_insufficient_flag=clean_support_insufficient_flag,
                    review_carry_forward_required=review_carry_forward_required,
                    chosen_clean_target=chosen_clean_target,
                    chosen_review_target=review_fill_target,
                    acceptable_zone_detail=acceptable_zone_evaluations.get(candidate_id),
                ),
            }

        return plan

    def _admit_acceptable_zone_candidates(
        self,
        *,
        remaining_clean_candidates: list[dict[str, Any]],
        current_chosen_candidates: list[dict[str, Any]],
        support_eligible_candidates: list[dict[str, Any]],
        run_context: dict[str, Any],
        acceptable_zone_evaluations: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not remaining_clean_candidates or not current_chosen_candidates:
            return []

        subject_neighborhood_code = str(run_context.get("subject_neighborhood_code") or "").strip()
        subject_subdivision_name = str(run_context.get("subject_subdivision_name") or "").strip()
        accepted_candidates: list[dict[str, Any]] = []
        base_metrics = self._governance_metrics(
            chosen_candidates=current_chosen_candidates,
            support_eligible_candidates=support_eligible_candidates,
            subject_neighborhood_code=subject_neighborhood_code,
            subject_subdivision_name=subject_subdivision_name,
        )
        current_warning_codes = set(base_metrics["concentration_warning_codes"])
        tail_reference_score = _as_float(
            current_chosen_candidates[-1].get("normalized_similarity_score")
        )
        core_reference_scores = [
            _as_float(candidate.get("normalized_similarity_score"))
            for candidate in current_chosen_candidates[:FINAL_COMP_PREFERRED_TARGET_MIN]
            if _as_float(candidate.get("normalized_similarity_score")) is not None
        ]
        core_reference_mean_score = (
            round(sum(core_reference_scores) / len(core_reference_scores), 4)
            if core_reference_scores
            else None
        )

        for candidate in remaining_clean_candidates[:ACCEPTABLE_ZONE_MAX_ADDITIONAL_COMPS]:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            candidate_score = _as_float(candidate.get("normalized_similarity_score"))
            score_gap = (
                round((tail_reference_score or 0.0) - candidate_score, 4)
                if tail_reference_score is not None and candidate_score is not None
                else None
            )
            core_mean_gap = (
                round((core_reference_mean_score or 0.0) - candidate_score, 4)
                if core_reference_mean_score is not None and candidate_score is not None
                else None
            )
            primary_reason_code = (
                (candidate.get("similarity_score_detail_json") or {})
                .get("eligibility_context", {})
                .get("primary_reason_code")
            )
            secondary_reason_codes = list(
                (
                    (candidate.get("similarity_score_detail_json") or {})
                    .get("eligibility_context", {})
                    .get("secondary_reason_codes")
                )
                or []
            )
            projected_candidates = current_chosen_candidates + accepted_candidates + [candidate]
            projected_metrics = self._governance_metrics(
                chosen_candidates=projected_candidates,
                support_eligible_candidates=support_eligible_candidates,
                subject_neighborhood_code=subject_neighborhood_code,
                subject_subdivision_name=subject_subdivision_name,
            )
            projected_warning_codes = set(projected_metrics["concentration_warning_codes"])
            new_warning_codes = sorted(projected_warning_codes - current_warning_codes)

            exclusion_reason_code = None
            if primary_reason_code:
                exclusion_reason_code = "acceptable_zone_primary_reason_present"
            elif secondary_reason_codes:
                exclusion_reason_code = "acceptable_zone_secondary_reason_present"
            elif candidate_score is None:
                exclusion_reason_code = "acceptable_zone_missing_similarity_score"
            elif candidate_score < ACCEPTABLE_ZONE_MIN_NORMALIZED_SCORE:
                exclusion_reason_code = "acceptable_zone_tail_score_below_floor"
            elif score_gap is not None and score_gap > ACCEPTABLE_ZONE_MAX_TAIL_SCORE_GAP:
                exclusion_reason_code = "acceptable_zone_tail_score_gap_too_wide"
            elif (
                core_mean_gap is not None
                and core_mean_gap > ACCEPTABLE_ZONE_MAX_CORE_MEAN_GAP
            ):
                exclusion_reason_code = "acceptable_zone_core_mean_gap_too_wide"
            elif new_warning_codes:
                exclusion_reason_code = "acceptable_zone_new_concentration_warning"

            admitted_flag = exclusion_reason_code is None
            acceptable_zone_evaluations[candidate_id] = {
                "candidate_flag": True,
                "admitted_flag": admitted_flag,
                "exclusion_reason_code": exclusion_reason_code,
                "tail_reference_score": tail_reference_score,
                "candidate_score": candidate_score,
                "tail_score_gap": score_gap,
                "core_reference_mean_score": core_reference_mean_score,
                "core_mean_gap": core_mean_gap,
                "new_warning_codes": new_warning_codes,
                "secondary_reason_codes": secondary_reason_codes,
                "primary_reason_code": primary_reason_code,
                "rule_version": CHOSEN_COMP_CONFIG_VERSION,
            }
            if not admitted_flag:
                break
            accepted_candidates.append(candidate)
            current_warning_codes = projected_warning_codes

        return accepted_candidates

    def _build_selection_governance(
        self,
        *,
        candidates: list[dict[str, Any]],
        chosen_comp_plan: dict[str, dict[str, Any]],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        chosen_candidates = [
            candidate
            for candidate in candidates
            if chosen_comp_plan[str(candidate["unequal_roll_candidate_id"])]["chosen_comp_status"]
            in {"chosen_comp", "review_chosen_comp"}
        ]
        chosen_candidates.sort(
            key=lambda candidate: (
                _as_int(
                    chosen_comp_plan[str(candidate["unequal_roll_candidate_id"])][
                        "chosen_comp_position"
                    ]
                )
                or 10_000_000,
                str(candidate.get("candidate_parcel_id") or ""),
            )
        )
        clean_chosen_candidates = [
            candidate
            for candidate in chosen_candidates
            if chosen_comp_plan[str(candidate["unequal_roll_candidate_id"])]["chosen_comp_status"]
            == "chosen_comp"
        ]
        review_chosen_candidates = [
            candidate
            for candidate in chosen_candidates
            if chosen_comp_plan[str(candidate["unequal_roll_candidate_id"])]["chosen_comp_status"]
            == "review_chosen_comp"
        ]
        chosen_count = len(chosen_candidates)
        clean_chosen_count = len(clean_chosen_candidates)
        review_chosen_count = len(review_chosen_candidates)
        final_comp_count_status = _final_comp_count_status(chosen_count)

        subject_neighborhood_code = str(run_context.get("subject_neighborhood_code") or "").strip()
        subject_subdivision_name = str(run_context.get("subject_subdivision_name") or "").strip()
        support_eligible_candidates = [
            candidate
            for candidate in candidates
            if candidate.get("final_selection_support_status")
            in {"selected_support", "review_selected_support"}
        ]
        governance_metrics = self._governance_metrics(
            chosen_candidates=chosen_candidates,
            support_eligible_candidates=support_eligible_candidates,
            subject_neighborhood_code=subject_neighborhood_code,
            subject_subdivision_name=subject_subdivision_name,
        )
        same_local_selected_count = governance_metrics["same_local_selected_count"]
        same_local_selected_share = governance_metrics["same_local_selected_share"]
        same_local_support_pool_count = governance_metrics["same_local_support_pool_count"]
        local_share_threshold_count = governance_metrics["local_share_threshold_count"]
        local_share_supply_exists = governance_metrics["local_share_supply_exists"]
        local_share_threshold_met = governance_metrics["local_share_threshold_met"]
        same_neighborhood_selected_count = governance_metrics["same_neighborhood_selected_count"]
        same_subdivision_selected_count = governance_metrics["same_subdivision_selected_count"]
        unique_selected_street_count = governance_metrics["unique_selected_street_count"]
        unique_support_street_count = governance_metrics["unique_support_street_count"]
        max_street_share = governance_metrics["max_street_share"]
        concentration_warning_codes = governance_metrics["concentration_warning_codes"]

        candidate_discovery_summary = dict(
            (run_context.get("summary_json") or {}).get("candidate_discovery_summary") or {}
        )
        fallback_used = bool(candidate_discovery_summary.get("fallback_used"))
        fallback_justification = candidate_discovery_summary.get(
            "same_neighborhood_insufficient_reason"
        )
        if fallback_used and not fallback_justification:
            fallback_justification = "same_neighborhood_supply_below_preferred_pool"

        manual_review_exception_prerequisites = {
            "count_in_exception_range": final_comp_count_status
            == "manual_review_exception_range",
            "same_locality_requirement_met": same_local_selected_count == chosen_count
            if chosen_count
            else False,
            "low_adjustment_burden_status": "not_evaluated",
            "low_dispersion_status": "not_evaluated",
            "no_material_feature_mismatches_status": "not_evaluated",
            "analyst_approval_required": True,
        }

        selection_governance_status = _selection_governance_status(
            final_comp_count_status=final_comp_count_status,
            review_chosen_count=review_chosen_count,
            concentration_warning_codes=concentration_warning_codes,
        )

        selected_comp_rows = [
            self._selected_comp_row(
                candidate=candidate,
                chosen_comp_assignment=chosen_comp_plan[
                    str(candidate["unequal_roll_candidate_id"])
                ],
            )
            for candidate in chosen_candidates
        ]
        near_miss_rows = [
            self._near_miss_row(
                candidate=candidate,
                chosen_comp_assignment=chosen_comp_plan[
                    str(candidate["unequal_roll_candidate_id"])
                ],
            )
            for candidate in candidates
            if chosen_comp_plan[str(candidate["unequal_roll_candidate_id"])]["chosen_comp_status"]
            not in {"chosen_comp", "review_chosen_comp"}
        ]

        failed_reason_counts = {
            "eligibility_primary_reason_counts": _count_by_key(
                candidates, lambda candidate: candidate.get("eligibility_reason_code")
            ),
            "shortlist_exclusion_reason_counts": _count_by_key(
                candidates,
                lambda candidate: (
                    (candidate.get("shortlist_detail_json") or {})
                    .get("shortlist_context", {})
                    .get("shortlist_exclusion_reason_code")
                ),
            ),
            "final_selection_support_exclusion_reason_counts": _count_by_key(
                candidates,
                lambda candidate: (
                    (candidate.get("final_selection_support_detail_json") or {})
                    .get("final_selection_support_context", {})
                    .get("exclusion_reason_code")
                ),
            ),
            "chosen_comp_exclusion_reason_counts": _count_by_key(
                candidates,
                lambda candidate: chosen_comp_plan[
                    str(candidate["unequal_roll_candidate_id"])
                ]["chosen_comp_detail_json"]["chosen_comp_context"]["exclusion_reason_code"],
            ),
        }

        selected_clean_count_before_acceptable_zone = min(
            sum(
                1
                for candidate in support_eligible_candidates
                if candidate.get("final_selection_support_status") == "selected_support"
            ),
            FINAL_COMP_PREFERRED_TARGET_MAX,
        )
        acceptable_zone_selected_rows = [
            row
            for row in selected_comp_rows
            if row["chosen_comp_status"] == "chosen_comp"
            and (row["chosen_comp_position"] or 0) > FINAL_COMP_PREFERRED_TARGET_MAX
        ]
        acceptable_zone_stop_reason_codes = sorted(
            {
                row["exclusion_reason_code"]
                for row in near_miss_rows
                if row["exclusion_reason_code"]
                and str(row["exclusion_reason_code"]).startswith("acceptable_zone_")
            }
        )
        selection_log_json = {
            "selection_log_version": CHOSEN_COMP_VERSION,
            "selection_log_config_version": CHOSEN_COMP_CONFIG_VERSION,
            "count_policy": {
                "preferred_final_comp_range": {
                    "min": FINAL_COMP_PREFERRED_TARGET_MIN,
                    "max": FINAL_COMP_PREFERRED_TARGET_MAX,
                },
                "acceptable_final_comp_range": {
                    "min": FINAL_COMP_ACCEPTABLE_TARGET_MIN,
                    "max": FINAL_COMP_ACCEPTABLE_TARGET_MAX,
                },
                "auto_supported_minimum": FINAL_COMP_AUTO_SUPPORTED_MINIMUM,
                "manual_review_exception_range": {
                    "min": FINAL_COMP_MANUAL_REVIEW_MIN,
                    "max": FINAL_COMP_MANUAL_REVIEW_MAX,
                },
                "unsupported_below": FINAL_COMP_MANUAL_REVIEW_MIN,
                "maximum_final_comp_count": FINAL_COMP_ACCEPTABLE_TARGET_MAX,
                "review_carry_forward_trigger_below_clean_count": (
                    FINAL_COMP_PREFERRED_TARGET_MIN
                ),
                "acceptable_zone_admission_rule": {
                    "preferred_operating_band": {
                        "min": FINAL_COMP_PREFERRED_TARGET_MIN,
                        "max": FINAL_COMP_PREFERRED_TARGET_MAX,
                    },
                    "acceptable_zone": {
                        "min": FINAL_COMP_PREFERRED_TARGET_MAX + 1,
                        "max": FINAL_COMP_ACCEPTABLE_TARGET_MAX,
                    },
                    "clean_support_only": True,
                    "review_carry_forward_allowed_in_acceptable_zone": False,
                    "tail_score_gap_max": ACCEPTABLE_ZONE_MAX_TAIL_SCORE_GAP,
                    "tail_score_floor": ACCEPTABLE_ZONE_MIN_NORMALIZED_SCORE,
                    "core_mean_gap_max": ACCEPTABLE_ZONE_MAX_CORE_MEAN_GAP,
                    "new_concentration_warnings_allowed": False,
                    "secondary_reason_codes_allowed": False,
                    "primary_reason_code_allowed": False,
                },
            },
            "candidate_pool": {
                "discovered_count": candidate_discovery_summary.get("discovered_count"),
                "same_neighborhood_count": candidate_discovery_summary.get(
                    "same_neighborhood_count"
                ),
                "county_sfr_fallback_count": candidate_discovery_summary.get(
                    "county_sfr_fallback_count"
                ),
                "eligible_count": candidate_discovery_summary.get("eligible_count"),
                "review_count": candidate_discovery_summary.get("review_count"),
                "excluded_count": candidate_discovery_summary.get("excluded_count"),
                "shortlisted_count": sum(
                    1
                    for candidate in candidates
                    if candidate.get("shortlist_status")
                    in {"shortlisted", "review_shortlisted"}
                ),
                "support_eligible_count": len(support_eligible_candidates),
                "clean_support_count": sum(
                    1
                    for candidate in support_eligible_candidates
                    if candidate.get("final_selection_support_status") == "selected_support"
                ),
                "review_support_count": sum(
                    1
                    for candidate in support_eligible_candidates
                    if candidate.get("final_selection_support_status")
                    == "review_selected_support"
                ),
                "chosen_comp_count": chosen_count,
                "clean_chosen_comp_count": clean_chosen_count,
                "review_chosen_comp_count": review_chosen_count,
            },
            "filters_applied": [
                "eligibility_plausibility_filters",
                "ranking_similarity_gate",
                "shortlist_score_first_with_rankable_preference_inside_close_score_band",
                "final_selection_support_shortlist_position_cutoff",
                "chosen_comp_clean_support_first_policy",
                "chosen_comp_review_carry_forward_only_when_clean_support_insufficient",
                "final_comp_count_governance",
                "acceptable_zone_tail_quality_admission_rule",
                "diversity_and_concentration_governance",
            ],
            "failed_reason_counts": failed_reason_counts,
            "governance": {
                "final_comp_count": chosen_count,
                "final_comp_count_status": final_comp_count_status,
                "selection_governance_status": selection_governance_status,
                "same_neighborhood_selected_count": same_neighborhood_selected_count,
                "same_subdivision_selected_count": same_subdivision_selected_count,
                "same_local_selected_count": same_local_selected_count,
                "same_local_selected_share": same_local_selected_share,
                "same_local_share_threshold": LOCAL_SHARE_THRESHOLD,
                "same_local_share_threshold_count": local_share_threshold_count,
                "same_local_supply_exists": local_share_supply_exists,
                "same_local_share_threshold_met": local_share_threshold_met,
                "fallback_used": fallback_used,
                "fallback_justification": fallback_justification,
                "max_street_share": max_street_share,
                "unique_selected_street_count": unique_selected_street_count,
                "unique_support_street_count": unique_support_street_count,
                "concentration_warning_codes": concentration_warning_codes,
                "manual_review_exception_prerequisites": manual_review_exception_prerequisites,
                "chosen_set_fully_clean_flag": review_chosen_count == 0,
                "acceptable_zone_admission": {
                    "evaluated": clean_chosen_count > FINAL_COMP_PREFERRED_TARGET_MAX
                    or same_local_support_pool_count > FINAL_COMP_PREFERRED_TARGET_MAX,
                    "selected_clean_count_before_acceptable_zone": (
                        selected_clean_count_before_acceptable_zone
                    ),
                    "admitted_count": len(acceptable_zone_selected_rows),
                    "admitted_positions": [
                        row["chosen_comp_position"] for row in acceptable_zone_selected_rows
                    ],
                    "stop_reason_codes": acceptable_zone_stop_reason_codes,
                    "stopped_in_preferred_band": len(acceptable_zone_selected_rows) == 0,
                },
            },
            "selected_comps": selected_comp_rows,
            "near_miss_comps": near_miss_rows,
        }

        return {
            "final_comp_count": chosen_count,
            "clean_chosen_comp_count": clean_chosen_count,
            "review_chosen_comp_count": review_chosen_count,
            "final_comp_count_status": final_comp_count_status,
            "selection_governance_status": selection_governance_status,
            "selection_log_json": selection_log_json,
        }

    def _persist_candidate_chosen_comp(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        chosen_comp_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET chosen_comp_position = %s,
                chosen_comp_status = %s,
                chosen_comp_version = %s,
                chosen_comp_config_version = %s,
                chosen_comp_detail_json = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                chosen_comp_assignment["chosen_comp_position"],
                chosen_comp_assignment["chosen_comp_status"],
                CHOSEN_COMP_VERSION,
                CHOSEN_COMP_CONFIG_VERSION,
                Jsonb(chosen_comp_assignment["chosen_comp_detail_json"]),
                unequal_roll_candidate_id,
            ),
        )

    def _governance_metrics(
        self,
        *,
        chosen_candidates: list[dict[str, Any]],
        support_eligible_candidates: list[dict[str, Any]],
        subject_neighborhood_code: str,
        subject_subdivision_name: str,
    ) -> dict[str, Any]:
        chosen_count = len(chosen_candidates)
        same_local_selected_count = sum(
            1
            for candidate in chosen_candidates
            if _same_local_cohort(
                candidate,
                subject_neighborhood_code=subject_neighborhood_code,
                subject_subdivision_name=subject_subdivision_name,
            )
        )
        same_local_selected_share = (
            round(same_local_selected_count / chosen_count, 4) if chosen_count else 0.0
        )
        same_local_support_pool_count = sum(
            1
            for candidate in support_eligible_candidates
            if _same_local_cohort(
                candidate,
                subject_neighborhood_code=subject_neighborhood_code,
                subject_subdivision_name=subject_subdivision_name,
            )
        )
        local_share_threshold_count = (
            math.ceil(chosen_count * LOCAL_SHARE_THRESHOLD) if chosen_count else 0
        )
        local_share_supply_exists = same_local_support_pool_count >= local_share_threshold_count
        local_share_threshold_met = (
            same_local_selected_share >= LOCAL_SHARE_THRESHOLD
            if local_share_threshold_count > 0
            else False
        )
        same_neighborhood_selected_count = sum(
            1
            for candidate in chosen_candidates
            if str(candidate.get("neighborhood_code") or "").strip() == subject_neighborhood_code
        )
        same_subdivision_selected_count = sum(
            1
            for candidate in chosen_candidates
            if subject_subdivision_name
            and str(candidate.get("subdivision_name") or "").strip() == subject_subdivision_name
        )
        street_counter = Counter(
            _street_key(candidate.get("address")) for candidate in chosen_candidates
        )
        street_counter.pop("", None)
        support_street_counter = Counter(
            _street_key(candidate.get("address")) for candidate in support_eligible_candidates
        )
        support_street_counter.pop("", None)
        unique_selected_street_count = len(street_counter)
        unique_support_street_count = len(support_street_counter)
        max_street_share = (
            round(max(street_counter.values()) / chosen_count, 4)
            if chosen_count and street_counter
            else 0.0
        )
        concentration_warning_codes: list[str] = []
        if (
            chosen_count >= FINAL_COMP_MANUAL_REVIEW_MIN
            and unique_support_street_count >= MIN_UNIQUE_STREET_TARGET
            and max_street_share > MICRO_STREET_CONCENTRATION_WARNING_THRESHOLD
        ):
            concentration_warning_codes.append("micro_street_concentration_warning")
        if (
            same_local_selected_count >= MIN_UNIQUE_STREET_TARGET
            and unique_selected_street_count
            < min(MIN_UNIQUE_STREET_TARGET, same_local_selected_count)
            and unique_support_street_count >= MIN_UNIQUE_STREET_TARGET
        ):
            concentration_warning_codes.append("local_cohort_spread_warning")
        if local_share_supply_exists and not local_share_threshold_met:
            concentration_warning_codes.append("same_local_share_below_threshold")
        return {
            "same_local_selected_count": same_local_selected_count,
            "same_local_selected_share": same_local_selected_share,
            "same_local_support_pool_count": same_local_support_pool_count,
            "local_share_threshold_count": local_share_threshold_count,
            "local_share_supply_exists": local_share_supply_exists,
            "local_share_threshold_met": local_share_threshold_met,
            "same_neighborhood_selected_count": same_neighborhood_selected_count,
            "same_subdivision_selected_count": same_subdivision_selected_count,
            "unique_selected_street_count": unique_selected_street_count,
            "unique_support_street_count": unique_support_street_count,
            "max_street_share": max_street_share,
            "concentration_warning_codes": concentration_warning_codes,
        }

    def _persist_run_selection_governance(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        governance: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_runs
            SET final_comp_count = %s,
                final_comp_count_status = %s,
                selection_governance_status = %s,
                selection_log_json = %s,
                updated_at = now()
            WHERE unequal_roll_run_id = %s
            """,
            (
                governance["final_comp_count"],
                governance["final_comp_count_status"],
                governance["selection_governance_status"],
                Jsonb(governance["selection_log_json"]),
                unequal_roll_run_id,
            ),
        )

    def _is_chosen_comp_eligible(self, candidate: dict[str, Any]) -> bool:
        if candidate.get("ranking_status") == "excluded_from_ranking":
            return False
        if candidate.get("shortlist_status") == "excluded_from_shortlist":
            return False
        return candidate.get("final_selection_support_status") in {
            "selected_support",
            "review_selected_support",
        }

    def _chosen_comp_exclusion_reason_code(self, candidate: dict[str, Any]) -> str:
        ranking_status = str(candidate.get("ranking_status") or "")
        shortlist_status = str(candidate.get("shortlist_status") or "")
        support_status = str(candidate.get("final_selection_support_status") or "")
        if ranking_status == "excluded_from_ranking":
            return "ranking_gate_excluded"
        if shortlist_status == "excluded_from_shortlist":
            return "shortlist_gate_excluded"
        if support_status == "excluded_from_selection_support":
            return "final_selection_support_gate_excluded"
        if support_status == "not_selected_support":
            return "final_selection_support_cutoff_not_met"
        if support_status == "not_evaluated":
            return "final_selection_support_not_evaluated"
        return "unsupported_chosen_comp_state"

    def _build_chosen_comp_detail_json(
        self,
        *,
        candidate: dict[str, Any],
        chosen_comp_status: str,
        chosen_comp_position: int | None,
        chosen_comp_eligible_flag: bool,
        chosen_comp_flag: bool,
        exclusion_reason_code: str | None,
        clean_support_available_count: int,
        review_support_available_count: int,
        clean_support_insufficient_flag: bool,
        review_carry_forward_required: bool,
        chosen_clean_target: int,
        chosen_review_target: int,
        acceptable_zone_detail: dict[str, Any] | None,
    ) -> dict[str, Any]:
        support_detail_json = dict(candidate.get("final_selection_support_detail_json") or {})
        shortlist_detail_json = dict(candidate.get("shortlist_detail_json") or {})
        ranking_detail_json = dict(candidate.get("ranking_detail_json") or {})
        similarity_score_detail_json = dict(candidate.get("similarity_score_detail_json") or {})
        support_position = _as_int(candidate.get("final_selection_support_position"))
        support_status = candidate.get("final_selection_support_status")
        return {
            "chosen_comp_version": CHOSEN_COMP_VERSION,
            "chosen_comp_config_version": CHOSEN_COMP_CONFIG_VERSION,
            "chosen_comp_status": chosen_comp_status,
            "chosen_comp_position": chosen_comp_position,
            "chosen_comp_policy": {
                "preferred_final_comp_range": {
                    "min": FINAL_COMP_PREFERRED_TARGET_MIN,
                    "max": FINAL_COMP_PREFERRED_TARGET_MAX,
                },
                "acceptable_final_comp_range": {
                    "min": FINAL_COMP_ACCEPTABLE_TARGET_MIN,
                    "max": FINAL_COMP_ACCEPTABLE_TARGET_MAX,
                },
                "auto_supported_minimum": FINAL_COMP_AUTO_SUPPORTED_MINIMUM,
                "manual_review_exception_range": {
                    "min": FINAL_COMP_MANUAL_REVIEW_MIN,
                    "max": FINAL_COMP_MANUAL_REVIEW_MAX,
                },
                "chosen_clean_target": chosen_clean_target,
                "chosen_review_target": chosen_review_target,
                "maximum_final_comp_count": FINAL_COMP_ACCEPTABLE_TARGET_MAX,
                "review_carry_forward_trigger_below_clean_count": (
                    FINAL_COMP_PREFERRED_TARGET_MIN
                ),
                "review_carry_forward_allowed_only_when_clean_support_insufficient": True,
                "acceptable_zone_admission_rule": {
                    "clean_support_only": True,
                    "review_carry_forward_allowed_in_acceptable_zone": False,
                    "tail_score_gap_max": ACCEPTABLE_ZONE_MAX_TAIL_SCORE_GAP,
                    "tail_score_floor": ACCEPTABLE_ZONE_MIN_NORMALIZED_SCORE,
                    "core_mean_gap_max": ACCEPTABLE_ZONE_MAX_CORE_MEAN_GAP,
                    "new_concentration_warnings_allowed": False,
                    "secondary_reason_codes_allowed": False,
                    "primary_reason_code_allowed": False,
                },
                "order_strategy": "clean_support_first_then_review_carry_forward_by_support_position",
            },
            "chosen_comp_context": {
                "chosen_comp_eligible_flag": chosen_comp_eligible_flag,
                "chosen_comp_flag": chosen_comp_flag,
                "exclusion_reason_code": exclusion_reason_code,
                "input_final_selection_support_status": support_status,
                "included_as_chosen_comp_status": chosen_comp_status,
                "clean_support_available_count": clean_support_available_count,
                "review_support_available_count": review_support_available_count,
                "clean_support_insufficient_flag": clean_support_insufficient_flag,
                "review_carry_forward_required": review_carry_forward_required,
                "review_carry_forward_flag": chosen_comp_status == "review_chosen_comp",
                "clean_support_preference_reordered_from_support": (
                    chosen_comp_position is not None
                    and support_position is not None
                    and chosen_comp_position != support_position
                ),
                "acceptable_zone_candidate_flag": bool(
                    acceptable_zone_detail and acceptable_zone_detail.get("candidate_flag")
                ),
                "acceptable_zone_admitted_flag": bool(
                    acceptable_zone_detail and acceptable_zone_detail.get("admitted_flag")
                ),
                "acceptable_zone_exclusion_reason_code": (
                    acceptable_zone_detail.get("exclusion_reason_code")
                    if acceptable_zone_detail
                    else None
                ),
            },
            "final_selection_support_context": {
                "final_selection_support_position": support_position,
                "final_selection_support_status": support_status,
                "final_selection_support_version": candidate.get(
                    "final_selection_support_version"
                ),
                "final_selection_support_config_version": candidate.get(
                    "final_selection_support_config_version"
                ),
                "final_selection_support_order_strategy": (
                    (support_detail_json.get("final_selection_support_policy") or {}).get(
                        "order_strategy"
                    )
                ),
                "final_selection_support_exclusion_reason_code": (
                    (support_detail_json.get("final_selection_support_context") or {}).get(
                        "exclusion_reason_code"
                    )
                ),
            },
            "shortlist_context": {
                "shortlist_position": _as_int(candidate.get("shortlist_position")),
                "shortlist_status": candidate.get("shortlist_status"),
                "shortlist_version": candidate.get("shortlist_version"),
                "shortlist_config_version": candidate.get("shortlist_config_version"),
                "close_score_threshold": (
                    (shortlist_detail_json.get("shortlist_policy") or {}).get(
                        "close_score_threshold"
                    )
                ),
                "close_score_policy_reordered_from_ranking": (
                    (shortlist_detail_json.get("shortlist_context") or {}).get(
                        "close_score_policy_reordered_from_ranking"
                    )
                ),
            },
            "ranking_context": {
                "ranking_position": _as_int(candidate.get("ranking_position")),
                "ranking_status": candidate.get("ranking_status"),
                "ranking_version": candidate.get("ranking_version"),
                "ranking_config_version": candidate.get("ranking_config_version"),
                "ranking_order_strategy": (
                    (ranking_detail_json.get("ranking_basis") or {}).get("order_strategy")
                ),
            },
            "score_context": {
                "normalized_similarity_score": _as_float(
                    candidate.get("normalized_similarity_score")
                ),
                "raw_similarity_score": _as_float(candidate.get("raw_similarity_score")),
                "scoring_version": candidate.get("scoring_version"),
                "scoring_config_version": candidate.get("scoring_config_version"),
                "base_similarity_score": similarity_score_detail_json.get(
                    "base_similarity_score"
                ),
                "eligibility_status_multiplier": similarity_score_detail_json.get(
                    "eligibility_status_multiplier"
                ),
                "fort_bend_bathroom_modifier": similarity_score_detail_json.get(
                    "fort_bend_bathroom_modifier"
                ),
                "primary_reason_code": (
                    similarity_score_detail_json.get("eligibility_context") or {}
                ).get("primary_reason_code"),
                "secondary_reason_codes": list(
                    ((similarity_score_detail_json.get("eligibility_context") or {}).get(
                        "secondary_reason_codes"
                    ))
                    or []
                ),
                "acceptable_zone_evaluation": acceptable_zone_detail,
            },
        }

    def _selected_comp_row(
        self,
        *,
        candidate: dict[str, Any],
        chosen_comp_assignment: dict[str, Any],
    ) -> dict[str, Any]:
        chosen_status = chosen_comp_assignment["chosen_comp_status"]
        same_local_flag = _same_local_cohort(
            candidate,
            subject_neighborhood_code="",
            subject_subdivision_name="",
        )
        del same_local_flag
        return {
            "unequal_roll_candidate_id": str(candidate["unequal_roll_candidate_id"]),
            "candidate_parcel_id": str(candidate["candidate_parcel_id"]),
            "chosen_comp_position": chosen_comp_assignment["chosen_comp_position"],
            "chosen_comp_status": chosen_status,
            "selection_reason_code": (
                "acceptable_zone_tail_admission"
                if chosen_status == "chosen_comp"
                and (
                    (
                        chosen_comp_assignment["chosen_comp_detail_json"]["chosen_comp_context"][
                            "acceptable_zone_admitted_flag"
                        ]
                    )
                )
                else "selected_support_survived"
                if chosen_status == "chosen_comp"
                else "review_carry_forward_due_to_clean_support_insufficient"
            ),
            "final_selection_support_position": _as_int(
                candidate.get("final_selection_support_position")
            ),
            "final_selection_support_status": candidate.get("final_selection_support_status"),
            "shortlist_position": _as_int(candidate.get("shortlist_position")),
            "shortlist_status": candidate.get("shortlist_status"),
            "ranking_position": _as_int(candidate.get("ranking_position")),
            "ranking_status": candidate.get("ranking_status"),
            "discovery_tier": candidate.get("discovery_tier"),
            "score_rank": _as_int(candidate.get("ranking_position")),
            "normalized_similarity_score": _as_float(
                candidate.get("normalized_similarity_score")
            ),
            "acceptable_zone_admitted_flag": chosen_comp_assignment["chosen_comp_detail_json"][
                "chosen_comp_context"
            ]["acceptable_zone_admitted_flag"],
            "primary_reason_code": (
                (candidate.get("similarity_score_detail_json") or {})
                .get("eligibility_context", {})
                .get("primary_reason_code")
            ),
        }

    def _near_miss_row(
        self,
        *,
        candidate: dict[str, Any],
        chosen_comp_assignment: dict[str, Any],
    ) -> dict[str, Any]:
        detail_json = chosen_comp_assignment["chosen_comp_detail_json"]
        return {
            "unequal_roll_candidate_id": str(candidate["unequal_roll_candidate_id"]),
            "candidate_parcel_id": str(candidate["candidate_parcel_id"]),
            "chosen_comp_status": chosen_comp_assignment["chosen_comp_status"],
            "chosen_comp_position": chosen_comp_assignment["chosen_comp_position"],
            "exclusion_reason_code": detail_json["chosen_comp_context"]["exclusion_reason_code"],
            "final_selection_support_status": candidate.get("final_selection_support_status"),
            "shortlist_status": candidate.get("shortlist_status"),
            "ranking_status": candidate.get("ranking_status"),
            "score_rank": _as_int(candidate.get("ranking_position")),
            "normalized_similarity_score": _as_float(
                candidate.get("normalized_similarity_score")
            ),
        }

    def _chosen_comp_sort_key(self, candidate: dict[str, Any]) -> tuple[int, int, int, str]:
        return (
            _as_int(candidate.get("final_selection_support_position")) or 10_000_000,
            _as_int(candidate.get("shortlist_position")) or 10_000_000,
            _as_int(candidate.get("ranking_position")) or 10_000_000,
            str(candidate.get("candidate_parcel_id") or ""),
        )


def _final_comp_count_status(final_comp_count: int) -> str:
    if FINAL_COMP_PREFERRED_TARGET_MIN <= final_comp_count <= FINAL_COMP_PREFERRED_TARGET_MAX:
        return "preferred_range"
    if FINAL_COMP_ACCEPTABLE_TARGET_MIN <= final_comp_count <= FINAL_COMP_ACCEPTABLE_TARGET_MAX:
        return "acceptable_range"
    if final_comp_count >= FINAL_COMP_AUTO_SUPPORTED_MINIMUM:
        return "auto_supported_minimum"
    if FINAL_COMP_MANUAL_REVIEW_MIN <= final_comp_count <= FINAL_COMP_MANUAL_REVIEW_MAX:
        return "manual_review_exception_range"
    return "unsupported_below_minimum"


def _selection_governance_status(
    *,
    final_comp_count_status: str,
    review_chosen_count: int,
    concentration_warning_codes: list[str],
) -> str:
    if final_comp_count_status == "unsupported_below_minimum":
        return "unsupported"
    if final_comp_count_status == "manual_review_exception_range":
        return "manual_review_required"
    if (
        final_comp_count_status in {"acceptable_range", "auto_supported_minimum"}
        or review_chosen_count > 0
        or concentration_warning_codes
    ):
        return "supported_with_warnings"
    return "auto_supported"

def _same_local_cohort(
    candidate: dict[str, Any],
    *,
    subject_neighborhood_code: str,
    subject_subdivision_name: str,
) -> bool:
    candidate_neighborhood_code = str(candidate.get("neighborhood_code") or "").strip()
    candidate_subdivision_name = str(candidate.get("subdivision_name") or "").strip()
    return bool(
        (subject_neighborhood_code and candidate_neighborhood_code == subject_neighborhood_code)
        or (
            subject_subdivision_name
            and candidate_subdivision_name == subject_subdivision_name
        )
    )


def _street_key(address: Any) -> str:
    text = str(address or "").upper().strip()
    if not text:
        return ""
    text = text.split(",")[0]
    text = re.sub(r"^\d+\s+", "", text)
    text = re.sub(r"\s+(APT|UNIT|STE|SUITE|#).*$", "", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _count_by_key(
    candidates: list[dict[str, Any]],
    key_fn: Any,
) -> dict[str, int]:
    counts = Counter()
    for candidate in candidates:
        key = key_fn(candidate)
        if key is None or key == "":
            continue
        counts[str(key)] += 1
    return dict(sorted(counts.items()))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
