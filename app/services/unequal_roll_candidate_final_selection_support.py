from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection

FINAL_SELECTION_SUPPORT_VERSION = "unequal_roll_final_selection_support_v2"
FINAL_SELECTION_SUPPORT_CONFIG_VERSION = "unequal_roll_final_selection_support_v2"
FINAL_SELECTION_SUPPORT_TARGET_SIZE = 20


@dataclass(frozen=True)
class UnequalRollCandidateFinalSelectionSupportResult:
    unequal_roll_run_id: str
    total_candidates: int
    support_eligible_count: int
    selected_support_count: int
    review_selected_support_count: int
    excluded_from_selection_support_count: int


class UnequalRollCandidateFinalSelectionSupportService:
    def build_final_selection_support_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateFinalSelectionSupportResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                candidates = self._fetch_candidates(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if not candidates:
                    raise LookupError(
                        "Unequal-roll shortlist candidates not found for run "
                        f"{unequal_roll_run_id}."
                    )

                support_plan = self._build_support_plan(candidates)
                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    support_assignment = support_plan[candidate_id]
                    self._persist_candidate_support(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        support_assignment=support_assignment,
                    )
            connection.commit()

        support_eligible_count = sum(
            1
            for support_assignment in support_plan.values()
            if support_assignment["support_eligible_flag"]
        )
        selected_support_count = sum(
            1
            for support_assignment in support_plan.values()
            if support_assignment["final_selection_support_status"]
            in {"selected_support", "review_selected_support"}
        )
        review_selected_support_count = sum(
            1
            for support_assignment in support_plan.values()
            if support_assignment["final_selection_support_status"] == "review_selected_support"
        )
        excluded_from_selection_support_count = sum(
            1
            for support_assignment in support_plan.values()
            if support_assignment["final_selection_support_status"]
            == "excluded_from_selection_support"
        )
        return UnequalRollCandidateFinalSelectionSupportResult(
            unequal_roll_run_id=unequal_roll_run_id,
            total_candidates=len(candidates),
            support_eligible_count=support_eligible_count,
            selected_support_count=selected_support_count,
            review_selected_support_count=review_selected_support_count,
            excluded_from_selection_support_count=excluded_from_selection_support_count,
        )

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
            ORDER BY shortlist_position NULLS LAST, ranking_position NULLS LAST, candidate_parcel_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _build_support_plan(
        self,
        candidates: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        plan: dict[str, dict[str, Any]] = {}
        support_eligible = [
            candidate for candidate in candidates if self._is_support_eligible(candidate)
        ]
        support_eligible.sort(key=self._support_sort_key)
        selected_support_candidates = support_eligible[:FINAL_SELECTION_SUPPORT_TARGET_SIZE]
        selected_support_candidate_ids = {
            str(candidate["unequal_roll_candidate_id"]) for candidate in selected_support_candidates
        }

        for support_position, candidate in enumerate(selected_support_candidates, start=1):
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            support_status = (
                "review_selected_support"
                if candidate.get("shortlist_status") == "review_shortlisted"
                else "selected_support"
            )
            plan[candidate_id] = {
                "final_selection_support_position": support_position,
                "final_selection_support_status": support_status,
                "support_eligible_flag": True,
                "final_selection_support_detail_json": self._build_support_detail_json(
                    candidate=candidate,
                    support_status=support_status,
                    support_position=support_position,
                    support_eligible_flag=True,
                    selected_support_flag=True,
                    exclusion_reason_code=None,
                ),
            }

        for candidate in support_eligible:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in selected_support_candidate_ids:
                continue
            plan[candidate_id] = {
                "final_selection_support_position": None,
                "final_selection_support_status": "not_selected_support",
                "support_eligible_flag": True,
                "final_selection_support_detail_json": self._build_support_detail_json(
                    candidate=candidate,
                    support_status="not_selected_support",
                    support_position=None,
                    support_eligible_flag=True,
                    selected_support_flag=False,
                    exclusion_reason_code="final_selection_support_cutoff_not_met",
                ),
            }

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in plan:
                continue
            plan[candidate_id] = {
                "final_selection_support_position": None,
                "final_selection_support_status": "excluded_from_selection_support",
                "support_eligible_flag": False,
                "final_selection_support_detail_json": self._build_support_detail_json(
                    candidate=candidate,
                    support_status="excluded_from_selection_support",
                    support_position=None,
                    support_eligible_flag=False,
                    selected_support_flag=False,
                    exclusion_reason_code=self._support_exclusion_reason_code(candidate),
                ),
            }

        return plan

    def _persist_candidate_support(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        support_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET final_selection_support_position = %s,
                final_selection_support_status = %s,
                final_selection_support_version = %s,
                final_selection_support_config_version = %s,
                final_selection_support_detail_json = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                support_assignment["final_selection_support_position"],
                support_assignment["final_selection_support_status"],
                FINAL_SELECTION_SUPPORT_VERSION,
                FINAL_SELECTION_SUPPORT_CONFIG_VERSION,
                Jsonb(support_assignment["final_selection_support_detail_json"]),
                unequal_roll_candidate_id,
            ),
        )

    def _is_support_eligible(self, candidate: dict[str, Any]) -> bool:
        return candidate.get("shortlist_status") in {"shortlisted", "review_shortlisted"}

    def _support_exclusion_reason_code(self, candidate: dict[str, Any]) -> str:
        shortlist_status = str(candidate.get("shortlist_status") or "")
        if shortlist_status == "excluded_from_shortlist":
            return "shortlist_gate_excluded"
        if shortlist_status == "not_shortlisted":
            return "shortlist_cutoff_not_met"
        if shortlist_status == "not_evaluated":
            return "shortlist_not_evaluated"
        return "unsupported_final_selection_support_state"

    def _build_support_detail_json(
        self,
        *,
        candidate: dict[str, Any],
        support_status: str,
        support_position: int | None,
        support_eligible_flag: bool,
        selected_support_flag: bool,
        exclusion_reason_code: str | None,
    ) -> dict[str, Any]:
        shortlist_detail_json = dict(candidate.get("shortlist_detail_json") or {})
        ranking_detail_json = dict(candidate.get("ranking_detail_json") or {})
        similarity_score_detail_json = dict(candidate.get("similarity_score_detail_json") or {})
        shortlist_status = candidate.get("shortlist_status")
        return {
            "final_selection_support_version": FINAL_SELECTION_SUPPORT_VERSION,
            "final_selection_support_config_version": FINAL_SELECTION_SUPPORT_CONFIG_VERSION,
            "final_selection_support_status": support_status,
            "final_selection_support_position": support_position,
            "final_selection_support_policy": {
                "input_pool_statuses": ["shortlisted", "review_shortlisted"],
                "target_size": FINAL_SELECTION_SUPPORT_TARGET_SIZE,
                "order_strategy": "shortlist_position_order",
            },
            "final_selection_support_context": {
                "support_eligible_flag": support_eligible_flag,
                "selected_support_flag": selected_support_flag,
                "exclusion_reason_code": exclusion_reason_code,
                "input_shortlist_status": shortlist_status,
                "review_carry_forward_flag": shortlist_status == "review_shortlisted",
                "included_as_support_status": support_status,
            },
            "shortlist_context": {
                "shortlist_position": _as_int(candidate.get("shortlist_position")),
                "shortlist_status": shortlist_status,
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
            },
        }

    def _support_sort_key(self, candidate: dict[str, Any]) -> tuple[int, int, str]:
        return (
            _as_int(candidate.get("shortlist_position")) or 10_000_000,
            _as_int(candidate.get("ranking_position")) or 10_000_000,
            str(candidate.get("candidate_parcel_id") or ""),
        )


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
