from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection

SHORTLIST_VERSION = "unequal_roll_shortlist_v2"
SHORTLIST_CONFIG_VERSION = "unequal_roll_shortlist_v2"
SHORTLIST_TARGET_SIZE = 20
SHORTLIST_CLOSE_SCORE_THRESHOLD = 0.015


@dataclass(frozen=True)
class UnequalRollCandidateShortlistResult:
    unequal_roll_run_id: str
    total_candidates: int
    shortlist_eligible_count: int
    shortlisted_count: int
    review_shortlisted_count: int
    excluded_from_shortlist_count: int


class UnequalRollCandidateShortlistService:
    def build_shortlist_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateShortlistResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                candidates = self._fetch_candidates(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if not candidates:
                    raise LookupError(
                        f"Unequal-roll ranking candidates not found for run {unequal_roll_run_id}."
                    )

                shortlist_plan = self._build_shortlist_plan(candidates)
                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    shortlist_assignment = shortlist_plan[candidate_id]
                    self._persist_candidate_shortlist(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        shortlist_assignment=shortlist_assignment,
                    )
            connection.commit()

        shortlist_eligible_count = sum(
            1
            for shortlist_assignment in shortlist_plan.values()
            if shortlist_assignment["shortlist_eligible_flag"]
        )
        shortlisted_count = sum(
            1
            for shortlist_assignment in shortlist_plan.values()
            if shortlist_assignment["shortlist_status"] in {"shortlisted", "review_shortlisted"}
        )
        review_shortlisted_count = sum(
            1
            for shortlist_assignment in shortlist_plan.values()
            if shortlist_assignment["shortlist_status"] == "review_shortlisted"
        )
        excluded_from_shortlist_count = sum(
            1
            for shortlist_assignment in shortlist_plan.values()
            if shortlist_assignment["shortlist_status"] == "excluded_from_shortlist"
        )
        return UnequalRollCandidateShortlistResult(
            unequal_roll_run_id=unequal_roll_run_id,
            total_candidates=len(candidates),
            shortlist_eligible_count=shortlist_eligible_count,
            shortlisted_count=shortlisted_count,
            review_shortlisted_count=review_shortlisted_count,
            excluded_from_shortlist_count=excluded_from_shortlist_count,
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
            ORDER BY ranking_position NULLS LAST, candidate_parcel_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _build_shortlist_plan(
        self,
        candidates: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        plan: dict[str, dict[str, Any]] = {}
        shortlist_eligible = [
            candidate for candidate in candidates if self._is_shortlist_eligible(candidate)
        ]
        shortlist_order = self._apply_close_score_policy(shortlist_eligible)
        effective_order_position_by_candidate_id = {
            str(candidate["unequal_roll_candidate_id"]): position
            for position, candidate in enumerate(shortlist_order, start=1)
        }

        shortlisted_candidates = shortlist_order[:SHORTLIST_TARGET_SIZE]
        shortlisted_candidate_ids = {
            str(candidate["unequal_roll_candidate_id"]) for candidate in shortlisted_candidates
        }
        for shortlist_position, candidate in enumerate(shortlisted_candidates, start=1):
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            shortlist_status = (
                "review_shortlisted"
                if candidate.get("ranking_status") == "review_rankable"
                else "shortlisted"
            )
            plan[candidate_id] = {
                "shortlist_position": shortlist_position,
                "shortlist_status": shortlist_status,
                "shortlist_eligible_flag": True,
                "shortlist_detail_json": self._build_shortlist_detail_json(
                    candidate=candidate,
                    shortlist_status=shortlist_status,
                    shortlist_position=shortlist_position,
                    shortlist_eligible_flag=True,
                    shortlisted_flag=True,
                    shortlist_exclusion_reason_code=None,
                    effective_order_position=effective_order_position_by_candidate_id[candidate_id],
                ),
            }

        for candidate in shortlist_eligible:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in shortlisted_candidate_ids:
                continue
            plan[candidate_id] = {
                "shortlist_position": None,
                "shortlist_status": "not_shortlisted",
                "shortlist_eligible_flag": True,
                "shortlist_detail_json": self._build_shortlist_detail_json(
                    candidate=candidate,
                    shortlist_status="not_shortlisted",
                    shortlist_position=None,
                    shortlist_eligible_flag=True,
                    shortlisted_flag=False,
                    shortlist_exclusion_reason_code="shortlist_cutoff_not_met",
                    effective_order_position=effective_order_position_by_candidate_id[candidate_id],
                ),
            }

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in plan:
                continue
            plan[candidate_id] = {
                "shortlist_position": None,
                "shortlist_status": "excluded_from_shortlist",
                "shortlist_eligible_flag": False,
                "shortlist_detail_json": self._build_shortlist_detail_json(
                    candidate=candidate,
                    shortlist_status="excluded_from_shortlist",
                    shortlist_position=None,
                    shortlist_eligible_flag=False,
                    shortlisted_flag=False,
                    shortlist_exclusion_reason_code=self._shortlist_exclusion_reason_code(
                        candidate
                    ),
                    effective_order_position=None,
                ),
            }

        return plan

    def _persist_candidate_shortlist(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        shortlist_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET shortlist_position = %s,
                shortlist_status = %s,
                shortlist_version = %s,
                shortlist_config_version = %s,
                shortlist_detail_json = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                shortlist_assignment["shortlist_position"],
                shortlist_assignment["shortlist_status"],
                SHORTLIST_VERSION,
                SHORTLIST_CONFIG_VERSION,
                Jsonb(shortlist_assignment["shortlist_detail_json"]),
                unequal_roll_candidate_id,
            ),
        )

    def _is_shortlist_eligible(self, candidate: dict[str, Any]) -> bool:
        if candidate.get("ranking_status") == "excluded_from_ranking":
            return False
        if candidate.get("ranking_position") is None:
            return False
        return candidate.get("normalized_similarity_score") is not None

    def _apply_close_score_policy(
        self,
        shortlist_eligible: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        baseline = sorted(
            shortlist_eligible,
            key=lambda candidate: (
                _as_int(candidate.get("ranking_position")) or 10_000_000,
                str(candidate.get("candidate_parcel_id") or ""),
            ),
        )
        ordered: list[dict[str, Any]] = []
        for candidate in baseline:
            insert_idx = len(ordered)
            while insert_idx > 0 and self._should_promote_for_close_score(
                candidate=candidate,
                previous_candidate=ordered[insert_idx - 1],
            ):
                insert_idx -= 1
            ordered.insert(insert_idx, candidate)
        return ordered

    def _should_promote_for_close_score(
        self,
        *,
        candidate: dict[str, Any],
        previous_candidate: dict[str, Any],
    ) -> bool:
        if candidate.get("ranking_status") != "rankable":
            return False
        if previous_candidate.get("ranking_status") != "review_rankable":
            return False

        candidate_score = _as_float(candidate.get("normalized_similarity_score"))
        previous_score = _as_float(previous_candidate.get("normalized_similarity_score"))
        if candidate_score is None or previous_score is None:
            return False
        return abs(candidate_score - previous_score) <= SHORTLIST_CLOSE_SCORE_THRESHOLD

    def _shortlist_exclusion_reason_code(self, candidate: dict[str, Any]) -> str:
        if candidate.get("ranking_status") == "excluded_from_ranking":
            return "ranking_gate_excluded"
        if candidate.get("ranking_position") is None:
            return "missing_ranking_position"
        if candidate.get("normalized_similarity_score") is None:
            return "missing_normalized_similarity_score"
        return "unsupported_shortlist_state"

    def _build_shortlist_detail_json(
        self,
        *,
        candidate: dict[str, Any],
        shortlist_status: str,
        shortlist_position: int | None,
        shortlist_eligible_flag: bool,
        shortlisted_flag: bool,
        shortlist_exclusion_reason_code: str | None,
        effective_order_position: int | None,
    ) -> dict[str, Any]:
        ranking_detail_json = dict(candidate.get("ranking_detail_json") or {})
        similarity_score_detail_json = dict(candidate.get("similarity_score_detail_json") or {})
        ranking_position = _as_int(candidate.get("ranking_position"))
        close_score_policy_reordered = (
            effective_order_position is not None
            and ranking_position is not None
            and effective_order_position != ranking_position
        )
        return {
            "shortlist_version": SHORTLIST_VERSION,
            "shortlist_config_version": SHORTLIST_CONFIG_VERSION,
            "shortlist_status": shortlist_status,
            "shortlist_position": shortlist_position,
            "shortlist_policy": {
                "order_strategy": (
                    "score_first_with_rankable_preference_inside_close_score_band"
                ),
                "score_field": "normalized_similarity_score",
                "close_score_threshold": SHORTLIST_CLOSE_SCORE_THRESHOLD,
                "shortlist_target_size": SHORTLIST_TARGET_SIZE,
            },
            "shortlist_context": {
                "shortlist_eligible_flag": shortlist_eligible_flag,
                "shortlisted_flag": shortlisted_flag,
                "shortlist_exclusion_reason_code": shortlist_exclusion_reason_code,
                "effective_shortlist_order_position": effective_order_position,
                "close_score_policy_reordered_from_ranking": close_score_policy_reordered,
                "included_as_ranking_status": candidate.get("ranking_status"),
            },
            "ranking_context": {
                "ranking_position": ranking_position,
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


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
