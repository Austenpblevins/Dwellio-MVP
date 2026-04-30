from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection

RANKING_VERSION = "unequal_roll_ranking_v1"
RANKING_CONFIG_VERSION = "unequal_roll_ranking_v1"


@dataclass(frozen=True)
class UnequalRollCandidateRankingResult:
    unequal_roll_run_id: str
    total_candidates: int
    rankable_count: int
    review_rankable_count: int
    excluded_from_ranking_count: int


class UnequalRollCandidateRankingService:
    def rank_candidates_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateRankingResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                candidates = self._fetch_candidates(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if not candidates:
                    raise LookupError(
                        f"Unequal-roll candidates not found for run {unequal_roll_run_id}."
                    )

                ranking_plan = self._build_ranking_plan(candidates)
                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    ranking_assignment = ranking_plan[candidate_id]
                    self._persist_candidate_ranking(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        ranking_assignment=ranking_assignment,
                    )
            connection.commit()

        rankable_count = sum(
            1
            for ranking_assignment in ranking_plan.values()
            if ranking_assignment["ranking_status"] == "rankable"
        )
        review_rankable_count = sum(
            1
            for ranking_assignment in ranking_plan.values()
            if ranking_assignment["ranking_status"] == "review_rankable"
        )
        excluded_from_ranking_count = sum(
            1
            for ranking_assignment in ranking_plan.values()
            if ranking_assignment["ranking_status"] == "excluded_from_ranking"
        )
        return UnequalRollCandidateRankingResult(
            unequal_roll_run_id=unequal_roll_run_id,
            total_candidates=len(candidates),
            rankable_count=rankable_count,
            review_rankable_count=review_rankable_count,
            excluded_from_ranking_count=excluded_from_ranking_count,
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
            ORDER BY created_at, unequal_roll_candidate_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _build_ranking_plan(
        self,
        candidates: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        plan: dict[str, dict[str, Any]] = {}
        rankable_candidates = [
            candidate for candidate in candidates if self._is_rankable_candidate(candidate)
        ]
        rankable_candidates.sort(key=self._ranking_sort_key)

        for ranking_position, candidate in enumerate(rankable_candidates, start=1):
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            ranking_status = (
                "review_rankable"
                if candidate.get("eligibility_status") == "review"
                else "rankable"
            )
            plan[candidate_id] = {
                "ranking_position": ranking_position,
                "ranking_status": ranking_status,
                "ranking_detail_json": self._build_ranking_detail_json(
                    candidate=candidate,
                    ranking_status=ranking_status,
                    ranking_position=ranking_position,
                    rankable_flag=True,
                    exclusion_reason_code=None,
                ),
            }

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            if candidate_id in plan:
                continue
            plan[candidate_id] = {
                "ranking_position": None,
                "ranking_status": "excluded_from_ranking",
                "ranking_detail_json": self._build_ranking_detail_json(
                    candidate=candidate,
                    ranking_status="excluded_from_ranking",
                    ranking_position=None,
                    rankable_flag=False,
                    exclusion_reason_code=self._ranking_exclusion_reason_code(candidate),
                ),
            }

        return plan

    def _persist_candidate_ranking(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        ranking_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET ranking_position = %s,
                ranking_status = %s,
                ranking_version = %s,
                ranking_config_version = %s,
                ranking_detail_json = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                ranking_assignment["ranking_position"],
                ranking_assignment["ranking_status"],
                RANKING_VERSION,
                RANKING_CONFIG_VERSION,
                Jsonb(ranking_assignment["ranking_detail_json"]),
                unequal_roll_candidate_id,
            ),
        )

    def _is_rankable_candidate(self, candidate: dict[str, Any]) -> bool:
        if candidate.get("eligibility_status") == "excluded":
            return False
        return self._score_available(candidate)

    def _score_available(self, candidate: dict[str, Any]) -> bool:
        return candidate.get("normalized_similarity_score") is not None and (
            candidate.get("raw_similarity_score") is not None
        )

    def _ranking_exclusion_reason_code(self, candidate: dict[str, Any]) -> str:
        if candidate.get("eligibility_status") == "excluded":
            return "eligibility_status_excluded"
        if not self._score_available(candidate):
            return "missing_similarity_score"
        return "unsupported_ranking_state"

    def _build_ranking_detail_json(
        self,
        *,
        candidate: dict[str, Any],
        ranking_status: str,
        ranking_position: int | None,
        rankable_flag: bool,
        exclusion_reason_code: str | None,
    ) -> dict[str, Any]:
        similarity_score_detail_json = dict(candidate.get("similarity_score_detail_json") or {})
        return {
            "ranking_version": RANKING_VERSION,
            "ranking_config_version": RANKING_CONFIG_VERSION,
            "ranking_status": ranking_status,
            "rankable_flag": rankable_flag,
            "ranking_position": ranking_position,
            "ranking_exclusion_reason_code": exclusion_reason_code,
            "ranking_basis": {
                "order_strategy": "eligibility_gate_then_similarity_score",
                "normalized_similarity_score": _as_float(
                    candidate.get("normalized_similarity_score")
                ),
                "raw_similarity_score": _as_float(candidate.get("raw_similarity_score")),
                "discovery_tier": candidate.get("discovery_tier"),
                "tie_breakers": [
                    "rankable_before_review_rankable_on_score_ties",
                    "same_neighborhood_before_fallback_on_score_ties",
                    "candidate_parcel_id_ascending",
                ],
            },
            "eligibility_context": {
                "eligibility_status": candidate.get("eligibility_status"),
                "eligibility_reason_code": candidate.get("eligibility_reason_code"),
                "review_rankable_flag": ranking_status == "review_rankable",
                "excluded_from_ranking_flag": ranking_status == "excluded_from_ranking",
            },
            "score_context": {
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

    def _ranking_sort_key(self, candidate: dict[str, Any]) -> tuple[float, float, int, str]:
        normalized_similarity_score = _as_float(candidate.get("normalized_similarity_score"))
        raw_similarity_score = _as_float(candidate.get("raw_similarity_score"))
        eligibility_status = str(candidate.get("eligibility_status") or "")
        eligibility_tie_break_order = 0 if eligibility_status == "eligible" else 1
        discovery_tier = str(candidate.get("discovery_tier") or "")
        discovery_tier_order = 0 if discovery_tier == "same_neighborhood" else 1
        return (
            -(normalized_similarity_score or 0.0),
            -(raw_similarity_score or 0.0),
            eligibility_tie_break_order,
            discovery_tier_order,
            str(candidate.get("candidate_parcel_id") or ""),
        )


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
