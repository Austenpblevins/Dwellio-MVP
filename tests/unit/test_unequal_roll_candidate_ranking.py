from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_candidate_ranking import (
    UnequalRollCandidateRankingService,
)


class SequenceCursor:
    def __init__(self, *, fetchall_results: list[list[dict[str, object]]]) -> None:
        self.fetchall_results = fetchall_results
        self.fetchall_index = 0
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> SequenceCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self.execute_calls.append((query, params))

    def fetchall(self) -> list[dict[str, object]]:
        if self.fetchall_index >= len(self.fetchall_results):
            return []
        result = self.fetchall_results[self.fetchall_index]
        self.fetchall_index += 1
        return result


class SequenceConnection:
    def __init__(self, *, fetchall_results: list[list[dict[str, object]]]) -> None:
        self.cursor_instance = SequenceCursor(fetchall_results=fetchall_results)
        self.commit_calls = 0

    def cursor(self) -> SequenceCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_calls += 1


def connection_factory(connection: SequenceConnection):
    @contextmanager
    def _connection():
        yield connection

    return _connection


def _candidate(
    *,
    unequal_roll_candidate_id: str,
    candidate_parcel_id: str,
    discovery_tier: str = "same_neighborhood",
    eligibility_status: str = "eligible",
    eligibility_reason_code: str | None = None,
    normalized_similarity_score: float = 0.95,
    raw_similarity_score: float = 95.0,
    fort_bend_bathroom_modifier: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "unequal_roll_candidate_id": unequal_roll_candidate_id,
        "unequal_roll_run_id": "run-1",
        "candidate_parcel_id": candidate_parcel_id,
        "discovery_tier": discovery_tier,
        "eligibility_status": eligibility_status,
        "eligibility_reason_code": eligibility_reason_code,
        "normalized_similarity_score": normalized_similarity_score,
        "raw_similarity_score": raw_similarity_score,
        "scoring_version": "unequal_roll_similarity_v1",
        "scoring_config_version": "unequal_roll_similarity_v1",
        "similarity_score_detail_json": {
            "base_similarity_score": raw_similarity_score,
            "eligibility_status_multiplier": {
                "eligibility_status": eligibility_status,
                "value": 1.0 if eligibility_status == "eligible" else 0.85,
            },
            "fort_bend_bathroom_modifier": fort_bend_bathroom_modifier
            or {
                "value": 1.0,
                "attachment_status": "not_applicable",
                "review_required": False,
            },
            "eligibility_context": {
                "primary_reason_code": eligibility_reason_code,
                "secondary_reason_codes": [],
            },
        },
    }


def test_rank_candidates_blocks_excluded_rows_from_rank_positions(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-eligible",
            candidate_parcel_id=str(uuid4()),
            eligibility_status="eligible",
            normalized_similarity_score=0.88,
            raw_similarity_score=88.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-excluded",
            candidate_parcel_id=str(uuid4()),
            eligibility_status="excluded",
            eligibility_reason_code="living_area_out_of_bounds",
            normalized_similarity_score=0.99,
            raw_similarity_score=99.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_ranking.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateRankingService().rank_candidates_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.total_candidates == 2
    assert result.rankable_count == 1
    assert result.review_rankable_count == 0
    assert result.excluded_from_ranking_count == 1
    assert connection.commit_calls == 1

    eligible_update = connection.cursor_instance.execute_calls[1][1]
    excluded_update = connection.cursor_instance.execute_calls[2][1]
    assert eligible_update[0] == 1
    assert eligible_update[1] == "rankable"
    assert excluded_update[0] is None
    assert excluded_update[1] == "excluded_from_ranking"
    assert excluded_update[4].obj["ranking_exclusion_reason_code"] == "eligibility_status_excluded"
    assert excluded_update[4].obj["rankable_flag"] is False


def test_rank_candidates_keeps_review_rows_rankable_and_flagged(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-eligible",
            candidate_parcel_id=str(uuid4()),
            eligibility_status="eligible",
            normalized_similarity_score=0.93,
            raw_similarity_score=93.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-review",
            candidate_parcel_id=str(uuid4()),
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
            normalized_similarity_score=0.82,
            raw_similarity_score=82.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_ranking.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateRankingService().rank_candidates_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.rankable_count == 1
    assert result.review_rankable_count == 1
    review_update = connection.cursor_instance.execute_calls[2][1]
    assert review_update[0] == 2
    assert review_update[1] == "review_rankable"
    assert review_update[4].obj["eligibility_context"]["review_rankable_flag"] is True
    assert review_update[4].obj["score_context"]["primary_reason_code"] == "fallback_geography_used"


def test_rank_candidates_orders_rankable_rows_by_persisted_score(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-low",
            candidate_parcel_id="parcel-c",
            discovery_tier="county_sfr_fallback",
            eligibility_status="eligible",
            normalized_similarity_score=0.77,
            raw_similarity_score=77.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-high",
            candidate_parcel_id="parcel-a",
            discovery_tier="same_neighborhood",
            eligibility_status="eligible",
            normalized_similarity_score=0.96,
            raw_similarity_score=96.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-mid",
            candidate_parcel_id="parcel-b",
            discovery_tier="same_neighborhood",
            eligibility_status="review",
            eligibility_reason_code="quality_adjacent",
            normalized_similarity_score=0.81,
            raw_similarity_score=81.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_ranking.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateRankingService().rank_candidates_for_run(unequal_roll_run_id="run-1")

    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    assert update_params_by_candidate_id["cand-high"][0] == 1
    assert update_params_by_candidate_id["cand-mid"][0] == 2
    assert update_params_by_candidate_id["cand-low"][0] == 3


def test_rank_candidates_breaks_score_ties_in_favor_of_plain_rankable(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-review-tie",
            candidate_parcel_id="parcel-b",
            discovery_tier="same_neighborhood",
            eligibility_status="review",
            eligibility_reason_code="quality_adjacent",
            normalized_similarity_score=0.81,
            raw_similarity_score=81.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-rankable-tie",
            candidate_parcel_id="parcel-a",
            discovery_tier="same_neighborhood",
            eligibility_status="eligible",
            normalized_similarity_score=0.81,
            raw_similarity_score=81.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_ranking.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateRankingService().rank_candidates_for_run(unequal_roll_run_id="run-1")

    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    assert update_params_by_candidate_id["cand-rankable-tie"][0] == 1
    assert update_params_by_candidate_id["cand-rankable-tie"][1] == "rankable"
    assert update_params_by_candidate_id["cand-review-tie"][0] == 2
    assert update_params_by_candidate_id["cand-review-tie"][1] == "review_rankable"
    assert (
        "rankable_before_review_rankable_on_score_ties"
        in update_params_by_candidate_id["cand-rankable-tie"][4].obj["ranking_basis"][
            "tie_breakers"
        ]
    )


def test_rank_candidates_persists_ranking_detail_with_fort_bend_boundary(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-fb",
            candidate_parcel_id=str(uuid4()),
            eligibility_status="review",
            eligibility_reason_code="fort_bend_bathroom_status_review",
            normalized_similarity_score=0.79,
            raw_similarity_score=79.0,
            fort_bend_bathroom_modifier={
                "value": 0.95,
                "attachment_status": "attached",
                "bathroom_count_status": "no_bathroom_source",
                "bathroom_count_confidence": "none",
                "review_required": True,
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_ranking.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateRankingService().rank_candidates_for_run(unequal_roll_run_id="run-1")

    update_params = connection.cursor_instance.execute_calls[1][1]
    ranking_detail_json = update_params[4].obj
    assert ranking_detail_json["ranking_status"] == "review_rankable"
    assert (
        ranking_detail_json["score_context"]["fort_bend_bathroom_modifier"]["review_required"]
        is True
    )
    assert "full_baths_derived" not in ranking_detail_json["score_context"][
        "fort_bend_bathroom_modifier"
    ]


def test_rank_candidates_raises_when_run_has_no_candidates(monkeypatch) -> None:
    connection = SequenceConnection(fetchall_results=[[]])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_ranking.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(LookupError, match="Unequal-roll candidates not found"):
        UnequalRollCandidateRankingService().rank_candidates_for_run(
            unequal_roll_run_id="run-empty"
        )
