from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_candidate_shortlist import (
    SHORTLIST_CLOSE_SCORE_THRESHOLD,
    SHORTLIST_TARGET_SIZE,
    UnequalRollCandidateShortlistService,
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
    ranking_position: int | None,
    ranking_status: str,
    normalized_similarity_score: float | None,
    raw_similarity_score: float | None,
    discovery_tier: str = "same_neighborhood",
    eligibility_status: str = "eligible",
    eligibility_reason_code: str | None = None,
    fort_bend_bathroom_modifier: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "unequal_roll_candidate_id": unequal_roll_candidate_id,
        "unequal_roll_run_id": "run-1",
        "candidate_parcel_id": candidate_parcel_id,
        "ranking_position": ranking_position,
        "ranking_status": ranking_status,
        "ranking_version": "unequal_roll_ranking_v1",
        "ranking_config_version": "unequal_roll_ranking_v1",
        "ranking_detail_json": {
            "ranking_basis": {
                "order_strategy": "eligibility_gate_then_similarity_score",
            }
        },
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


def test_shortlist_blocks_excluded_from_ranking_candidates(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-shortlisted",
            candidate_parcel_id=str(uuid4()),
            ranking_position=1,
            ranking_status="rankable",
            normalized_similarity_score=0.94,
            raw_similarity_score=94.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-excluded",
            candidate_parcel_id=str(uuid4()),
            ranking_position=None,
            ranking_status="excluded_from_ranking",
            normalized_similarity_score=0.99,
            raw_similarity_score=99.0,
            eligibility_status="excluded",
            eligibility_reason_code="living_area_out_of_bounds",
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateShortlistService().build_shortlist_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.shortlist_eligible_count == 1
    assert result.shortlisted_count == 1
    assert result.excluded_from_shortlist_count == 1
    assert connection.commit_calls == 1

    shortlisted_update = connection.cursor_instance.execute_calls[1][1]
    excluded_update = connection.cursor_instance.execute_calls[2][1]
    assert shortlisted_update[0] == 1
    assert shortlisted_update[1] == "shortlisted"
    assert excluded_update[0] is None
    assert excluded_update[1] == "excluded_from_shortlist"
    assert excluded_update[4].obj["shortlist_context"]["shortlist_exclusion_reason_code"] == (
        "ranking_gate_excluded"
    )


def test_shortlist_keeps_review_rankable_candidates_visible_when_warranted(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-rankable",
            candidate_parcel_id=str(uuid4()),
            ranking_position=1,
            ranking_status="rankable",
            normalized_similarity_score=0.95,
            raw_similarity_score=95.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-review",
            candidate_parcel_id=str(uuid4()),
            ranking_position=2,
            ranking_status="review_rankable",
            normalized_similarity_score=0.83,
            raw_similarity_score=83.0,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateShortlistService().build_shortlist_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.shortlisted_count == 2
    assert result.review_shortlisted_count == 1
    review_update = connection.cursor_instance.execute_calls[2][1]
    assert review_update[1] == "review_shortlisted"
    assert review_update[4].obj["shortlist_context"]["included_as_ranking_status"] == (
        "review_rankable"
    )


def test_shortlist_remains_score_first_outside_close_score_band(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-review-high",
            candidate_parcel_id="parcel-b",
            ranking_position=1,
            ranking_status="review_rankable",
            normalized_similarity_score=0.91,
            raw_similarity_score=91.0,
            eligibility_status="review",
            eligibility_reason_code="quality_adjacent",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-rankable-lower",
            candidate_parcel_id="parcel-a",
            ranking_position=2,
            ranking_status="rankable",
            normalized_similarity_score=0.88,
            raw_similarity_score=88.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateShortlistService().build_shortlist_for_run(unequal_roll_run_id="run-1")

    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    assert update_params_by_candidate_id["cand-review-high"][0] == 1
    assert update_params_by_candidate_id["cand-review-high"][1] == "review_shortlisted"
    assert (
        update_params_by_candidate_id["cand-review-high"][4].obj["shortlist_context"][
            "close_score_policy_reordered_from_ranking"
        ]
        is False
    )


def test_shortlist_prefers_rankable_inside_close_score_band(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-review-close",
            candidate_parcel_id="parcel-b",
            ranking_position=1,
            ranking_status="review_rankable",
            normalized_similarity_score=0.905,
            raw_similarity_score=90.5,
            eligibility_status="review",
            eligibility_reason_code="quality_adjacent",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-rankable-close",
            candidate_parcel_id="parcel-a",
            ranking_position=2,
            ranking_status="rankable",
            normalized_similarity_score=0.895,
            raw_similarity_score=89.5,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateShortlistService().build_shortlist_for_run(unequal_roll_run_id="run-1")

    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    assert update_params_by_candidate_id["cand-rankable-close"][0] == 1
    assert update_params_by_candidate_id["cand-review-close"][0] == 2
    assert (
        update_params_by_candidate_id["cand-rankable-close"][4].obj["shortlist_context"][
            "close_score_policy_reordered_from_ranking"
        ]
        is True
    )
    assert (
        update_params_by_candidate_id["cand-rankable-close"][4].obj["shortlist_policy"][
            "close_score_threshold"
        ]
        == SHORTLIST_CLOSE_SCORE_THRESHOLD
    )


def test_shortlist_persists_cutoff_detail_for_non_shortlisted_candidates(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-{index}",
            candidate_parcel_id=f"parcel-{index}",
            ranking_position=index,
            ranking_status="rankable",
            normalized_similarity_score=0.99 - (index * 0.01),
            raw_similarity_score=99.0 - float(index),
        )
        for index in range(1, SHORTLIST_TARGET_SIZE + 2)
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateShortlistService().build_shortlist_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.shortlisted_count == SHORTLIST_TARGET_SIZE
    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    last_candidate = update_params_by_candidate_id[f"cand-{SHORTLIST_TARGET_SIZE + 1}"]
    assert last_candidate[0] is None
    assert last_candidate[1] == "not_shortlisted"
    assert last_candidate[4].obj["shortlist_context"]["shortlist_exclusion_reason_code"] == (
        "shortlist_cutoff_not_met"
    )
    assert last_candidate[4].obj["shortlist_policy"]["shortlist_target_size"] == (
        SHORTLIST_TARGET_SIZE
    )


def test_shortlist_preserves_fort_bend_bathroom_boundary(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-fb",
            candidate_parcel_id=str(uuid4()),
            ranking_position=1,
            ranking_status="review_rankable",
            normalized_similarity_score=0.82,
            raw_similarity_score=82.0,
            discovery_tier="same_neighborhood",
            eligibility_status="review",
            eligibility_reason_code="fort_bend_bathroom_status_review",
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
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateShortlistService().build_shortlist_for_run(unequal_roll_run_id="run-1")

    update_params = connection.cursor_instance.execute_calls[1][1]
    shortlist_detail_json = update_params[4].obj
    assert shortlist_detail_json["shortlist_status"] == "review_shortlisted"
    assert shortlist_detail_json["score_context"]["fort_bend_bathroom_modifier"][
        "review_required"
    ] is True
    assert "full_baths_derived" not in shortlist_detail_json["score_context"][
        "fort_bend_bathroom_modifier"
    ]


def test_shortlist_raises_when_run_has_no_ranked_candidates(monkeypatch) -> None:
    connection = SequenceConnection(fetchall_results=[[]])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_shortlist.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(LookupError, match="Unequal-roll ranking candidates not found"):
        UnequalRollCandidateShortlistService().build_shortlist_for_run(
            unequal_roll_run_id="run-empty"
        )
