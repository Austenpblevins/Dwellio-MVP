from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_candidate_final_selection_support import (
    FINAL_SELECTION_SUPPORT_TARGET_SIZE,
    UnequalRollCandidateFinalSelectionSupportService,
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
    shortlist_position: int | None,
    shortlist_status: str,
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
        "shortlist_position": shortlist_position,
        "shortlist_status": shortlist_status,
        "shortlist_version": "unequal_roll_shortlist_v1",
        "shortlist_config_version": "unequal_roll_shortlist_v1",
        "shortlist_detail_json": {
            "shortlist_policy": {
                "close_score_threshold": 0.015,
                "shortlist_target_size": 8,
            },
            "shortlist_context": {
                "close_score_policy_reordered_from_ranking": False,
            },
        },
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


def test_final_selection_support_hard_gates_ranking_and_shortlist_exclusions(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-selected",
            candidate_parcel_id=str(uuid4()),
            shortlist_position=1,
            shortlist_status="shortlisted",
            ranking_position=1,
            ranking_status="rankable",
            normalized_similarity_score=0.94,
            raw_similarity_score=94.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-excluded-ranking",
            candidate_parcel_id=str(uuid4()),
            shortlist_position=None,
            shortlist_status="excluded_from_shortlist",
            ranking_position=None,
            ranking_status="excluded_from_ranking",
            normalized_similarity_score=0.99,
            raw_similarity_score=99.0,
            eligibility_status="excluded",
            eligibility_reason_code="living_area_out_of_bounds",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-not-shortlisted",
            candidate_parcel_id=str(uuid4()),
            shortlist_position=None,
            shortlist_status="not_shortlisted",
            ranking_position=9,
            ranking_status="review_rankable",
            normalized_similarity_score=0.71,
            raw_similarity_score=71.0,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_final_selection_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateFinalSelectionSupportService().build_final_selection_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.support_eligible_count == 1
    assert result.selected_support_count == 1
    assert result.excluded_from_selection_support_count == 2
    assert connection.commit_calls == 1

    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    assert update_params_by_candidate_id["cand-selected"][1] == "selected_support"
    assert (
        update_params_by_candidate_id["cand-excluded-ranking"][1]
        == "excluded_from_selection_support"
    )
    assert (
        update_params_by_candidate_id["cand-excluded-ranking"][4].obj[
            "final_selection_support_context"
        ]["exclusion_reason_code"]
        == "shortlist_gate_excluded"
    )
    assert (
        update_params_by_candidate_id["cand-not-shortlisted"][4].obj[
            "final_selection_support_context"
        ]["exclusion_reason_code"]
        == "shortlist_cutoff_not_met"
    )


def test_final_selection_support_carries_review_shortlisted_rows_forward(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-rankable",
            candidate_parcel_id=str(uuid4()),
            shortlist_position=1,
            shortlist_status="shortlisted",
            ranking_position=1,
            ranking_status="rankable",
            normalized_similarity_score=0.95,
            raw_similarity_score=95.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-review",
            candidate_parcel_id=str(uuid4()),
            shortlist_position=2,
            shortlist_status="review_shortlisted",
            ranking_position=2,
            ranking_status="review_rankable",
            normalized_similarity_score=0.82,
            raw_similarity_score=82.0,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_final_selection_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateFinalSelectionSupportService().build_final_selection_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.selected_support_count == 2
    assert result.review_selected_support_count == 1
    review_update = connection.cursor_instance.execute_calls[2][1]
    assert review_update[1] == "review_selected_support"
    assert (
        review_update[4].obj["final_selection_support_context"]["review_carry_forward_flag"]
        is True
    )


def test_final_selection_support_orders_by_shortlist_position(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-two",
            candidate_parcel_id="parcel-b",
            shortlist_position=2,
            shortlist_status="review_shortlisted",
            ranking_position=2,
            ranking_status="review_rankable",
            normalized_similarity_score=0.88,
            raw_similarity_score=88.0,
            eligibility_status="review",
            eligibility_reason_code="quality_adjacent",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-one",
            candidate_parcel_id="parcel-a",
            shortlist_position=1,
            shortlist_status="shortlisted",
            ranking_position=1,
            ranking_status="rankable",
            normalized_similarity_score=0.91,
            raw_similarity_score=91.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_final_selection_support.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateFinalSelectionSupportService().build_final_selection_support_for_run(
        unequal_roll_run_id="run-1"
    )

    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    assert update_params_by_candidate_id["cand-one"][0] == 1
    assert update_params_by_candidate_id["cand-two"][0] == 2


def test_final_selection_support_applies_support_cutoff_policy(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-{index}",
            candidate_parcel_id=f"parcel-{index}",
            shortlist_position=index,
            shortlist_status="shortlisted" if index < 4 else "review_shortlisted",
            ranking_position=index,
            ranking_status="rankable" if index < 4 else "review_rankable",
            normalized_similarity_score=0.99 - (index * 0.01),
            raw_similarity_score=99.0 - float(index),
            eligibility_status="eligible" if index < 4 else "review",
            eligibility_reason_code=None if index < 4 else "wide_living_area_gap",
        )
        for index in range(1, FINAL_SELECTION_SUPPORT_TARGET_SIZE + 2)
    ]
    connection = SequenceConnection(fetchall_results=[candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_final_selection_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateFinalSelectionSupportService().build_final_selection_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.selected_support_count == FINAL_SELECTION_SUPPORT_TARGET_SIZE
    update_params_by_candidate_id = {
        params[5]: params for _, params in connection.cursor_instance.execute_calls[1:]
    }
    cutoff_candidate = update_params_by_candidate_id[
        f"cand-{FINAL_SELECTION_SUPPORT_TARGET_SIZE + 1}"
    ]
    assert cutoff_candidate[0] is None
    assert cutoff_candidate[1] == "not_selected_support"
    assert (
        cutoff_candidate[4].obj["final_selection_support_context"]["exclusion_reason_code"]
        == "final_selection_support_cutoff_not_met"
    )
    assert (
        cutoff_candidate[4].obj["final_selection_support_policy"]["target_size"]
        == FINAL_SELECTION_SUPPORT_TARGET_SIZE
    )


def test_final_selection_support_preserves_fort_bend_bathroom_boundary(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-fb",
            candidate_parcel_id=str(uuid4()),
            shortlist_position=1,
            shortlist_status="review_shortlisted",
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
        "app.services.unequal_roll_candidate_final_selection_support.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateFinalSelectionSupportService().build_final_selection_support_for_run(
        unequal_roll_run_id="run-1"
    )

    update_params = connection.cursor_instance.execute_calls[1][1]
    detail_json = update_params[4].obj
    assert detail_json["final_selection_support_status"] == "review_selected_support"
    assert detail_json["score_context"]["fort_bend_bathroom_modifier"]["review_required"] is True
    assert "full_baths_derived" not in detail_json["score_context"][
        "fort_bend_bathroom_modifier"
    ]


def test_final_selection_support_raises_when_run_has_no_shortlist_candidates(
    monkeypatch,
) -> None:
    connection = SequenceConnection(fetchall_results=[[]])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_final_selection_support.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(LookupError, match="Unequal-roll shortlist candidates not found"):
        UnequalRollCandidateFinalSelectionSupportService().build_final_selection_support_for_run(
            unequal_roll_run_id="run-empty"
        )
