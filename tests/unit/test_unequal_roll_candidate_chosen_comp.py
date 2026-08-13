from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_candidate_chosen_comp import (
    CHOSEN_COMP_VERSION,
    FINAL_COMP_ACCEPTABLE_TARGET_MAX,
    FINAL_COMP_ACCEPTABLE_TARGET_MIN,
    FINAL_COMP_AUTO_SUPPORTED_MINIMUM,
    FINAL_COMP_MANUAL_REVIEW_MAX,
    FINAL_COMP_MANUAL_REVIEW_MIN,
    FINAL_COMP_PREFERRED_TARGET_MAX,
    FINAL_COMP_PREFERRED_TARGET_MIN,
    UnequalRollCandidateChosenCompService,
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

    def fetchone(self) -> dict[str, object] | None:
        if self.fetchall_index >= len(self.fetchall_results):
            return None
        result = self.fetchall_results[self.fetchall_index]
        self.fetchall_index += 1
        if not result:
            return None
        return result[0]


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


def _run_context(
    *,
    summary_json: dict[str, object] | None = None,
    subject_neighborhood_code: str = "N1",
    subject_subdivision_name: str = "SUB1",
) -> dict[str, object]:
    return {
        "unequal_roll_run_id": "run-1",
        "summary_json": summary_json or {
            "candidate_discovery_summary": {
                "discovered_count": 100,
                "same_neighborhood_count": 25,
                "county_sfr_fallback_count": 75,
                "eligible_count": 10,
                "review_count": 5,
                "excluded_count": 85,
                "fallback_used": True,
                "same_neighborhood_insufficient_reason": "same_neighborhood_supply_below_preferred_pool",
            }
        },
        "subject_parcel_id": str(uuid4()),
        "county_id": "harris",
        "tax_year": 2026,
        "subject_neighborhood_code": subject_neighborhood_code,
        "subject_subdivision_name": subject_subdivision_name,
    }


def _candidate(
    *,
    unequal_roll_candidate_id: str,
    candidate_parcel_id: str,
    address: str = "100 MAIN ST",
    neighborhood_code: str = "N1",
    subdivision_name: str = "SUB1",
    final_selection_support_position: int | None,
    final_selection_support_status: str,
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
        "address": address,
        "neighborhood_code": neighborhood_code,
        "subdivision_name": subdivision_name,
        "final_selection_support_position": final_selection_support_position,
        "final_selection_support_status": final_selection_support_status,
        "final_selection_support_version": "unequal_roll_final_selection_support_v2",
        "final_selection_support_config_version": "unequal_roll_final_selection_support_v2",
        "final_selection_support_detail_json": {
            "final_selection_support_policy": {
                "target_size": 20,
                "order_strategy": "shortlist_position_order",
            },
            "final_selection_support_context": {
                "exclusion_reason_code": None,
            },
        },
        "shortlist_position": shortlist_position,
        "shortlist_status": shortlist_status,
        "shortlist_version": "unequal_roll_shortlist_v2",
        "shortlist_config_version": "unequal_roll_shortlist_v2",
        "shortlist_detail_json": {
            "shortlist_policy": {
                "close_score_threshold": 0.015,
                "shortlist_target_size": 20,
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


def _update_params_by_candidate_id(connection: SequenceConnection) -> dict[str, tuple[object, ...]]:
    return {params[5]: params for _, params in connection.cursor_instance.execute_calls[2:-1]}


def _run_update_params(connection: SequenceConnection) -> tuple[object, ...]:
    return connection.cursor_instance.execute_calls[-1][1]


def test_chosen_comp_prefers_clean_support_before_review_carry_forward(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-review-early",
            candidate_parcel_id="parcel-r1",
            final_selection_support_position=1,
            final_selection_support_status="review_selected_support",
            shortlist_position=1,
            shortlist_status="review_shortlisted",
            ranking_position=1,
            ranking_status="review_rankable",
            normalized_similarity_score=0.92,
            raw_similarity_score=92.0,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-clean-1",
            candidate_parcel_id="parcel-c1",
            final_selection_support_position=2,
            final_selection_support_status="selected_support",
            shortlist_position=2,
            shortlist_status="shortlisted",
            ranking_position=2,
            ranking_status="rankable",
            normalized_similarity_score=0.91,
            raw_similarity_score=91.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-clean-2",
            candidate_parcel_id="parcel-c2",
            final_selection_support_position=3,
            final_selection_support_status="selected_support",
            shortlist_position=3,
            shortlist_status="shortlisted",
            ranking_position=3,
            ranking_status="rankable",
            normalized_similarity_score=0.9,
            raw_similarity_score=90.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == 3
    assert result.review_chosen_comp_count == 1
    assert result.final_comp_count_status == "unsupported_below_minimum"
    updates = _update_params_by_candidate_id(connection)
    assert updates["cand-clean-1"][0] == 1
    assert updates["cand-clean-1"][1] == "chosen_comp"
    assert updates["cand-clean-2"][0] == 2
    review_candidate = updates["cand-review-early"]
    assert review_candidate[0] == 3
    assert review_candidate[1] == "review_chosen_comp"
    assert (
        review_candidate[4].obj["chosen_comp_context"][
            "clean_support_preference_reordered_from_support"
        ]
        is True
    )


def test_chosen_comp_uses_review_only_when_clean_support_is_insufficient(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-clean-{index}",
            candidate_parcel_id=f"parcel-clean-{index}",
            address=f"{100 + index} OAK ST",
            final_selection_support_position=index,
            final_selection_support_status="selected_support",
            shortlist_position=index,
            shortlist_status="shortlisted",
            ranking_position=index,
            ranking_status="rankable",
            normalized_similarity_score=0.99 - (index * 0.01),
            raw_similarity_score=99.0 - float(index),
        )
        for index in range(1, FINAL_COMP_PREFERRED_TARGET_MIN)
    ]
    candidates.extend(
        [
            _candidate(
                unequal_roll_candidate_id="cand-review-1",
                candidate_parcel_id="parcel-review-1",
                address="999 ELM ST",
                final_selection_support_position=20,
                final_selection_support_status="review_selected_support",
                shortlist_position=20,
                shortlist_status="review_shortlisted",
                ranking_position=20,
                ranking_status="review_rankable",
                normalized_similarity_score=0.8,
                raw_similarity_score=80.0,
                eligibility_status="review",
                eligibility_reason_code="fallback_geography_used",
            ),
            _candidate(
                unequal_roll_candidate_id="cand-review-2",
                candidate_parcel_id="parcel-review-2",
                address="1000 ELM ST",
                final_selection_support_position=21,
                final_selection_support_status="review_selected_support",
                shortlist_position=21,
                shortlist_status="review_shortlisted",
                ranking_position=21,
                ranking_status="review_rankable",
                normalized_similarity_score=0.79,
                raw_similarity_score=79.0,
                eligibility_status="review",
                eligibility_reason_code="fallback_geography_used",
            ),
        ]
    )
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MIN + 1
    assert result.review_chosen_comp_count == 2
    assert result.final_comp_count_status == "preferred_range"
    updates = _update_params_by_candidate_id(connection)
    assert updates["cand-review-1"][1] == "review_chosen_comp"
    assert updates["cand-review-2"][1] == "review_chosen_comp"


def test_chosen_comp_does_not_use_review_when_clean_support_meets_preferred_minimum(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-clean-{index}",
            candidate_parcel_id=f"parcel-clean-{index}",
            address=f"{200 + index} MAPLE ST",
            final_selection_support_position=index,
            final_selection_support_status="selected_support",
            shortlist_position=index,
            shortlist_status="shortlisted",
            ranking_position=index,
            ranking_status="rankable",
            normalized_similarity_score=0.99 - (index * 0.01),
            raw_similarity_score=99.0 - float(index),
        )
        for index in range(1, FINAL_COMP_PREFERRED_TARGET_MIN + 1)
    ]
    candidates.append(
        _candidate(
            unequal_roll_candidate_id="cand-review-extra",
            candidate_parcel_id="parcel-review-extra",
            address="500 PINE ST",
            final_selection_support_position=30,
            final_selection_support_status="review_selected_support",
            shortlist_position=30,
            shortlist_status="review_shortlisted",
            ranking_position=30,
            ranking_status="review_rankable",
            normalized_similarity_score=0.8,
            raw_similarity_score=80.0,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        )
    )
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MIN
    assert result.review_chosen_comp_count == 0
    updates = _update_params_by_candidate_id(connection)
    review_candidate = updates["cand-review-extra"]
    assert review_candidate[1] == "not_chosen_comp"
    assert (
        review_candidate[4].obj["chosen_comp_context"]["exclusion_reason_code"]
        == "clean_support_preferred_before_review_carry_forward"
    )


def test_chosen_comp_uses_full_clean_support_range_without_midpoint_target(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-clean-{index}",
            candidate_parcel_id=f"parcel-clean-{index}",
            address=f"{300 + index} CEDAR ST",
            final_selection_support_position=index,
            final_selection_support_status="selected_support",
            shortlist_position=index,
            shortlist_status="shortlisted",
            ranking_position=index,
            ranking_status="rankable",
            normalized_similarity_score=0.995 - (index * 0.005),
            raw_similarity_score=99.5 - float(index),
        )
        for index in range(1, FINAL_COMP_PREFERRED_TARGET_MAX + 1)
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MAX
    assert result.clean_chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MAX
    assert result.review_chosen_comp_count == 0
    assert result.final_comp_count_status == "preferred_range"


def test_chosen_comp_stops_in_preferred_band_when_tail_quality_degrades(
    monkeypatch,
) -> None:
    candidates = []
    for index in range(1, FINAL_COMP_PREFERRED_TARGET_MAX + 1):
        score = 0.998 - (index * 0.003)
        candidates.append(
            _candidate(
                unequal_roll_candidate_id=f"cand-clean-{index}",
                candidate_parcel_id=f"parcel-clean-{index}",
                address=f"{400 + index} WALNUT ST",
                final_selection_support_position=index,
                final_selection_support_status="selected_support",
                shortlist_position=index,
                shortlist_status="shortlisted",
                ranking_position=index,
                ranking_status="rankable",
                normalized_similarity_score=score,
                raw_similarity_score=score * 100.0,
            )
        )
    candidates.extend(
        [
            _candidate(
                unequal_roll_candidate_id="cand-clean-19",
                candidate_parcel_id="parcel-clean-19",
                address="419 WALNUT ST",
                final_selection_support_position=19,
                final_selection_support_status="selected_support",
                shortlist_position=19,
                shortlist_status="shortlisted",
                ranking_position=19,
                ranking_status="rankable",
                normalized_similarity_score=0.89,
                raw_similarity_score=89.0,
            ),
            _candidate(
                unequal_roll_candidate_id="cand-clean-20",
                candidate_parcel_id="parcel-clean-20",
                address="420 WALNUT ST",
                final_selection_support_position=20,
                final_selection_support_status="selected_support",
                shortlist_position=20,
                shortlist_status="shortlisted",
                ranking_position=20,
                ranking_status="rankable",
                normalized_similarity_score=0.88,
                raw_similarity_score=88.0,
            ),
        ]
    )
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MAX
    assert result.clean_chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MAX
    assert result.review_chosen_comp_count == 0
    assert result.final_comp_count_status == "preferred_range"
    updates = _update_params_by_candidate_id(connection)
    assert updates["cand-clean-19"][1] == "not_chosen_comp"
    assert (
        updates["cand-clean-19"][4].obj["chosen_comp_context"][
            "acceptable_zone_exclusion_reason_code"
        ]
        == "acceptable_zone_tail_score_below_floor"
    )
    run_update = _run_update_params(connection)
    selection_log_json = run_update[3].obj
    assert selection_log_json["governance"]["acceptable_zone_admission"]["admitted_count"] == 0
    assert "acceptable_zone_tail_score_below_floor" in selection_log_json["governance"][
        "acceptable_zone_admission"
    ]["stop_reason_codes"]


def test_chosen_comp_can_extend_into_acceptable_zone_when_tail_quality_holds(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-clean-{index}",
            candidate_parcel_id=f"parcel-clean-{index}",
            address=f"{500 + index} SYCAMORE ST",
            final_selection_support_position=index,
            final_selection_support_status="selected_support",
            shortlist_position=index,
            shortlist_status="shortlisted",
            ranking_position=index,
            ranking_status="rankable",
            normalized_similarity_score=0.991 - (index * 0.001),
            raw_similarity_score=(0.991 - (index * 0.001)) * 100.0,
        )
        for index in range(1, FINAL_COMP_ACCEPTABLE_TARGET_MAX + 1)
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == FINAL_COMP_ACCEPTABLE_TARGET_MAX
    assert result.clean_chosen_comp_count == FINAL_COMP_ACCEPTABLE_TARGET_MAX
    assert result.review_chosen_comp_count == 0
    assert result.final_comp_count_status == "acceptable_range"
    assert result.selection_governance_status == "supported_with_warnings"
    updates = _update_params_by_candidate_id(connection)
    assert updates["cand-clean-19"][1] == "chosen_comp"
    assert (
        updates["cand-clean-19"][4].obj["chosen_comp_context"][
            "acceptable_zone_admitted_flag"
        ]
        is True
    )
    assert (
        updates["cand-clean-20"][4].obj["score_context"]["acceptable_zone_evaluation"][
            "tail_score_gap"
        ]
        <= 0.025
    )
    run_update = _run_update_params(connection)
    selection_log_json = run_update[3].obj
    assert selection_log_json["governance"]["acceptable_zone_admission"]["admitted_count"] == 2
    assert selection_log_json["count_policy"]["acceptable_zone_admission_rule"][
        "tail_score_gap_max"
    ] == 0.025
    assert selection_log_json["count_policy"]["acceptable_zone_admission_rule"][
        "core_mean_gap_max"
    ] == 0.02
    assert selection_log_json["selected_comps"][-1]["selection_reason_code"] == (
        "acceptable_zone_tail_admission"
    )


def test_chosen_comp_stops_in_preferred_band_when_tail_is_too_far_below_core_mean(
    monkeypatch,
) -> None:
    candidates = []
    for index in range(1, FINAL_COMP_PREFERRED_TARGET_MAX + 1):
        score = 0.99
        candidates.append(
            _candidate(
                unequal_roll_candidate_id=f"cand-clean-{index}",
                candidate_parcel_id=f"parcel-clean-{index}",
                address=f"{600 + index} CYPRESS ST",
                final_selection_support_position=index,
                final_selection_support_status="selected_support",
                shortlist_position=index,
                shortlist_status="shortlisted",
                ranking_position=index,
                ranking_status="rankable",
                normalized_similarity_score=score,
                raw_similarity_score=score * 100.0,
            )
        )
    candidates.extend(
        [
            _candidate(
                unequal_roll_candidate_id="cand-clean-19",
                candidate_parcel_id="parcel-clean-19",
                address="619 CYPRESS ST",
                final_selection_support_position=19,
                final_selection_support_status="selected_support",
                shortlist_position=19,
                shortlist_status="shortlisted",
                ranking_position=19,
                ranking_status="rankable",
                normalized_similarity_score=0.968,
                raw_similarity_score=96.8,
            ),
            _candidate(
                unequal_roll_candidate_id="cand-clean-20",
                candidate_parcel_id="parcel-clean-20",
                address="620 CYPRESS ST",
                final_selection_support_position=20,
                final_selection_support_status="selected_support",
                shortlist_position=20,
                shortlist_status="shortlisted",
                ranking_position=20,
                ranking_status="rankable",
                normalized_similarity_score=0.967,
                raw_similarity_score=96.7,
            ),
        ]
    )
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_count == FINAL_COMP_PREFERRED_TARGET_MAX
    assert result.final_comp_count_status == "preferred_range"
    updates = _update_params_by_candidate_id(connection)
    tail_detail = updates["cand-clean-19"][4].obj
    assert updates["cand-clean-19"][1] == "not_chosen_comp"
    assert (
        tail_detail["chosen_comp_context"]["acceptable_zone_exclusion_reason_code"]
        == "acceptable_zone_core_mean_gap_too_wide"
    )
    assert (
        tail_detail["score_context"]["acceptable_zone_evaluation"]["core_mean_gap"] > 0.02
    )
    run_update = _run_update_params(connection)
    selection_log_json = run_update[3].obj
    assert "acceptable_zone_core_mean_gap_too_wide" in selection_log_json["governance"][
        "acceptable_zone_admission"
    ]["stop_reason_codes"]


def test_chosen_comp_hard_gates_excluded_states(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-chosen",
            candidate_parcel_id=str(uuid4()),
            final_selection_support_position=1,
            final_selection_support_status="selected_support",
            shortlist_position=1,
            shortlist_status="shortlisted",
            ranking_position=1,
            ranking_status="rankable",
            normalized_similarity_score=0.95,
            raw_similarity_score=95.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-shortlist-gated",
            candidate_parcel_id=str(uuid4()),
            final_selection_support_position=None,
            final_selection_support_status="excluded_from_selection_support",
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
            unequal_roll_candidate_id="cand-support-cutoff",
            candidate_parcel_id=str(uuid4()),
            final_selection_support_position=None,
            final_selection_support_status="not_selected_support",
            shortlist_position=21,
            shortlist_status="not_shortlisted",
            ranking_position=21,
            ranking_status="review_rankable",
            normalized_similarity_score=0.71,
            raw_similarity_score=71.0,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        ),
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.chosen_comp_eligible_count == 1
    assert result.chosen_comp_count == 1
    assert result.excluded_from_chosen_comp_count == 2
    updates = _update_params_by_candidate_id(connection)
    assert updates["cand-chosen"][1] == "chosen_comp"
    assert (
        updates["cand-shortlist-gated"][4].obj["chosen_comp_context"]["exclusion_reason_code"]
        == "ranking_gate_excluded"
    )
    assert (
        updates["cand-support-cutoff"][4].obj["chosen_comp_context"]["exclusion_reason_code"]
        == "final_selection_support_cutoff_not_met"
    )


def test_chosen_comp_count_policy_classifies_preferred_acceptable_auto_minimum_and_manual_review(
    monkeypatch,
) -> None:
    candidate_sets = [
        (
            "preferred_range",
            FINAL_COMP_PREFERRED_TARGET_MIN,
            "preferred_range",
        ),
        (
            "acceptable_range",
            FINAL_COMP_ACCEPTABLE_TARGET_MIN,
            "acceptable_range",
        ),
        (
            "auto_supported_minimum",
            FINAL_COMP_AUTO_SUPPORTED_MINIMUM,
            "auto_supported_minimum",
        ),
        (
            "manual_review_exception_range",
            FINAL_COMP_MANUAL_REVIEW_MIN,
            "manual_review_exception_range",
        ),
    ]

    for label, clean_count, expected_status in candidate_sets:
        candidates = [
            _candidate(
                unequal_roll_candidate_id=f"{label}-cand-{index}",
                candidate_parcel_id=f"{label}-parcel-{index}",
                address=f"{100 + index} {label} ST",
                final_selection_support_position=index,
                final_selection_support_status="selected_support",
                shortlist_position=index,
                shortlist_status="shortlisted",
                ranking_position=index,
                ranking_status="rankable",
                normalized_similarity_score=0.99 - (index * 0.01),
                raw_similarity_score=99.0 - float(index),
            )
            for index in range(1, clean_count + 1)
        ]
        connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
        monkeypatch.setattr(
            "app.services.unequal_roll_candidate_chosen_comp.get_connection",
            connection_factory(connection),
        )

        result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
            unequal_roll_run_id="run-1"
        )

        assert result.final_comp_count_status == expected_status


def test_chosen_comp_below_six_is_unsupported(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-{index}",
            candidate_parcel_id=f"parcel-{index}",
            final_selection_support_position=index,
            final_selection_support_status="selected_support",
            shortlist_position=index,
            shortlist_status="shortlisted",
            ranking_position=index,
            ranking_status="rankable",
            normalized_similarity_score=0.99 - (index * 0.01),
            raw_similarity_score=99.0 - float(index),
        )
        for index in range(1, FINAL_COMP_MANUAL_REVIEW_MIN)
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.final_comp_count_status == "unsupported_below_minimum"
    assert result.selection_governance_status == "unsupported"


def test_chosen_comp_persists_selection_log_and_diversity_warnings(monkeypatch) -> None:
    candidates = []
    for index in range(1, 16):
        candidates.append(
            _candidate(
                unequal_roll_candidate_id=f"cand-{index}",
                candidate_parcel_id=f"parcel-{index}",
                address=f"{100 + index} MAIN ST",
                neighborhood_code="N1",
                subdivision_name="SUB1",
                final_selection_support_position=index,
                final_selection_support_status="selected_support",
                shortlist_position=index,
                shortlist_status="shortlisted",
                ranking_position=index,
                ranking_status="rankable",
                normalized_similarity_score=0.99 - (index * 0.01),
                raw_similarity_score=99.0 - float(index),
            )
        )
    candidates.extend(
        [
            _candidate(
                unequal_roll_candidate_id="cand-16",
                candidate_parcel_id="parcel-16",
                address="900 OAK ST",
                neighborhood_code="N1",
                subdivision_name="SUB1",
                final_selection_support_position=16,
                final_selection_support_status="selected_support",
                shortlist_position=16,
                shortlist_status="shortlisted",
                ranking_position=16,
                ranking_status="rankable",
                normalized_similarity_score=0.70,
                raw_similarity_score=70.0,
            ),
            _candidate(
                unequal_roll_candidate_id="cand-17",
                candidate_parcel_id="parcel-17",
                address="901 PINE ST",
                neighborhood_code="N1",
                subdivision_name="SUB1",
                final_selection_support_position=17,
                final_selection_support_status="selected_support",
                shortlist_position=17,
                shortlist_status="shortlisted",
                ranking_position=17,
                ranking_status="rankable",
                normalized_similarity_score=0.69,
                raw_similarity_score=69.0,
            ),
            _candidate(
                unequal_roll_candidate_id="cand-18",
                candidate_parcel_id="parcel-18",
                address="902 ELM ST",
                neighborhood_code="N1",
                subdivision_name="SUB1",
                final_selection_support_position=18,
                final_selection_support_status="selected_support",
                shortlist_position=18,
                shortlist_status="shortlisted",
                ranking_position=18,
                ranking_status="rankable",
                normalized_similarity_score=0.68,
                raw_similarity_score=68.0,
            ),
            _candidate(
                unequal_roll_candidate_id="cand-excluded",
                candidate_parcel_id="parcel-excluded",
                address="999 FALLBACK ST",
                neighborhood_code="N2",
                subdivision_name="SUB2",
                final_selection_support_position=None,
                final_selection_support_status="excluded_from_selection_support",
                shortlist_position=None,
                shortlist_status="excluded_from_shortlist",
                ranking_position=None,
                ranking_status="excluded_from_ranking",
                normalized_similarity_score=0.67,
                raw_similarity_score=67.0,
                eligibility_status="excluded",
                eligibility_reason_code="living_area_out_of_bounds",
            ),
        ]
    )
    run_context = _run_context(
        summary_json={
            "candidate_discovery_summary": {
                "discovered_count": 100,
                "same_neighborhood_count": 40,
                "county_sfr_fallback_count": 60,
                "eligible_count": 12,
                "review_count": 0,
                "excluded_count": 88,
                "fallback_used": True,
                "same_neighborhood_insufficient_reason": "same_neighborhood_supply_below_preferred_pool",
            }
        }
    )
    connection = SequenceConnection(fetchall_results=[[run_context], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.final_comp_count_status == "preferred_range"
    assert result.selection_governance_status == "supported_with_warnings"
    run_update = _run_update_params(connection)
    assert run_update[0] == 18
    assert run_update[1] == "preferred_range"
    assert run_update[2] == "supported_with_warnings"
    selection_log_json = run_update[3].obj
    assert selection_log_json["selection_log_version"] == CHOSEN_COMP_VERSION
    assert selection_log_json["count_policy"]["preferred_final_comp_range"] == {
        "min": FINAL_COMP_PREFERRED_TARGET_MIN,
        "max": FINAL_COMP_PREFERRED_TARGET_MAX,
    }
    assert selection_log_json["count_policy"]["maximum_final_comp_count"] == (
        FINAL_COMP_ACCEPTABLE_TARGET_MAX
    )
    assert selection_log_json["count_policy"]["acceptable_zone_admission_rule"][
        "clean_support_only"
    ] is True
    assert selection_log_json["governance"]["fallback_justification"] == (
        "same_neighborhood_supply_below_preferred_pool"
    )
    assert "micro_street_concentration_warning" in selection_log_json["governance"][
        "concentration_warning_codes"
    ]
    assert selection_log_json["selected_comps"]
    assert selection_log_json["near_miss_comps"]


def test_chosen_comp_preserves_fort_bend_bathroom_boundary(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-fb-review",
            candidate_parcel_id=str(uuid4()),
            address="101 BAYOU ST",
            neighborhood_code="FB1",
            subdivision_name="FB-SUB",
            final_selection_support_position=1,
            final_selection_support_status="review_selected_support",
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
    run_context = _run_context(
        subject_neighborhood_code="FB1",
        subject_subdivision_name="FB-SUB",
    )
    connection = SequenceConnection(fetchall_results=[[run_context], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
        unequal_roll_run_id="run-1"
    )

    update_params = connection.cursor_instance.execute_calls[2][1]
    detail_json = update_params[4].obj
    assert detail_json["chosen_comp_status"] == "review_chosen_comp"
    assert detail_json["score_context"]["fort_bend_bathroom_modifier"]["review_required"] is True
    assert "full_baths_derived" not in detail_json["score_context"][
        "fort_bend_bathroom_modifier"
    ]


def test_chosen_comp_raises_when_run_has_no_support_candidates(monkeypatch) -> None:
    connection = SequenceConnection(fetchall_results=[[_run_context()], []])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_chosen_comp.get_connection",
        connection_factory(connection),
    )

    with pytest.raises(
        LookupError, match="Unequal-roll final-selection-support candidates not found"
    ):
        UnequalRollCandidateChosenCompService().build_chosen_comp_set_for_run(
            unequal_roll_run_id="run-empty"
        )
