from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.services.unequal_roll_candidate_adjustment_support import (
    ADJUSTMENT_SUPPORT_VERSION,
    UnequalRollCandidateAdjustmentSupportService,
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


def _run_context() -> dict[str, object]:
    return {
        "unequal_roll_run_id": "run-1",
        "final_comp_count_status": "preferred_range",
        "selection_governance_status": "supported_with_warnings",
        "selection_log_json": {
            "selection_log_version": "unequal_roll_chosen_comp_v5",
            "count_policy": {},
            "governance": {},
        },
        "subject_parcel_id": str(uuid4()),
        "county_id": "fort_bend",
        "tax_year": 2026,
        "living_area_sf": 2400.0,
        "year_built": 2010,
        "effective_age": 8.0,
        "bedrooms": 4,
        "full_baths": 3.0,
        "half_baths": 1.0,
        "stories": 2.0,
        "quality_code": "2",
        "condition_code": "1",
        "pool_flag": False,
        "land_sf": 7200.0,
        "land_acres": 0.165,
        "appraised_value": 410000.0,
    }


def _candidate(
    *,
    unequal_roll_candidate_id: str,
    chosen_comp_status: str,
    chosen_comp_position: int | None,
    final_selection_support_status: str = "selected_support",
    shortlist_status: str = "shortlisted",
    ranking_status: str = "rankable",
    eligibility_status: str = "eligible",
    eligibility_reason_code: str | None = None,
    normalized_similarity_score: float = 0.96,
    raw_similarity_score: float = 96.0,
    full_baths: float | None = 2.0,
    half_baths: float | None = 1.0,
    quality_code: str | None = "2",
    condition_code: str | None = "1",
    effective_age: float | None = 10.0,
    valuation_bathroom_features: dict[str, object] | None = None,
    acceptable_zone_admitted_flag: bool = False,
) -> dict[str, object]:
    return {
        "unequal_roll_candidate_id": unequal_roll_candidate_id,
        "unequal_roll_run_id": "run-1",
        "candidate_parcel_id": str(uuid4()),
        "county_id": "fort_bend",
        "tax_year": 2026,
        "address": "100 MAIN ST",
        "discovery_tier": "same_neighborhood",
        "living_area_sf": 2300.0,
        "year_built": 2012,
        "effective_age": effective_age,
        "bedrooms": 4,
        "full_baths": full_baths,
        "half_baths": half_baths,
        "stories": 2.0,
        "quality_code": quality_code,
        "condition_code": condition_code,
        "pool_flag": False,
        "land_sf": 7000.0,
        "land_acres": 0.16,
        "appraised_value": 385000.0,
        "source_provenance_json": {},
        "candidate_snapshot_json": {
            "valuation_bathroom_features": valuation_bathroom_features
            or {
                "attachment_status": "attached",
                "bathroom_count_status": "exact_supported",
                "bathroom_count_confidence": "high",
                "bathroom_equivalent_derived": 2.5,
            }
        },
        "chosen_comp_position": chosen_comp_position,
        "chosen_comp_status": chosen_comp_status,
        "chosen_comp_version": "unequal_roll_chosen_comp_v5",
        "chosen_comp_config_version": "unequal_roll_chosen_comp_v5",
        "chosen_comp_detail_json": {
            "chosen_comp_context": {
                "acceptable_zone_admitted_flag": acceptable_zone_admitted_flag,
                "acceptable_zone_candidate_flag": acceptable_zone_admitted_flag,
                "acceptable_zone_exclusion_reason_code": None,
            },
            "score_context": {
                "acceptable_zone_evaluation": {
                    "candidate_flag": acceptable_zone_admitted_flag,
                    "admitted_flag": acceptable_zone_admitted_flag,
                    "tail_reference_score": 0.95,
                    "candidate_score": normalized_similarity_score,
                    "tail_score_gap": 0.004,
                    "core_reference_mean_score": 0.972,
                    "core_mean_gap": 0.012,
                    "new_warning_codes": [],
                    "rule_version": "unequal_roll_chosen_comp_v5",
                }
            },
        },
        "final_selection_support_status": final_selection_support_status,
        "shortlist_status": shortlist_status,
        "ranking_status": ranking_status,
        "eligibility_status": eligibility_status,
        "eligibility_reason_code": eligibility_reason_code,
        "normalized_similarity_score": normalized_similarity_score,
        "raw_similarity_score": raw_similarity_score,
        "scoring_version": "unequal_roll_similarity_v1",
        "scoring_config_version": "unequal_roll_similarity_v1",
        "similarity_score_detail_json": {
            "fort_bend_bathroom_modifier": {
                "value": 1.0,
                "attachment_status": "attached",
                "review_required": False,
            }
        },
    }


def _candidate_updates_by_id(
    connection: SequenceConnection,
) -> dict[str, tuple[object, ...]]:
    return {params[5]: params for _, params in connection.cursor_instance.execute_calls[2:-1]}


def _run_update_params(connection: SequenceConnection) -> tuple[object, ...]:
    return connection.cursor_instance.execute_calls[-1][1]


def test_adjustment_support_preserves_clean_and_review_chosen_comp_distinction(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-clean",
            chosen_comp_status="chosen_comp",
            chosen_comp_position=1,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-review",
            chosen_comp_status="review_chosen_comp",
            chosen_comp_position=2,
            eligibility_status="review",
            eligibility_reason_code="fallback_geography_used",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-near-miss",
            chosen_comp_status="not_chosen_comp",
            chosen_comp_position=None,
            final_selection_support_status="not_selected_support",
            shortlist_status="shortlisted",
        ),
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentSupportService().build_adjustment_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.adjustment_ready_count == 1
    assert result.review_adjustment_ready_count == 1
    assert result.excluded_from_adjustment_support_count == 1
    updates = _candidate_updates_by_id(connection)
    assert updates["cand-clean"][1] == "adjustment_ready"
    assert updates["cand-review"][1] == "adjustment_ready_with_review"
    review_detail = updates["cand-review"][4].obj
    assert review_detail["governance_carry_forward"]["review_carry_forward_flag"] is True
    assert updates["cand-near-miss"][1] == "excluded_from_adjustment_support"


def test_adjustment_support_preserves_acceptable_zone_tail_governance_detail(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-tail",
            chosen_comp_status="chosen_comp",
            chosen_comp_position=19,
            acceptable_zone_admitted_flag=True,
            normalized_similarity_score=0.964,
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_support.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentSupportService().build_adjustment_support_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_updates_by_id(connection)
    detail = updates["cand-tail"][4].obj
    assert detail["governance_carry_forward"]["acceptable_zone_admitted_flag"] is True
    assert (
        detail["governance_carry_forward"]["acceptable_zone_evaluation"]["core_mean_gap"]
        == 0.012
    )
    run_update = _run_update_params(connection)
    selection_log_json = run_update[0].obj
    assert (
        selection_log_json["adjustment_scaffolding"]["acceptable_zone_tail_count"] == 1
    )


def test_adjustment_support_persists_burden_and_dispersion_scaffolding(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-limited",
            chosen_comp_status="chosen_comp",
            chosen_comp_position=1,
            effective_age=None,
            quality_code=None,
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentSupportService().build_adjustment_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.adjustment_limited_count == 1
    updates = _candidate_updates_by_id(connection)
    detail = updates["cand-limited"][4].obj
    assert updates["cand-limited"][1] == "adjustment_limited"
    assert detail["adjustment_readiness"]["missing_critical_channels"] == ["quality"]
    assert detail["adjustment_burden_scaffolding"]["status"] == "initialized_not_evaluated"
    assert detail["dispersion_scaffolding"]["status"] == "initialized_not_evaluated"
    run_update = _run_update_params(connection)
    selection_log_json = run_update[0].obj
    assert selection_log_json["adjustment_scaffolding"]["status_counts"][
        "adjustment_limited"
    ] == 1


def test_adjustment_support_preserves_fort_bend_bathroom_boundary_context(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath",
            chosen_comp_status="review_chosen_comp",
            chosen_comp_position=1,
            full_baths=None,
            eligibility_status="review",
            eligibility_reason_code="fort_bend_bathroom_count_review_required",
            valuation_bathroom_features={
                "attachment_status": "attached",
                "bathroom_count_status": "reconciled_fractional_plumbing",
                "bathroom_count_confidence": "medium",
                "bathroom_equivalent_derived": 2.75,
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentSupportService().build_adjustment_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.review_adjustment_ready_count == 1
    updates = _candidate_updates_by_id(connection)
    detail = updates["cand-bath"][4].obj
    assert updates["cand-bath"][1] == "adjustment_ready_with_review"
    assert (
        detail["bathroom_boundary_context"][
            "canonical_fields_replaced_by_valuation_only_features_flag"
        ]
        is False
    )
    assert (
        detail["adjustment_channels"]["full_bath"]["valuation_support_attachment_status"]
        == "attached"
    )
    assert detail["adjustment_channels"]["full_bath"]["readiness_status"] == "review_required"


def test_adjustment_support_uses_exact_fort_bend_bathroom_support_as_secondary_basis(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath-exact",
            chosen_comp_status="review_chosen_comp",
            chosen_comp_position=1,
            full_baths=None,
            eligibility_status="review",
            eligibility_reason_code="fort_bend_bathroom_count_review_required",
            valuation_bathroom_features={
                "attachment_status": "attached",
                "bathroom_count_status": "exact_supported",
                "bathroom_count_confidence": "high",
                "full_baths_derived": 2.0,
                "half_baths_derived": 1.0,
                "bathroom_equivalent_derived": 2.5,
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_support.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentSupportService().build_adjustment_support_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.review_adjustment_ready_count == 1
    updates = _candidate_updates_by_id(connection)
    detail = updates["cand-bath-exact"][4].obj
    full_bath_channel = detail["adjustment_channels"]["full_bath"]
    assert updates["cand-bath-exact"][1] == "adjustment_ready_with_review"
    assert full_bath_channel["readiness_status"] == "ready"
    assert full_bath_channel["basis_source_code"] == (
        "fort_bend_valuation_bathroom_features_exact"
    )
    assert full_bath_channel["secondary_source_used_flag"] is True
    assert full_bath_channel["valuation_support_auto_usable_flag"] is True
    assert full_bath_channel["valuation_support_basis_field"] == "full_baths_derived"
    assert full_bath_channel["candidate_value"] == 2.0
    assert (
        detail["bathroom_boundary_context"][
            "canonical_fields_replaced_by_valuation_only_features_flag"
        ]
        is False
    )
