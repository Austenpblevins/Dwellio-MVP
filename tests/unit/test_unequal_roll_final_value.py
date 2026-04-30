from __future__ import annotations

from contextlib import contextmanager

from app.services.unequal_roll_final_value import (
    FINAL_VALUE_VERSION,
    UnequalRollFinalValueService,
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
    selection_governance_status: str = "supported",
    support_status: str = "supported",
    final_comp_count_status: str = "preferred_range",
    fallback_used: bool = False,
    raw_sf_divergence_check_flag: bool = False,
    subject_appraised_value: float = 500000.0,
) -> dict[str, object]:
    return {
        "unequal_roll_run_id": "run-1",
        "support_status": support_status,
        "selection_governance_status": selection_governance_status,
        "final_comp_count_status": final_comp_count_status,
        "summary_json": {
            "candidate_discovery_summary": {
                "fallback_used": fallback_used,
            }
        },
        "selection_log_json": {
            "adjustment_math": {
                "dispersion_scaffolding": {
                    "raw_sf_divergence_check_flag": raw_sf_divergence_check_flag,
                }
            }
        },
        "county_id": "fort_bend",
        "tax_year": 2026,
        "appraised_value": subject_appraised_value,
        "living_area_sf": 2500.0,
        "full_baths": 3.0,
        "half_baths": 1.0,
    }


def _candidate(
    *,
    unequal_roll_candidate_id: str,
    adjusted_appraised_value: float,
    appraised_value: float,
    adjusted_set_status: str,
    chosen_comp_status: str = "chosen_comp",
    chosen_comp_position: int = 1,
    adjustment_math_status: str = "adjusted",
    burden_status: str = "within_thresholds",
    burden_reason_codes: list[str] | None = None,
    source_status: str = "fallback_only",
    adjusted_set_reason_codes: list[str] | None = None,
    review_carry_forward_flag: bool = False,
    acceptable_zone_admitted_flag: bool = False,
    hybrid_supported_source_flag: bool = False,
    unresolved_review_only_channel_count: int = 0,
    conflict_divergence_governance: dict[str, object] | None = None,
    bathroom_boundary_context: dict[str, object] | None = None,
    adjustment_pct_of_raw_value: float = 0.045,
    material_adjustment_count: int = 2,
    nontrivial_adjustment_sources_count: int = 2,
    dominant_adjustment_channel: str = "gla",
    address: str | None = None,
) -> dict[str, object]:
    return {
        "unequal_roll_candidate_id": unequal_roll_candidate_id,
        "unequal_roll_run_id": "run-1",
        "candidate_parcel_id": f"parcel-{unequal_roll_candidate_id}",
        "address": address or f"{unequal_roll_candidate_id.upper()} MAIN ST",
        "county_id": "fort_bend",
        "tax_year": 2026,
        "living_area_sf": 2400.0,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "appraised_value": appraised_value,
        "chosen_comp_status": chosen_comp_status,
        "chosen_comp_position": chosen_comp_position,
        "adjustment_math_status": adjustment_math_status,
        "adjusted_appraised_value": adjusted_appraised_value,
        "total_absolute_adjustment": round(abs(adjusted_appraised_value - appraised_value), 2),
        "adjustment_pct_of_raw_value": adjustment_pct_of_raw_value,
        "material_adjustment_count": material_adjustment_count,
        "nontrivial_adjustment_sources_count": nontrivial_adjustment_sources_count,
        "adjustment_summary_json": {
            "review_carry_forward_flag": review_carry_forward_flag,
            "acceptable_zone_governance": {
                "acceptable_zone_admitted_flag": acceptable_zone_admitted_flag,
                "acceptable_zone_candidate_flag": acceptable_zone_admitted_flag,
                "acceptable_zone_exclusion_reason_code": None,
                "acceptable_zone_evaluation": {
                    "candidate_flag": acceptable_zone_admitted_flag,
                    "admitted_flag": acceptable_zone_admitted_flag,
                    "core_mean_gap": 0.012,
                },
            },
            "source_governance": {
                "source_governance_status": source_status,
            },
            "burden_governance": {
                "status": burden_status,
                "reason_codes": burden_reason_codes or [],
            },
            "adjusted_set_governance": {
                "status": adjusted_set_status,
                "reason_codes": adjusted_set_reason_codes or [],
                "hybrid_supported_source_flag": hybrid_supported_source_flag,
                "unresolved_review_only_channel_count": (
                    unresolved_review_only_channel_count
                ),
                "conflict_divergence_governance": (
                    conflict_divergence_governance
                    or {
                        "mild_divergence_only_flag": False,
                        "adjusted_outlier_conflict_flag": False,
                    }
                ),
            },
            "adjustment_conflict_support": {
                "dominant_adjustment_channel": dominant_adjustment_channel,
            },
            "dispersion_scaffolding": {
                "status": "evaluated_iqr_scaffold",
            },
            "bathroom_boundary_context": bathroom_boundary_context
            or {
                "canonical_fields": {
                    "full_baths": 2.0,
                    "half_baths": 1.0,
                },
                "valuation_bathroom_features": {
                    "attachment_status": "attached",
                    "bathroom_count_status": "exact_supported",
                    "bathroom_count_confidence": "high",
                    "full_baths_derived": 2.0,
                },
                "canonical_fields_replaced_by_valuation_only_features_flag": False,
            },
        },
    }


def _adjustment_line(
    candidate_id: str,
    adjustment_type: str,
    amount: float | None,
) -> dict[str, object]:
    return {
        "unequal_roll_candidate_id": candidate_id,
        "adjustment_type": adjustment_type,
        "signed_adjustment_amount": amount,
    }


def _candidate_updates_by_id(
    connection: SequenceConnection,
) -> dict[str, tuple[object, ...]]:
    updates: dict[str, tuple[object, ...]] = {}
    for query, params in connection.cursor_instance.execute_calls:
        if query.lstrip().startswith("UPDATE unequal_roll_candidates"):
            updates[str(params[3])] = params
    return updates


def _run_update_params(connection: SequenceConnection) -> tuple[object, ...]:
    return [
        params
        for query, params in connection.cursor_instance.execute_calls
        if query.lstrip().startswith("UPDATE unequal_roll_runs")
    ][0]


def test_final_value_persists_median_and_governance_detail(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-1",
            adjusted_appraised_value=352000.0,
            appraised_value=342000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=1,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-2",
            adjusted_appraised_value=356000.0,
            appraised_value=346000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=2,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-3",
            adjusted_appraised_value=360000.0,
            appraised_value=350000.0,
            adjusted_set_status="usable_with_review_adjusted_comp",
            chosen_comp_position=3,
            chosen_comp_status="review_chosen_comp",
            review_carry_forward_flag=True,
            adjusted_set_reason_codes=["review_carry_forward_requires_review_visibility"],
        ),
        _candidate(
            unequal_roll_candidate_id="cand-4",
            adjusted_appraised_value=364000.0,
            appraised_value=359000.0,
            adjusted_set_status="usable_with_review_adjusted_comp",
            chosen_comp_position=4,
            acceptable_zone_admitted_flag=True,
            adjusted_set_reason_codes=["acceptable_zone_tail_requires_review_visibility"],
        ),
        _candidate(
            unequal_roll_candidate_id="cand-5",
            adjusted_appraised_value=368000.0,
            appraised_value=358000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=5,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-6",
            adjusted_appraised_value=372000.0,
            appraised_value=362000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=6,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-7",
            adjusted_appraised_value=376000.0,
            appraised_value=366000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=7,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-8",
            adjusted_appraised_value=380000.0,
            appraised_value=370000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=8,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-9",
            adjusted_appraised_value=430000.0,
            appraised_value=430000.0,
            adjusted_set_status="review_heavy_adjusted_comp",
            chosen_comp_position=9,
            burden_status="manual_review_recommended",
        ),
        _candidate(
            unequal_roll_candidate_id="cand-10",
            adjusted_appraised_value=495000.0,
            appraised_value=440000.0,
            adjusted_set_status="likely_exclude_adjusted_comp",
            chosen_comp_position=10,
            burden_status="exclude_recommended",
        ),
    ]
    adjustment_lines = [
        _adjustment_line("cand-1", "gla", 10000.0),
        _adjustment_line("cand-2", "gla", 10000.0),
        _adjustment_line("cand-3", "age", 10000.0),
        _adjustment_line("cand-4", "full_bath", 5000.0),
        _adjustment_line("cand-5", "gla", 10000.0),
        _adjustment_line("cand-6", "gla", 10000.0),
        _adjustment_line("cand-7", "gla", 10000.0),
        _adjustment_line("cand-8", "gla", 10000.0),
        _adjustment_line("cand-9", "condition", 30000.0),
        _adjustment_line("cand-10", "quality", 55000.0),
    ]
    connection = SequenceConnection(
        fetchall_results=[[_run_context()], candidates, adjustment_lines]
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_final_value.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollFinalValueService().build_final_value_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.final_value_status == "supported_with_review"
    assert result.requested_roll_value == 366000.0
    assert result.requested_reduction_amount == 134000.0
    assert result.included_count == 8
    assert result.included_with_review_count == 2
    assert result.excluded_review_heavy_count == 1
    assert result.excluded_likely_exclude_count == 1

    candidate_updates = _candidate_updates_by_id(connection)
    assert candidate_updates["cand-1"][1] == "included_in_final_value"
    assert candidate_updates["cand-3"][1] == "included_in_final_value_with_review"
    assert candidate_updates["cand-4"][0] == 4
    assert candidate_updates["cand-9"][1] == "excluded_review_heavy"
    assert candidate_updates["cand-10"][1] == "excluded_likely_exclude"

    candidate_detail = candidate_updates["cand-4"][2].obj
    assert candidate_detail["final_value_version"] == FINAL_VALUE_VERSION
    assert candidate_detail["final_value_position"] == 4
    assert (
        candidate_detail["governance_carry_forward"]["acceptable_zone_governance"][
            "acceptable_zone_admitted_flag"
        ]
        is True
    )
    assert (
        candidate_detail["bathroom_boundary_context"][
            "canonical_fields_replaced_by_valuation_only_features_flag"
        ]
        is False
    )

    run_update = _run_update_params(connection)
    assert run_update[0] == "supported_with_review"
    assert run_update[1] == FINAL_VALUE_VERSION
    assert run_update[3] == 366000.0
    final_value_detail = run_update[6].obj
    assert (
        final_value_detail["methodology_guardrails"][
            "final_requested_value_formula"
        ]
        == "median_of_adjusted_appraised_values"
    )
    assert final_value_detail["median_calculation"]["middle_value_lower"] == 364000.0
    assert final_value_detail["median_calculation"]["middle_value_upper"] == 368000.0
    assert final_value_detail["final_value_set_summary"]["included_count"] == 8
    assert (
        final_value_detail["final_value_set_summary"]["excluded_likely_exclude_count"]
        == 1
    )
    assert len(final_value_detail["included_comp_rows"]) == 8
    assert len(final_value_detail["excluded_comp_rows"]) == 2
    assert (
        final_value_detail["carried_forward_governance"]["adjustment_math"][
            "dispersion_scaffolding"
        ]["raw_sf_divergence_check_flag"]
        is False
    )
    selection_log_json = run_update[7].obj
    assert selection_log_json["final_value"]["requested_roll_value"] == 366000.0
    summary_json = run_update[8].obj
    assert summary_json["final_value_summary"]["requested_reduction_amount"] == 134000.0


def test_final_value_uses_manual_review_for_all_review_visible_minimum_set(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-{index}",
            adjusted_appraised_value=350000.0 + (index * 5000.0),
            appraised_value=340000.0 + (index * 5000.0),
            adjusted_set_status="usable_with_review_adjusted_comp",
            chosen_comp_status="review_chosen_comp",
            chosen_comp_position=index,
            review_carry_forward_flag=True,
            adjusted_set_reason_codes=["review_carry_forward_requires_review_visibility"],
        )
        for index in range(1, 7)
    ]
    adjustment_lines = [
        _adjustment_line(f"cand-{index}", "gla", 10000.0)
        for index in range(1, 7)
    ]
    connection = SequenceConnection(
        fetchall_results=[
            [_run_context(selection_governance_status="supported_with_warnings")],
            candidates,
            adjustment_lines,
        ]
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_final_value.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollFinalValueService().build_final_value_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.final_value_status == "manual_review_required"
    assert result.requested_roll_value == 367500.0
    assert result.included_count == 6

    run_update = _run_update_params(connection)
    final_value_detail = run_update[6].obj
    assert final_value_detail["qa_flags"]["all_included_review_visible_flag"] is True
    assert (
        final_value_detail["qa_flags"]["final_value_set_under_auto_supported_minimum_flag"]
        is True
    )
    assert (
        final_value_detail["final_value_set_summary"]["all_included_review_visible_flag"]
        is True
    )


def test_final_value_uses_supported_for_clean_auto_supported_set(monkeypatch) -> None:
    values = [
        362000.0,
        366000.0,
        370000.0,
        374000.0,
        378000.0,
        382000.0,
        386000.0,
        390000.0,
    ]
    candidates = [
        _candidate(
            unequal_roll_candidate_id=f"cand-{index}",
            adjusted_appraised_value=value,
            appraised_value=value - 8000.0,
            adjusted_set_status="usable_adjusted_comp",
            chosen_comp_position=index,
            adjustment_pct_of_raw_value=0.022,
            material_adjustment_count=1,
            nontrivial_adjustment_sources_count=1,
        )
        for index, value in enumerate(values, start=1)
    ]
    adjustment_lines = [
        _adjustment_line(f"cand-{index}", "gla", 8000.0)
        for index in range(1, 9)
    ]
    connection = SequenceConnection(
        fetchall_results=[[_run_context()], candidates, adjustment_lines]
    )
    monkeypatch.setattr(
        "app.services.unequal_roll_final_value.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollFinalValueService().build_final_value_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.final_value_status == "supported"
    assert result.requested_roll_value == 376000.0
    assert result.requested_reduction_amount == 124000.0
    assert result.requested_reduction_pct == 0.248

    run_update = _run_update_params(connection)
    final_value_detail = run_update[6].obj
    assert final_value_detail["qa_flags"]["all_included_review_visible_flag"] is False
    assert final_value_detail["stability_metrics"]["median_all"] == 376000.0
    assert final_value_detail["stability_metrics"]["average_adjustment_pct"] == 0.022
    assert final_value_detail["stability_metrics"]["dominant_adjustment_channel"] == "gla"
