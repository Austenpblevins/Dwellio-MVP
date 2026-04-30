from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

from app.services.unequal_roll_candidate_adjustment_math import (
    ADJUSTMENT_MATH_VERSION,
    UnequalRollCandidateAdjustmentMathService,
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
        "final_comp_count_status": "acceptable_range",
        "selection_governance_status": "supported_with_warnings",
        "selection_log_json": {
            "selection_log_version": "unequal_roll_chosen_comp_v5",
            "count_policy": {},
            "governance": {},
            "adjustment_scaffolding": {},
        },
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
    adjustment_support_status: str,
    adjustment_support_position: int | None,
    normalized_similarity_score: float = 0.96,
    appraised_value: float = 385000.0,
    full_baths: float | None = 2.0,
    half_baths: float | None = 1.0,
    effective_age: float | None = 10.0,
    quality_code: str | None = "2",
    condition_code: str | None = "1",
    pool_flag: bool = False,
    acceptable_zone_admitted_flag: bool = False,
    channel_overrides: dict[str, dict[str, object]] | None = None,
    valuation_bathroom_features: dict[str, object] | None = None,
) -> dict[str, object]:
    adjustment_channels = {
        "gla": {
            "readiness_status": "ready",
            "potential_adjustment_flag": True,
            "subject_value": 2400.0,
            "candidate_value": 2300.0,
        },
        "age_or_effective_age": {
            "readiness_status": "ready",
            "potential_adjustment_flag": True,
            "basis_field": "effective_age",
            "subject_value": 8.0,
            "candidate_value": effective_age,
        },
        "full_bath": {
            "readiness_status": "ready" if full_baths is not None else "review_required",
            "potential_adjustment_flag": full_baths is not None and full_baths != 3.0,
        },
        "half_bath": {
            "readiness_status": "ready",
            "potential_adjustment_flag": False,
        },
        "bedroom": {
            "readiness_status": "ready",
            "potential_adjustment_flag": False,
        },
        "story": {
            "readiness_status": "ready",
            "potential_adjustment_flag": False,
        },
        "quality": {
            "readiness_status": "ready" if quality_code is not None else "review_required",
            "potential_adjustment_flag": quality_code not in {None, "2"},
        },
        "condition": {
            "readiness_status": "ready" if condition_code is not None else "review_required",
            "potential_adjustment_flag": condition_code not in {None, "1"},
        },
        "pool": {
            "readiness_status": "ready",
            "potential_adjustment_flag": pool_flag is True,
        },
        "land_site": {
            "readiness_status": "ready",
            "potential_adjustment_flag": True,
        },
    }
    if channel_overrides:
        adjustment_channels.update(channel_overrides)
    return {
        "unequal_roll_candidate_id": unequal_roll_candidate_id,
        "unequal_roll_run_id": "run-1",
        "candidate_parcel_id": str(uuid4()),
        "county_id": "fort_bend",
        "tax_year": 2026,
        "living_area_sf": 2300.0,
        "year_built": 2012,
        "effective_age": effective_age,
        "bedrooms": 4,
        "full_baths": full_baths,
        "half_baths": half_baths,
        "stories": 2.0,
        "quality_code": quality_code,
        "condition_code": condition_code,
        "pool_flag": pool_flag,
        "land_sf": 7000.0,
        "land_acres": 0.16,
        "appraised_value": appraised_value,
        "chosen_comp_status": chosen_comp_status,
        "chosen_comp_position": adjustment_support_position,
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
                }
            },
        },
        "eligibility_reason_code": (
            "fallback_geography_used"
            if chosen_comp_status == "review_chosen_comp"
            else None
        ),
        "normalized_similarity_score": normalized_similarity_score,
        "adjustment_support_position": adjustment_support_position,
        "adjustment_support_status": adjustment_support_status,
        "adjustment_support_version": "unequal_roll_adjustment_support_v1",
        "adjustment_support_config_version": "unequal_roll_adjustment_support_v1",
        "adjustment_support_detail_json": {
            "adjustment_channels": adjustment_channels,
            "bathroom_boundary_context": {
                "canonical_fields": {
                    "full_baths": full_baths,
                    "half_baths": half_baths,
                },
                "valuation_bathroom_features": valuation_bathroom_features
                or {
                    "attachment_status": "attached",
                    "bathroom_count_status": "exact_supported",
                    "bathroom_count_confidence": "high",
                },
                "canonical_fields_replaced_by_valuation_only_features_flag": False,
            },
        },
        "candidate_snapshot_json": {
            "valuation_bathroom_features": valuation_bathroom_features
            or {
                "attachment_status": "attached",
                "bathroom_count_status": "exact_supported",
                "bathroom_count_confidence": "high",
            }
        },
    }


def _candidate_update_params_by_id(connection: SequenceConnection) -> dict[str, tuple[object, ...]]:
    updates: dict[str, tuple[object, ...]] = {}
    for query, params in connection.cursor_instance.execute_calls:
        if query.lstrip().startswith("UPDATE unequal_roll_candidates"):
            updates[str(params[10])] = params
    return updates


def _adjustment_insert_params(connection: SequenceConnection) -> list[tuple[object, ...]]:
    return [
        params
        for query, params in connection.cursor_instance.execute_calls
        if query.lstrip().startswith("INSERT INTO unequal_roll_adjustments")
    ]


def _run_update_params(connection: SequenceConnection) -> tuple[object, ...]:
    return [
        params
        for query, params in connection.cursor_instance.execute_calls
        if query.lstrip().startswith("UPDATE unequal_roll_runs")
    ][0]


def _dispersion_support_with_candidate_flags(
    candidate_flags: dict[str, dict[str, object]],
) -> dict[str, object]:
    return {
        "candidate_flags": candidate_flags,
        "run_summary": {
            "status": "evaluated_iqr_scaffold",
            "median_all": 400000.0,
            "median_minus_high_low": 398000.0,
            "trimmed_median_adjusted_value": 398000.0,
            "max_leave_one_out_delta": 7500.0,
            "median_absolute_deviation_adjusted_values": 12000.0,
            "adjusted_value_iqr": 24000.0,
            "raw_value_stats": {"median": 390000.0, "iqr": 18000.0},
            "raw_value_per_sf_stats": {"median": 165.0, "iqr": 6.0},
            "adjusted_value_stats": {"median": 400000.0, "iqr": 24000.0},
            "adjusted_value_per_sf_stats": {"median": 170.0, "iqr": 8.0},
            "raw_adjusted_divergence_summary": {
                "median_rank_shift": 2,
                "max_rank_shift": 2,
                "divergence_flag": any(
                    bool(flags.get("raw_adjusted_divergence_flag"))
                    for flags in candidate_flags.values()
                ),
            },
            "raw_value_outlier_count": 0,
            "raw_value_per_sf_outlier_count": 0,
            "adjusted_value_outlier_count": sum(
                1
                for flags in candidate_flags.values()
                if bool(flags.get("adjusted_value_outlier_flag"))
            ),
            "adjusted_value_per_sf_outlier_count": sum(
                1
                for flags in candidate_flags.values()
                if bool(flags.get("adjusted_value_per_sf_outlier_flag"))
            ),
            "adjusted_conflict_indicator_count": sum(
                1
                for flags in candidate_flags.values()
                if bool(flags.get("adjusted_conflict_indicator_flag"))
            ),
        },
    }


def test_adjustment_math_persists_line_items_and_candidate_summary(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-clean",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=1,
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.adjusted_count == 1
    inserts = _adjustment_insert_params(connection)
    assert len(inserts) == 10
    gla_insert = next(params for params in inserts if params[4] == "gla")
    assert gla_insert[5] == "subject_raw_value_per_sf_scaled_fallback"
    assert gla_insert[10] is not None
    updates = _candidate_update_params_by_id(connection)
    candidate_update = updates["cand-clean"]
    assert candidate_update[0] == "adjusted"
    assert candidate_update[1] == ADJUSTMENT_MATH_VERSION
    summary_json = candidate_update[3].obj
    assert summary_json["line_item_summary"]["line_item_count"] == 10
    assert summary_json["burden_summary"]["total_absolute_adjustment"] == candidate_update[6]
    assert (
        summary_json["source_governance"]["monetized_fallback_channel_count"] >= 1
    )
    assert summary_json["source_governance"]["unresolved_review_only_channel_count"] == 0
    assert summary_json["source_governance"]["source_governance_status"] == "fallback_only"
    assert (
        gla_insert[6].obj["source_precedence"]["label"]
        == "county_configured_fallback_schedule"
    )


def test_adjustment_math_persists_burden_totals_and_thresholds(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-heavy",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_limited",
            adjustment_support_position=1,
            full_baths=1.0,
            effective_age=20.0,
            quality_code="0",
            condition_code="-1",
            pool_flag=True,
            channel_overrides={
                "story": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": True,
                }
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.adjusted_limited_count == 1
    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-heavy"][3].obj
    assert updates["cand-heavy"][0] == "adjusted_limited"
    assert summary_json["burden_summary"]["total_absolute_adjustment"] > 0
    assert summary_json["burden_summary"]["material_adjustment_count"] >= 1
    assert summary_json["burden_summary"]["burden_status"] in {
        "review_threshold_exceeded",
        "exclude_threshold_exceeded",
        "within_thresholds",
    }
    assert summary_json["burden_governance"]["status"] in {
        "warning",
        "manual_review_recommended",
        "exclude_recommended",
        "within_thresholds",
    }


def test_adjustment_math_persists_dispersion_scaffolding(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-1",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=1,
            appraised_value=360000.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-2",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=2,
            appraised_value=420000.0,
        ),
        _candidate(
            unequal_roll_candidate_id="cand-3",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=3,
            appraised_value=780000.0,
        ),
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-1"][3].obj
    assert summary_json["dispersion_scaffolding"]["status"] == "evaluated_iqr_scaffold"
    run_update = _run_update_params(connection)
    selection_log_json = run_update[0].obj
    assert selection_log_json["adjustment_math"]["dispersion_scaffolding"]["status"] == (
        "evaluated_iqr_scaffold"
    )
    assert "raw_value_stats" in selection_log_json["adjustment_math"]["dispersion_scaffolding"]
    assert (
        "median_all"
        in selection_log_json["adjustment_math"]["dispersion_scaffolding"]
    )
    assert (
        "raw_adjusted_divergence_summary"
        in selection_log_json["adjustment_math"]["dispersion_scaffolding"]
    )


def test_adjustment_math_uses_warning_for_single_moderate_unresolved_channel(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-age-review",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_limited",
            adjustment_support_position=1,
            channel_overrides={
                "age_or_effective_age": {
                    "readiness_status": "review_required",
                    "potential_adjustment_flag": True,
                    "basis_field": "effective_age",
                    "subject_value": 8.0,
                    "candidate_value": None,
                }
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-age-review"][3].obj
    burden_governance = summary_json["burden_governance"]
    assert burden_governance["status"] == "warning"
    assert burden_governance["unresolved_channel_impact"] == "warning"
    assert burden_governance["reason_codes"] == ["limited_unresolved_channels_present"]
    assert burden_governance["unresolved_material_difference_channels"] == ["age"]
    assert burden_governance["unresolved_channel_severity_counts"] == {"moderate": 1}


def test_adjustment_math_uses_hybrid_source_for_exact_fort_bend_bathroom_basis(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath-exact",
            chosen_comp_status="review_chosen_comp",
            adjustment_support_status="adjustment_ready_with_review",
            adjustment_support_position=1,
            full_baths=None,
            valuation_bathroom_features={
                "attachment_status": "attached",
                "bathroom_count_status": "exact_supported",
                "bathroom_count_confidence": "high",
                "full_baths_derived": 2.0,
                "half_baths_derived": 1.0,
                "bathroom_equivalent_derived": 2.5,
            },
            channel_overrides={
                "full_bath": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": True,
                    "subject_value": 3.0,
                    "candidate_value": 2.0,
                    "difference_value": 1.0,
                    "adjustment_basis_status": "county_secondary_basis_supported",
                    "basis_source_code": "fort_bend_valuation_bathroom_features_exact",
                    "basis_source_reason_code": (
                        "canonical_candidate_missing_county_exact_support_used"
                    ),
                    "secondary_source_used_flag": True,
                    "canonical_candidate_missing_flag": True,
                    "valuation_support_basis_field": "full_baths_derived",
                    "valuation_support_basis_value": 2.0,
                    "valuation_support_basis_status": "exact_supported",
                    "valuation_support_basis_confidence": "high",
                }
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    inserts = _adjustment_insert_params(connection)
    full_bath_insert = next(params for params in inserts if params[4] == "full_bath")
    assert full_bath_insert[5] == "fort_bend_exact_bath_basis_with_fallback_rate"
    assert (
        full_bath_insert[6].obj["source_precedence"]["label"]
        == "county_supported_secondary_basis_with_fallback_rate"
    )
    assert full_bath_insert[6].obj["basis_source_support"]["secondary_source_used_flag"] is True
    assert full_bath_insert[10] is not None

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-bath-exact"][3].obj
    assert summary_json["source_governance"]["preferred_source_available_flag"] is True
    assert summary_json["source_governance"]["unresolved_review_only_channel_count"] == 0
    assert (
        summary_json["source_governance"]["source_governance_status"]
        == "hybrid_supported_with_fallback"
    )
    assert (
        summary_json["adjusted_set_governance"]["status"]
        == "usable_with_review_adjusted_comp"
    )
    assert (
        "hybrid_supported_source_requires_review_visibility"
        in summary_json["adjusted_set_governance"]["reason_codes"]
    )


def test_adjustment_math_keeps_severe_unresolved_channels_as_exclude_recommended(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-severe-review",
            chosen_comp_status="review_chosen_comp",
            adjustment_support_status="adjustment_limited_with_review",
            adjustment_support_position=1,
            full_baths=None,
            quality_code=None,
                channel_overrides={
                    "full_bath": {
                        "readiness_status": "review_required",
                        "potential_adjustment_flag": True,
                        "subject_value": 3.0,
                        "candidate_value": None,
                    },
                "quality": {
                    "readiness_status": "review_required",
                    "potential_adjustment_flag": True,
                    "subject_value": "2",
                    "candidate_value": None,
                },
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-severe-review"][3].obj
    burden_governance = summary_json["burden_governance"]
    assert burden_governance["status"] == "exclude_recommended"
    assert burden_governance["unresolved_channel_impact"] == "exclude_recommended"
    assert (
        "multiple_high_severity_unresolved_channels_present"
        in burden_governance["reason_codes"]
    )
    assert burden_governance["unresolved_material_difference_channels"] == [
        "full_bath",
        "quality",
    ]
    assert burden_governance["unresolved_channel_severity_counts"] == {"high": 2}
    assert (
        summary_json["adjusted_set_governance"]["status"]
        == "likely_exclude_adjusted_comp"
    )


def test_adjustment_math_marks_clean_fallback_comp_as_usable_adjusted_comp(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-usable-clean",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=1,
            channel_overrides={
                "gla": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                    "subject_value": 2400.0,
                    "candidate_value": 2390.0,
                },
                "age_or_effective_age": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                    "basis_field": "effective_age",
                    "subject_value": 8.0,
                    "candidate_value": 8.0,
                },
                "full_bath": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                    "subject_value": 3.0,
                    "candidate_value": 3.0,
                    "difference_value": 0.0,
                    "basis_source_code": "canonical_roll",
                },
                "land_site": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                },
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-usable-clean"][3].obj
    assert summary_json["adjusted_set_governance"]["status"] == "usable_adjusted_comp"
    assert summary_json["adjusted_set_governance"]["reason_codes"] == []


def test_adjustment_math_keeps_low_burden_clean_divergence_case_usable_without_review(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-clean-divergence",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=1,
            channel_overrides={
                "gla": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": True,
                    "subject_value": 2400.0,
                    "candidate_value": 2385.0,
                },
                "age_or_effective_age": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                    "basis_field": "effective_age",
                    "subject_value": 8.0,
                    "candidate_value": 8.0,
                },
                "full_bath": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                    "subject_value": 3.0,
                    "candidate_value": 3.0,
                },
                "land_site": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                },
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )
    monkeypatch.setattr(
        UnequalRollCandidateAdjustmentMathService,
        "_build_dispersion_support",
        lambda self, adjusted_candidates: _dispersion_support_with_candidate_flags(
            {
                "cand-clean-divergence": {
                    "raw_appraised_value": 385000.0,
                    "raw_appraised_value_per_sf": 167.39,
                    "adjusted_appraised_value": 389537.5,
                    "adjusted_appraised_value_per_sf": 169.36,
                    "raw_value_outlier_flag": False,
                    "raw_value_per_sf_outlier_flag": False,
                    "adjusted_value_outlier_flag": False,
                    "adjusted_value_per_sf_outlier_flag": False,
                    "raw_adjusted_rank_shift": 2,
                    "raw_adjusted_divergence_flag": True,
                    "adjusted_conflict_indicator_flag": True,
                }
            }
        ),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-clean-divergence"][3].obj
    assert summary_json["adjusted_set_governance"]["status"] == "usable_adjusted_comp"
    assert summary_json["adjusted_set_governance"]["reason_codes"] == []
    assert (
        summary_json["adjusted_set_governance"]["conflict_divergence_governance"][
            "mild_divergence_only_flag"
        ]
        is True
    )
    assert (
        summary_json["adjusted_set_governance"]["conflict_divergence_governance"][
            "divergence_requires_review_flag"
        ]
        is False
    )


def test_adjustment_math_keeps_unresolved_review_only_bath_case_weaker_than_hybrid_supported(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath-review-only",
            chosen_comp_status="review_chosen_comp",
            adjustment_support_status="adjustment_ready_with_review",
            adjustment_support_position=1,
            full_baths=None,
            valuation_bathroom_features={
                "attachment_status": "attached",
                "bathroom_count_status": "reconciled_fractional_plumbing",
                "bathroom_count_confidence": "medium",
                "bathroom_equivalent_derived": 2.75,
            },
            channel_overrides={
                "full_bath": {
                    "readiness_status": "review_required",
                    "potential_adjustment_flag": True,
                    "subject_value": 3.0,
                    "candidate_value": None,
                }
            },
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-bath-review-only"][3].obj
    assert summary_json["source_governance"]["unresolved_review_only_channel_count"] >= 1
    assert (
        summary_json["adjusted_set_governance"]["status"]
        == "likely_exclude_adjusted_comp"
    )
    assert (
        "burden_governance_exclude_recommended"
        in summary_json["adjusted_set_governance"]["reason_codes"]
    )
    run_update = _run_update_params(connection)
    selection_log_json = run_update[0].obj
    assert (
        selection_log_json["adjustment_math"]["adjusted_set_governance"][
            "likely_exclude_adjusted_comp_count"
        ]
        == 1
    )


def test_adjustment_math_keeps_low_burden_unresolved_review_only_case_usable_with_review(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath-mild-review",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=1,
            full_baths=None,
            channel_overrides={
                "gla": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": True,
                    "subject_value": 2400.0,
                    "candidate_value": 2380.0,
                },
                "full_bath": {
                    "readiness_status": "review_required",
                    "potential_adjustment_flag": False,
                    "subject_value": 3.0,
                    "candidate_value": None,
                },
                "land_site": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                },
            },
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
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )
    monkeypatch.setattr(
        UnequalRollCandidateAdjustmentMathService,
        "_build_dispersion_support",
        lambda self, adjusted_candidates: _dispersion_support_with_candidate_flags(
            {
                "cand-bath-mild-review": {
                    "raw_appraised_value": 385000.0,
                    "raw_appraised_value_per_sf": 167.39,
                    "adjusted_appraised_value": 389537.5,
                    "adjusted_appraised_value_per_sf": 169.36,
                    "raw_value_outlier_flag": False,
                    "raw_value_per_sf_outlier_flag": False,
                    "adjusted_value_outlier_flag": False,
                    "adjusted_value_per_sf_outlier_flag": False,
                    "raw_adjusted_rank_shift": 2,
                    "raw_adjusted_divergence_flag": True,
                    "adjusted_conflict_indicator_flag": True,
                }
            }
        ),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-bath-mild-review"][3].obj
    assert (
        summary_json["adjusted_set_governance"]["status"]
        == "usable_with_review_adjusted_comp"
    )
    assert (
        "unresolved_review_only_channels_present"
        in summary_json["adjusted_set_governance"]["reason_codes"]
    )
    assert (
        "unresolved_review_only_with_adjusted_conflict_indicator"
        not in summary_json["adjusted_set_governance"]["reason_codes"]
    )
    assert (
        summary_json["adjusted_set_governance"]["conflict_divergence_governance"][
            "unresolved_review_only_conflict_escalation_flag"
        ]
        is False
    )


def test_adjustment_math_keeps_unresolved_review_only_outlier_case_review_heavy(
    monkeypatch,
) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath-strong-review",
            chosen_comp_status="chosen_comp",
            adjustment_support_status="adjustment_ready",
            adjustment_support_position=1,
            full_baths=None,
            channel_overrides={
                "gla": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": True,
                    "subject_value": 2400.0,
                    "candidate_value": 2380.0,
                },
                "full_bath": {
                    "readiness_status": "review_required",
                    "potential_adjustment_flag": False,
                    "subject_value": 3.0,
                    "candidate_value": None,
                },
                "land_site": {
                    "readiness_status": "ready",
                    "potential_adjustment_flag": False,
                },
            },
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
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )
    monkeypatch.setattr(
        UnequalRollCandidateAdjustmentMathService,
        "_build_dispersion_support",
        lambda self, adjusted_candidates: _dispersion_support_with_candidate_flags(
            {
                "cand-bath-strong-review": {
                    "raw_appraised_value": 385000.0,
                    "raw_appraised_value_per_sf": 167.39,
                    "adjusted_appraised_value": 418000.0,
                    "adjusted_appraised_value_per_sf": 181.74,
                    "raw_value_outlier_flag": False,
                    "raw_value_per_sf_outlier_flag": False,
                    "adjusted_value_outlier_flag": True,
                    "adjusted_value_per_sf_outlier_flag": False,
                    "raw_adjusted_rank_shift": 3,
                    "raw_adjusted_divergence_flag": True,
                    "adjusted_conflict_indicator_flag": True,
                }
            }
        ),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-bath-strong-review"][3].obj
    assert (
        summary_json["adjusted_set_governance"]["status"]
        == "review_heavy_adjusted_comp"
    )
    assert (
        "unresolved_review_only_with_adjusted_conflict_indicator"
        in summary_json["adjusted_set_governance"]["reason_codes"]
    )
    assert (
        summary_json["adjusted_set_governance"]["conflict_divergence_governance"][
            "adjusted_outlier_conflict_flag"
        ]
        is True
    )


def test_adjustment_math_marks_manual_review_burden_case_as_review_heavy(
    monkeypatch,
) -> None:
    candidate = _candidate(
        unequal_roll_candidate_id="cand-review-heavy",
        chosen_comp_status="chosen_comp",
        adjustment_support_status="adjustment_ready",
        adjustment_support_position=1,
        full_baths=2.0,
        quality_code="1",
        pool_flag=True,
        channel_overrides={
            "gla": {
                "readiness_status": "ready",
                "potential_adjustment_flag": True,
                "subject_value": 2400.0,
                "candidate_value": 2300.0,
            },
            "age_or_effective_age": {
                "readiness_status": "ready",
                "potential_adjustment_flag": False,
                "basis_field": "effective_age",
                "subject_value": 8.0,
                "candidate_value": 8.0,
            },
            "full_bath": {
                "readiness_status": "ready",
                "potential_adjustment_flag": True,
                "subject_value": 3.0,
                "candidate_value": 2.0,
            },
            "story": {
                "readiness_status": "ready",
                "potential_adjustment_flag": True,
            },
            "condition": {
                "readiness_status": "ready",
                "potential_adjustment_flag": False,
            },
        },
    )
    candidate["stories"] = 1.0
    candidates = [candidate]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-review-heavy"][3].obj
    assert summary_json["burden_summary"]["material_adjustment_count"] == 5
    assert summary_json["burden_governance"]["status"] == "manual_review_recommended"
    assert (
        summary_json["adjusted_set_governance"]["status"]
        == "review_heavy_adjusted_comp"
    )
    assert (
        "burden_governance_manual_review_recommended"
        in summary_json["adjusted_set_governance"]["reason_codes"]
    )


def test_adjustment_math_preserves_review_and_acceptable_zone_governance(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-review-tail",
            chosen_comp_status="review_chosen_comp",
            adjustment_support_status="adjustment_ready_with_review",
            adjustment_support_position=19,
            acceptable_zone_admitted_flag=True,
        )
    ]
    connection = SequenceConnection(fetchall_results=[[_run_context()], candidates])
    monkeypatch.setattr(
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.review_adjusted_count == 1
    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-review-tail"][3].obj
    assert updates["cand-review-tail"][0] == "adjusted_with_review"
    assert summary_json["review_carry_forward_flag"] is True
    assert (
        summary_json["acceptable_zone_governance"]["acceptable_zone_admitted_flag"] is True
    )
    run_update = _run_update_params(connection)
    selection_log_json = run_update[0].obj
    assert selection_log_json["adjustment_math"]["review_carry_forward_count"] == 1


def test_adjustment_math_preserves_fort_bend_bathroom_boundary_context(monkeypatch) -> None:
    candidates = [
        _candidate(
            unequal_roll_candidate_id="cand-bath",
            chosen_comp_status="review_chosen_comp",
            adjustment_support_status="adjustment_limited_with_review",
            adjustment_support_position=1,
            full_baths=None,
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
        "app.services.unequal_roll_candidate_adjustment_math.get_connection",
        connection_factory(connection),
    )

    result = UnequalRollCandidateAdjustmentMathService().build_adjustment_math_for_run(
        unequal_roll_run_id="run-1"
    )

    assert result.review_adjusted_limited_count == 1
    inserts = _adjustment_insert_params(connection)
    full_bath_insert = next(params for params in inserts if params[4] == "full_bath")
    assert full_bath_insert[11] == "scaffold_review"
    assert (
        full_bath_insert[6].obj["source_precedence"]["quality_tier"]
        == "unresolved_review_only"
    )
    updates = _candidate_update_params_by_id(connection)
    summary_json = updates["cand-bath"][3].obj
    assert (
        summary_json["bathroom_boundary_context"][
            "canonical_fields_replaced_by_valuation_only_features_flag"
        ]
        is False
    )
    assert (
        summary_json["source_governance"]["unresolved_review_only_channel_count"] >= 1
    )
