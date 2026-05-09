from __future__ import annotations

from infra.scripts.run_unequal_roll_full_reranking_experiment import (
    EXPERIMENTAL_FULL_RERANKING,
    ExperimentalRerankingConfig,
    build_guardrail_summary,
    build_group_summary,
    build_model_backed_only_rows,
    build_neighborhood_stats,
    build_outlier_sensitivity_summary,
    build_payload,
    build_subject_comparison_row,
    build_value_interpretation_transition_counts,
    compute_experimental_rerank_score,
    estimate_adjustment_burden_ratio,
    summarize_replay_result,
    select_full_reranking_harvest,
    validate_selection_override,
)
from app.services.unequal_roll_no_persist_replay import UnequalRollReplayRequest
from app.services.unequal_roll_smart_harvest import SameNeighborhoodHarvestSelection


def _subject() -> dict[str, object]:
    return {
        "county_id": "harris",
        "appraised_value": 300000.0,
        "living_area_sf": 2000.0,
        "subdivision_name": "Oak",
        "land_sf": 7000.0,
        "bedrooms": 4,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "stories": 2.0,
        "pool_flag": False,
        "quality_code": "AVG",
        "condition_code": "AVG",
        "effective_age": 10.0,
    }


def _row(
    account: str,
    *,
    appraised_value: float = 300000.0,
    living_area_sf: float = 2000.0,
    subdivision_name: str = "Oak",
    land_sf: float = 7000.0,
    bedrooms: int = 4,
    full_baths: float = 2.0,
    half_baths: float = 1.0,
    stories: float = 2.0,
    pool_flag: bool = False,
    quality_code: str = "AVG",
    condition_code: str = "AVG",
    effective_age: float = 10.0,
) -> dict[str, object]:
    return {
        "parcel_id": f"p-{account}",
        "account_number": account,
        "appraised_value": appraised_value,
        "living_area_sf": living_area_sf,
        "subdivision_name": subdivision_name,
        "land_sf": land_sf,
        "land_acres": land_sf / 43560.0,
        "bedrooms": bedrooms,
        "full_baths": full_baths,
        "half_baths": half_baths,
        "stories": stories,
        "pool_flag": pool_flag,
        "quality_code": quality_code,
        "condition_code": condition_code,
        "effective_age": effective_age,
    }


def test_compute_experimental_rerank_score_applies_outlier_penalties() -> None:
    config = ExperimentalRerankingConfig()
    subject = _subject()
    universe = [
        _row("A", appraised_value=300000.0, living_area_sf=2000.0),
        _row("B", appraised_value=420000.0, living_area_sf=1800.0, subdivision_name="Other"),
    ]
    stats = build_neighborhood_stats(universe)
    close = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=universe[0],
        neighborhood_stats=stats,
        config=config,
    )
    risky = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=universe[1],
        neighborhood_stats=stats,
        config=config,
    )
    assert risky["experimental_score"] < close["experimental_score"]
    assert "subdivision_mismatch_penalty" in risky["trigger_labels"]


def test_select_full_reranking_harvest_uses_experimental_score_order() -> None:
    config = ExperimentalRerankingConfig(experiment_harvest_cap=2)
    subject = _subject()
    rows = [
        _row("HIGH", appraised_value=420000.0, living_area_sf=1800.0, subdivision_name="Other"),
        _row("MID", appraised_value=305000.0, living_area_sf=1990.0),
        _row("LOW", appraised_value=290000.0, living_area_sf=2010.0),
    ]
    selection, meta = select_full_reranking_harvest(
        subject_snapshot=subject,
        universe_rows=rows,
        config=config,
    )
    assert selection.strategy == EXPERIMENTAL_FULL_RERANKING
    assert [row["account_number"] for row in selection.selected_rows] == ["LOW", "MID"]
    assert meta["selected_signal_counts"]["subdivision_mismatch_penalty"] == 0


def test_validate_selection_override_rejects_wrong_county_tax_year_neighborhood_and_subject() -> None:
    subject_snapshot = {
        "county_id": "harris",
        "neighborhood_code": "222.02",
    }
    request = UnequalRollReplayRequest(
        county_id="harris",
        account_number="SUBJECT1",
        requested_tax_year=2026,
    )
    config = ExperimentalRerankingConfig(experiment_harvest_cap=2)
    selection = SameNeighborhoodHarvestSelection(
        strategy=EXPERIMENTAL_FULL_RERANKING,
        universe_count=1,
        selected_count=1,
        cap_used=2,
        excluded_by_cap=0,
        scored_universe=[],
        selected_rows=[
            {
                "parcel_id": "p1",
                "county_id": "fort_bend",
                "tax_year": 2026,
                "account_number": "A1",
                "neighborhood_code": "222.02",
            }
        ],
    )
    assert (
        validate_selection_override(
            subject_snapshot=subject_snapshot,
            request=request,
            selection=selection,
            config=config,
        )
        == "selection_override_county_mismatch"
    )
    selection = SameNeighborhoodHarvestSelection(
        strategy=EXPERIMENTAL_FULL_RERANKING,
        universe_count=1,
        selected_count=1,
        cap_used=2,
        excluded_by_cap=0,
        scored_universe=[],
        selected_rows=[
            {
                "parcel_id": "p1",
                "county_id": "harris",
                "tax_year": 2025,
                "account_number": "A1",
                "neighborhood_code": "222.02",
            }
        ],
    )
    assert validate_selection_override(subject_snapshot=subject_snapshot, request=request, selection=selection, config=config) == "selection_override_tax_year_mismatch"
    selection = SameNeighborhoodHarvestSelection(
        strategy=EXPERIMENTAL_FULL_RERANKING,
        universe_count=1,
        selected_count=1,
        cap_used=2,
        excluded_by_cap=0,
        scored_universe=[],
        selected_rows=[
            {
                "parcel_id": "p1",
                "county_id": "harris",
                "tax_year": 2026,
                "account_number": "A1",
                "neighborhood_code": "7137.00",
            }
        ],
    )
    assert validate_selection_override(subject_snapshot=subject_snapshot, request=request, selection=selection, config=config) == "selection_override_neighborhood_mismatch"
    selection = SameNeighborhoodHarvestSelection(
        strategy=EXPERIMENTAL_FULL_RERANKING,
        universe_count=1,
        selected_count=1,
        cap_used=2,
        excluded_by_cap=0,
        scored_universe=[],
        selected_rows=[
            {
                "parcel_id": "p1",
                "county_id": "harris",
                "tax_year": 2026,
                "account_number": "SUBJECT1",
                "neighborhood_code": "222.02",
            }
        ],
    )
    assert validate_selection_override(subject_snapshot=subject_snapshot, request=request, selection=selection, config=config) == "selection_override_includes_subject_account"


def test_estimate_adjustment_burden_ratio_is_null_safe() -> None:
    ratio = estimate_adjustment_burden_ratio(
        subject_snapshot={"appraised_value": None, "living_area_sf": None},
        row={},
    )
    assert ratio is None


def test_build_subject_comparison_row_is_null_safe_for_blocked_rerank() -> None:
    subject = type(
        "SelectedSubject",
        (),
        {
            "county_id": "harris",
            "account_number": "A1",
            "neighborhood_code": "222.02",
            "selection_source": "targeted",
        },
    )()
    row = build_subject_comparison_row(
        subject=subject,
        current_result={"replay_status": "completed"},
        smart_result={"replay_status": "completed"},
        reranked_result={"replay_status": "blocked", "blocker_code": "subject_not_found"},
        rerank_meta=None,
        lower_value_signal=None,
    )
    assert row["comparison_ready"] is False
    assert row["blocker_code"] == "subject_not_found"


def test_summarize_replay_result_prefers_safe_exposed_values_for_unsupported_status() -> None:
    summary = summarize_replay_result(
        {
            "replay_status": "completed",
            "support_status": "manual_review_required",
            "final_value_status": "unsupported",
            "requested_roll_value": 100000.0,
            "requested_reduction_amount": 50000.0,
            "requested_reduction_pct": 0.33,
            "safe_requested_roll_value": None,
            "safe_requested_reduction_amount": 0.0,
            "safe_requested_reduction_pct": 0.0,
            "value_interpretation": "no_reduction_when_unsupported",
            "included_comp_count": 0,
            "excluded_review_heavy_count": 0,
            "excluded_likely_exclude_count": 0,
            "final_value_detail_json": {"included_comp_rows": []},
        }
    )
    assert summary["requested_reduction_amount"] == 0.0
    assert summary["requested_reduction_pct"] == 0.0
    assert summary["value_interpretation"] == "no_reduction_when_unsupported"


def test_build_payload_discloses_true_full_reranking_and_guardrails() -> None:
    row = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "8309.06",
        "comparison_ready": True,
        "smart_vs_current_taxpayer_delta": 1000.0,
        "rerank_vs_current_taxpayer_delta": 2000.0,
        "rerank_vs_smart_taxpayer_delta": 1000.0,
        "rerank_vs_smart_similarity_delta": 0.01,
        "rerank_review_heavy_delta_vs_smart": 0,
        "rerank_likely_exclude_delta_vs_smart": 0,
        "rerank_support_status_drift_vs_smart": False,
        "rerank_final_status_drift_vs_smart": False,
        "smart_final_value_status": "supported_with_review",
        "rerank_final_value_status": "supported_with_review",
        "final_status_transition_smart_to_rerank": "supported_with_review -> supported_with_review",
        "current_value_interpretation": "final_model_value",
        "smart_value_interpretation": "final_model_value",
        "rerank_value_interpretation": "final_model_value",
        "lower_value_credible_alternative_signal": {"automation_status": "manual_review_only"},
    }
    payload = build_payload(
        selection_summary={
            "selection_mode": "targeted",
            "selected_subject_count": 1,
        },
        subject_rows=[row],
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert payload["guardrails"] == build_guardrail_summary()
    assert payload["input_contract"]["full_candidate_reranking"] is True
    assert payload["input_contract"]["post_selection_swap_only"] is False
    assert payload["model_backed_only_summary"]["comparison_ready_count"] == 1
    assert payload["attrition_summary"]["rerank_selected_top_100_count"] == 0
    assert payload["attrition_summary"]["rerank_final_included_comp_count"] == 0
    assert "final_model_value -> final_model_value" in payload["value_interpretation_transitions"]["smart_to_rerank"]
    assert payload["recommendation"] in {
        "keep_analysis_only_or_abandon",
        "refine_before_more_validation",
        "continue_bounded_validation_only",
    }


def test_build_group_summary_tracks_transition_counts() -> None:
    summary = build_group_summary(
        [
            {
                "comparison_ready": True,
                "rerank_vs_smart_taxpayer_delta": -1500.0,
                "smart_vs_current_taxpayer_delta": 2000.0,
                "rerank_vs_current_taxpayer_delta": 500.0,
                "rerank_vs_smart_similarity_delta": -0.01,
                "rerank_review_heavy_delta_vs_smart": 1,
                "rerank_likely_exclude_delta_vs_smart": 0,
                "rerank_support_status_drift_vs_smart": False,
                "rerank_final_status_drift_vs_smart": True,
                "smart_final_value_status": "supported_with_review",
                "rerank_final_value_status": "manual_review_required",
                "final_status_transition_smart_to_rerank": "supported_with_review -> manual_review_required",
                "lower_value_credible_alternative_signal": {"automation_status": "safe_automated_candidate"},
            }
        ],
        label="harris",
    )
    assert summary["material_loss_count"] == 1
    assert summary["final_status_transition_counts"] == {
        "supported_with_review -> manual_review_required": 1
    }
    assert summary["final_status_true_downgrade_count"] == 1
    assert summary["final_status_true_upgrade_count"] == 0
    assert summary["final_status_manual_or_unsupported_result_count"] == 1
    assert summary["safe_lower_value_signal_count"] == 1


def test_build_value_interpretation_transition_counts_and_outlier_sensitivity_summary() -> None:
    rows = [
        {
            "comparison_ready": True,
            "current_value_interpretation": "final_model_value",
            "smart_value_interpretation": "diagnostic_only",
            "rerank_value_interpretation": "final_model_value",
            "rerank_vs_smart_taxpayer_delta": 100.0,
        },
        {
            "comparison_ready": True,
            "current_value_interpretation": "final_model_value",
            "smart_value_interpretation": "final_model_value",
            "rerank_value_interpretation": "final_model_value",
            "rerank_vs_smart_taxpayer_delta": 50.0,
        },
        {
            "comparison_ready": True,
            "current_value_interpretation": "final_model_value",
            "smart_value_interpretation": "final_model_value",
            "rerank_value_interpretation": "final_model_value",
            "rerank_vs_smart_taxpayer_delta": -25.0,
        },
    ]
    transitions = build_value_interpretation_transition_counts(
        rows,
        source_key="smart_value_interpretation",
        target_key="rerank_value_interpretation",
    )
    assert transitions["diagnostic_only -> final_model_value"] == 1
    sensitivity = build_outlier_sensitivity_summary(rows)
    assert sensitivity["net_taxpayer_delta_rerank_vs_smart"] == 125.0
    assert sensitivity["net_excluding_top_1_gain"] == 25.0


def test_build_payload_recommendation_is_conservative_when_unsupported_drift_occurs() -> None:
    row = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "8309.06",
        "comparison_ready": True,
        "smart_vs_current_taxpayer_delta": 1000.0,
        "rerank_vs_current_taxpayer_delta": 2000.0,
        "rerank_vs_smart_taxpayer_delta": 1000.0,
        "rerank_vs_smart_similarity_delta": 0.01,
        "rerank_review_heavy_delta_vs_smart": 0,
        "rerank_likely_exclude_delta_vs_smart": 0,
        "rerank_support_status_drift_vs_smart": False,
        "rerank_final_status_drift_vs_smart": True,
        "smart_final_value_status": "manual_review_required",
        "rerank_final_value_status": "unsupported",
        "final_status_transition_smart_to_rerank": "manual_review_required -> unsupported",
        "current_value_interpretation": "final_model_value",
        "smart_value_interpretation": "final_model_value",
        "rerank_value_interpretation": "diagnostic_only",
        "lower_value_credible_alternative_signal": {"automation_status": "manual_review_only"},
    }
    payload = build_payload(
        selection_summary={"selection_mode": "targeted", "selected_subject_count": 1},
        subject_rows=[row],
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert payload["recommendation"] == "refine_before_more_validation"


def test_build_group_summary_separates_true_downgrade_count_from_broad_manual_or_unsupported_result_count() -> None:
    summary = build_group_summary(
        [
            {
                "comparison_ready": True,
                "rerank_vs_smart_taxpayer_delta": 1500.0,
                "smart_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_current_taxpayer_delta": 1500.0,
                "rerank_vs_smart_similarity_delta": 0.01,
                "rerank_review_heavy_delta_vs_smart": 0,
                "rerank_likely_exclude_delta_vs_smart": 0,
                "rerank_support_status_drift_vs_smart": False,
                "rerank_final_status_drift_vs_smart": False,
                "smart_final_value_status": "manual_review_required",
                "rerank_final_value_status": "manual_review_required",
                "final_status_transition_smart_to_rerank": "manual_review_required -> manual_review_required",
                "lower_value_credible_alternative_signal": {},
            },
            {
                "comparison_ready": True,
                "rerank_vs_smart_taxpayer_delta": 500.0,
                "smart_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_current_taxpayer_delta": 500.0,
                "rerank_vs_smart_similarity_delta": 0.0,
                "rerank_review_heavy_delta_vs_smart": 0,
                "rerank_likely_exclude_delta_vs_smart": 0,
                "rerank_support_status_drift_vs_smart": False,
                "rerank_final_status_drift_vs_smart": True,
                "smart_final_value_status": "supported_with_review",
                "rerank_final_value_status": "manual_review_required",
                "final_status_transition_smart_to_rerank": "supported_with_review -> manual_review_required",
                "lower_value_credible_alternative_signal": {},
            },
            {
                "comparison_ready": True,
                "rerank_vs_smart_taxpayer_delta": 500.0,
                "smart_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_current_taxpayer_delta": 500.0,
                "rerank_vs_smart_similarity_delta": 0.0,
                "rerank_review_heavy_delta_vs_smart": 0,
                "rerank_likely_exclude_delta_vs_smart": 0,
                "rerank_support_status_drift_vs_smart": False,
                "rerank_final_status_drift_vs_smart": True,
                "smart_final_value_status": "manual_review_required",
                "rerank_final_value_status": "supported_with_review",
                "final_status_transition_smart_to_rerank": "manual_review_required -> supported_with_review",
                "lower_value_credible_alternative_signal": {},
            },
        ],
        label="overall",
    )
    assert summary["final_status_true_downgrade_count"] == 1
    assert summary["final_status_true_upgrade_count"] == 1
    assert summary["final_status_manual_or_unsupported_result_count"] == 2


def test_build_payload_uses_clarified_attrition_names_and_model_backed_top_case_lists() -> None:
    rows = [
        {
            "county_id": "harris",
            "subject_account": "GAIN1",
            "neighborhood_code": "8309.06",
            "comparison_ready": True,
            "smart_vs_current_taxpayer_delta": 0.0,
            "rerank_vs_current_taxpayer_delta": 5000.0,
            "rerank_vs_smart_taxpayer_delta": 5000.0,
            "rerank_vs_smart_similarity_delta": 0.01,
            "rerank_review_heavy_delta_vs_smart": 0,
            "rerank_likely_exclude_delta_vs_smart": 0,
            "rerank_support_status_drift_vs_smart": False,
            "rerank_final_status_drift_vs_smart": False,
            "smart_final_value_status": "supported_with_review",
            "rerank_final_value_status": "supported_with_review",
            "final_status_transition_smart_to_rerank": "supported_with_review -> supported_with_review",
            "current_value_interpretation": "final_model_value",
            "smart_value_interpretation": "final_model_value",
            "rerank_value_interpretation": "final_model_value",
            "rerank_selected_top_100_count": 100,
            "rerank_final_included_comp_count": 8,
            "primary_explanation": "test_gain",
            "lower_value_credible_alternative_signal": {},
        },
        {
            "county_id": "harris",
            "subject_account": "LOSS1",
            "neighborhood_code": "8309.06",
            "comparison_ready": True,
            "smart_vs_current_taxpayer_delta": 0.0,
            "rerank_vs_current_taxpayer_delta": -500.0,
            "rerank_vs_smart_taxpayer_delta": -500.0,
            "rerank_vs_smart_similarity_delta": -0.01,
            "rerank_review_heavy_delta_vs_smart": 0,
            "rerank_likely_exclude_delta_vs_smart": 0,
            "rerank_support_status_drift_vs_smart": False,
            "rerank_final_status_drift_vs_smart": False,
            "smart_final_value_status": "manual_review_required",
            "rerank_final_value_status": "manual_review_required",
            "final_status_transition_smart_to_rerank": "manual_review_required -> manual_review_required",
            "current_value_interpretation": "final_model_value",
            "smart_value_interpretation": "final_model_value",
            "rerank_value_interpretation": "final_model_value",
            "rerank_selected_top_100_count": 100,
            "rerank_final_included_comp_count": 7,
            "primary_explanation": "test_loss",
            "lower_value_credible_alternative_signal": {},
        },
        {
            "county_id": "harris",
            "subject_account": "NONMODEL",
            "neighborhood_code": "8309.06",
            "comparison_ready": True,
            "smart_vs_current_taxpayer_delta": 0.0,
            "rerank_vs_current_taxpayer_delta": 9000.0,
            "rerank_vs_smart_taxpayer_delta": 9000.0,
            "rerank_vs_smart_similarity_delta": 0.0,
            "rerank_review_heavy_delta_vs_smart": 0,
            "rerank_likely_exclude_delta_vs_smart": 0,
            "rerank_support_status_drift_vs_smart": False,
            "rerank_final_status_drift_vs_smart": False,
            "smart_final_value_status": "unsupported",
            "rerank_final_value_status": "unsupported",
            "final_status_transition_smart_to_rerank": "unsupported -> unsupported",
            "current_value_interpretation": "final_model_value",
            "smart_value_interpretation": "diagnostic_only",
            "rerank_value_interpretation": "diagnostic_only",
            "rerank_selected_top_100_count": 100,
            "rerank_final_included_comp_count": 0,
            "primary_explanation": "non_model",
            "lower_value_credible_alternative_signal": {},
        },
    ]
    payload = build_payload(
        selection_summary={"selection_mode": "targeted", "selected_subject_count": 3},
        subject_rows=rows,
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert payload["attrition_summary"] == {
        "rerank_selected_top_100_count": 300,
        "rerank_final_included_comp_count": 15,
    }
    assert payload["model_backed_top_gains"][0]["account"] == "GAIN1"
    assert payload["model_backed_top_losses"][0]["account"] == "LOSS1"
    assert [row["subject_account"] for row in build_model_backed_only_rows(rows)] == ["GAIN1", "LOSS1"]
