from __future__ import annotations

from infra.scripts.run_unequal_roll_full_reranking_experiment import (
    EXPERIMENTAL_FULL_RERANKING,
    ExperimentalRerankingConfig,
    build_guardrail_summary,
    build_group_summary,
    build_chunk_comparability_summary,
    build_variant_complexity_summary,
    build_parser,
    compute_discovery_fetch_limit,
    finalize_neighborhood_candidates,
    build_model_backed_only_rows,
    build_neighborhood_stats,
    build_outlier_sensitivity_summary,
    build_payload,
    build_subject_cohort_fingerprint,
    apply_neighborhood_exclusions,
    build_segment_posture_table,
    build_variant_configurations,
    build_subject_comparison_row,
    build_value_interpretation_transition_counts,
    classify_segment_posture,
    compute_experimental_rerank_score,
    estimate_adjustment_burden_ratio,
    summarize_penalty_contribution,
    summarize_replay_result,
    select_variant_configurations,
    select_full_reranking_harvest,
    run_subject_experiment,
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


def test_apply_neighborhood_exclusions_preserves_order_and_removes_excluded_values() -> None:
    assert apply_neighborhood_exclusions(
        ["8309.06", "7137.00", "7068.04", "5902-00"],
        ["7137.00", "5902-00"],
    ) == ["8309.06", "7068.04"]


def test_finalize_neighborhood_candidates_backfills_after_exclusions() -> None:
    assert finalize_neighborhood_candidates(
        ["215.03", "222.02", "229.60", "790.00", "2215.00", "1347.00"],
        excluded_neighborhoods=["215.03", "222.02"],
        target_limit=4,
    ) == ["229.60", "790.00", "2215.00", "1347.00"]


def test_compute_discovery_fetch_limit_backfills_exclusions() -> None:
    assert compute_discovery_fetch_limit(target_limit=10, excluded_count=5) >= 15
    assert compute_discovery_fetch_limit(target_limit=10, excluded_count=0) >= 40


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
        variant=build_variant_configurations(ExperimentalRerankingConfig())["all_penalties"],
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
    variant_definitions = build_variant_configurations(ExperimentalRerankingConfig())
    row = {
        "variant_key": "all_penalties",
        "variant_label": "All Penalties",
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
        variant_definitions=variant_definitions,
        all_variant_keys=list(variant_definitions.keys()),
        executed_variant_keys=["all_penalties"],
        runtime_notes=["Runtime-aware partial matrix: only the requested variant subset was executed in this run."],
        variants=[variant_definitions["all_penalties"]],
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert payload["guardrails"] == build_guardrail_summary()
    assert payload["input_contract"]["full_candidate_reranking"] is True
    assert payload["input_contract"]["post_selection_swap_only"] is False
    assert payload["model_backed_only_summary"]["comparison_ready_count"] == 1
    assert payload["attrition_summary"]["rerank_selected_top_100_count"] == 0
    assert payload["attrition_summary"]["rerank_final_included_comp_count"] == 0
    assert payload["execution_matrix"]["matrix_status"] == "partial_matrix"
    assert "without_land_mismatch" in payload["execution_matrix"]["unexecuted_variant_keys"]
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
    assert summary["unsupported_result_count"] == 0
    assert summary["true_transition_to_unsupported_count"] == 0
    assert summary["unsupported_stays_unsupported_count"] == 0
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
    variant_definitions = build_variant_configurations(ExperimentalRerankingConfig())
    row = {
        "variant_key": "all_penalties",
        "variant_label": "All Penalties",
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
        variant_definitions=variant_definitions,
        all_variant_keys=list(variant_definitions.keys()),
        executed_variant_keys=["all_penalties"],
        runtime_notes=["Runtime-aware partial matrix: only the requested variant subset was executed in this run."],
        variants=[variant_definitions["all_penalties"]],
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


def test_build_group_summary_splits_unsupported_result_and_transition_counts() -> None:
    summary = build_group_summary(
        [
            {
                "comparison_ready": True,
                "rerank_vs_smart_taxpayer_delta": 0.0,
                "smart_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_smart_similarity_delta": 0.0,
                "rerank_review_heavy_delta_vs_smart": 0,
                "rerank_likely_exclude_delta_vs_smart": 0,
                "rerank_support_status_drift_vs_smart": False,
                "rerank_final_status_drift_vs_smart": True,
                "smart_final_value_status": "manual_review_required",
                "rerank_final_value_status": "unsupported",
                "final_status_transition_smart_to_rerank": "manual_review_required -> unsupported",
                "lower_value_credible_alternative_signal": {},
            },
            {
                "comparison_ready": True,
                "rerank_vs_smart_taxpayer_delta": 0.0,
                "smart_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_current_taxpayer_delta": 0.0,
                "rerank_vs_smart_similarity_delta": 0.0,
                "rerank_review_heavy_delta_vs_smart": 0,
                "rerank_likely_exclude_delta_vs_smart": 0,
                "rerank_support_status_drift_vs_smart": False,
                "rerank_final_status_drift_vs_smart": False,
                "smart_final_value_status": "unsupported",
                "rerank_final_value_status": "unsupported",
                "final_status_transition_smart_to_rerank": "unsupported -> unsupported",
                "lower_value_credible_alternative_signal": {},
            },
        ],
        label="overall",
    )
    assert summary["unsupported_result_count"] == 2
    assert summary["true_transition_to_unsupported_count"] == 1
    assert summary["unsupported_stays_unsupported_count"] == 1


def test_build_payload_uses_clarified_attrition_names_and_model_backed_top_case_lists() -> None:
    variant_definitions = build_variant_configurations(ExperimentalRerankingConfig())
    rows = [
        {
            "variant_key": "all_penalties",
            "variant_label": "All Penalties",
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
            "variant_key": "all_penalties",
            "variant_label": "All Penalties",
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
            "variant_key": "all_penalties",
            "variant_label": "All Penalties",
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
        variant_definitions=variant_definitions,
        all_variant_keys=list(variant_definitions.keys()),
        executed_variant_keys=["all_penalties"],
        runtime_notes=["Runtime-aware partial matrix: only the requested variant subset was executed in this run."],
        variants=[variant_definitions["all_penalties"]],
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert payload["attrition_summary"] == {
        "rerank_selected_top_100_count": 300,
        "rerank_final_included_comp_count": 15,
    }
    assert payload["model_backed_top_gains"][0]["account"] == "GAIN1"
    assert payload["model_backed_top_losses"][0]["account"] == "LOSS1"
    assert [row["subject_account"] for row in build_model_backed_only_rows(rows)] == ["GAIN1", "LOSS1"]
    assert "all_penalties" in payload["variant_summaries"]


def test_build_variant_configurations_and_selection() -> None:
    variants = build_variant_configurations(ExperimentalRerankingConfig())
    assert "all_penalties" in variants
    assert variants["without_subdivision_micro_location"].config.subdivision_mismatch_penalty == 0.0
    assert variants["without_subdivision_micro_location"].config.micro_location_proxy_extra_penalty == 0.0
    assert variants["without_land_mismatch"].config.land_mismatch_penalty == 0.0
    assert variants["soft_land_mismatch"].config.land_mismatch_penalty == 2.0
    assert variants["without_bedroom_mismatch"].config.bedroom_mismatch_penalty_cap == 0.0
    assert variants["without_value_psf_price_tier"].config.price_tier_penalty == 0.0
    assert variants["without_adjustment_burden"].config.adjustment_burden_hard_penalty == 0.0
    assert variants["soft_adjustment_burden"].config.adjustment_burden_hard_penalty == 3.0
    assert variants["soft_land_and_adjustment_burden"].config.land_mismatch_penalty == 2.0
    assert variants["simple_value_tier_rerank"].config.subdivision_mismatch_penalty == 0.0
    assert variants["simple_value_tier_rerank"].config.land_mismatch_penalty == 0.0
    assert variants["simple_value_tier_rerank"].config.adjustment_burden_hard_penalty == 0.0
    assert variants["simple_value_tier_rerank"].config.lower_value_credible_bonus == 0.0
    assert variants["value_tier_plus_micro_location"].config.subdivision_mismatch_penalty > 0.0
    assert variants["value_tier_plus_micro_location"].config.land_mismatch_penalty == 0.0
    assert variants["value_tier_plus_micro_location_plus_soft_land"].config.land_mismatch_penalty == 2.0
    assert variants["value_tier_plus_micro_location_plus_soft_land"].config.severe_land_mismatch_penalty == 0.0
    assert variants["without_lower_value_bonus"].config.lower_value_credible_bonus == 0.0
    assert variants["base_similarity_only"].disabled_families
    selected = select_variant_configurations(variants, ["all_penalties", "without_land_mismatch"])
    assert [variant.key for variant in selected] == ["all_penalties", "without_land_mismatch"]


def test_penalty_ablation_disables_only_intended_family_in_score() -> None:
    subject = _subject()
    risky = _row(
        "RISKY",
        appraised_value=420000.0,
        living_area_sf=1800.0,
        subdivision_name="Other",
        land_sf=12000.0,
        bedrooms=6,
    )
    stats = build_neighborhood_stats([_row("BASE"), risky])
    variants = build_variant_configurations(ExperimentalRerankingConfig())
    with_land = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=risky,
        neighborhood_stats=stats,
        config=variants["all_penalties"].config,
    )
    without_land = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=risky,
        neighborhood_stats=stats,
        config=variants["without_land_mismatch"].config,
    )
    assert "land_mismatch_penalty" in with_land["trigger_labels"] or "severe_land_mismatch_penalty" in with_land["trigger_labels"]
    assert "land_mismatch_penalty" not in without_land["trigger_labels"]
    assert "severe_land_mismatch_penalty" not in without_land["trigger_labels"]
    assert "bedroom_mismatch_penalty" in without_land["trigger_labels"]


def test_simple_variants_disable_expected_penalty_triggers() -> None:
    subject = _subject()
    risky = _row(
        "RISKY",
        appraised_value=420000.0,
        living_area_sf=1800.0,
        subdivision_name="Other",
        land_sf=12000.0,
        bedrooms=6,
    )
    stats = build_neighborhood_stats([_row("BASE"), risky])
    variants = build_variant_configurations(ExperimentalRerankingConfig())

    simple = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=risky,
        neighborhood_stats=stats,
        config=variants["simple_value_tier_rerank"].config,
    )
    assert "subdivision_mismatch_penalty" not in simple["trigger_labels"]
    assert "land_mismatch_penalty" not in simple["trigger_labels"]
    assert "severe_land_mismatch_penalty" not in simple["trigger_labels"]
    assert "bedroom_mismatch_penalty" not in simple["trigger_labels"]
    assert "adjustment_burden_soft_penalty" not in simple["trigger_labels"]

    micro = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=risky,
        neighborhood_stats=stats,
        config=variants["value_tier_plus_micro_location"].config,
    )
    assert "subdivision_mismatch_penalty" in micro["trigger_labels"]
    assert "land_mismatch_penalty" not in micro["trigger_labels"]

    soft_land = compute_experimental_rerank_score(
        subject_snapshot=subject,
        row=risky,
        neighborhood_stats=stats,
        config=variants["value_tier_plus_micro_location_plus_soft_land"].config,
    )
    assert "subdivision_mismatch_penalty" in soft_land["trigger_labels"]
    assert "land_mismatch_penalty" in soft_land["trigger_labels"]
    assert "severe_land_mismatch_penalty" not in soft_land["trigger_labels"]


def test_build_variant_complexity_summary_explains_simple_variants() -> None:
    variants = build_variant_configurations(ExperimentalRerankingConfig())
    simple_summary = build_variant_complexity_summary(variants["simple_value_tier_rerank"])
    assert simple_summary["active_penalty_families"] == ["value_psf_price_tier"]
    assert "value-tier" in simple_summary["plain_english_explanation"]
    assert "Bedroom" in simple_summary["plain_english_explanation"]
    micro_summary = build_variant_complexity_summary(variants["value_tier_plus_micro_location"])
    assert micro_summary["active_penalty_families"] == [
        "value_psf_price_tier",
        "subdivision_micro_location",
    ]
    soft_land_summary = build_variant_complexity_summary(
        variants["value_tier_plus_micro_location_plus_soft_land"]
    )
    assert soft_land_summary["active_penalty_families"] == [
        "value_psf_price_tier",
        "subdivision_micro_location",
        "soft_land_mismatch",
    ]


def test_build_payload_includes_variant_complexity_summary() -> None:
    variant_definitions = build_variant_configurations(ExperimentalRerankingConfig())
    row = {
        "variant_key": "simple_value_tier_rerank",
        "variant_label": "Simple Value-Tier Rerank",
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "229.60",
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
        "lower_value_credible_alternative_signal": {},
    }
    payload = build_payload(
        selection_summary={"selection_mode": "targeted", "selected_subject_count": 1},
        subject_rows=[row],
        variant_definitions=variant_definitions,
        all_variant_keys=list(variant_definitions.keys()),
        executed_variant_keys=["simple_value_tier_rerank"],
        runtime_notes=["partial"],
        variants=[variant_definitions["simple_value_tier_rerank"]],
        experiment_config=ExperimentalRerankingConfig(),
    )
    complexity = payload["variant_definitions"]["simple_value_tier_rerank"]["complexity_summary"]
    assert complexity["active_penalty_families"] == ["value_psf_price_tier"]
    assert "lower-value bonus" in complexity["plain_english_explanation"]


def test_parser_accepts_candidate_universe_limit() -> None:
    parser = build_parser()
    args = parser.parse_args(["--database-url", "postgresql://example", "--candidate-universe-limit", "500"])
    assert args.candidate_universe_limit == 500


def test_build_payload_preserves_bounded_proxy_disclosure() -> None:
    variant_definitions = build_variant_configurations(ExperimentalRerankingConfig())
    row = {
        "variant_key": "all_penalties",
        "variant_label": "All Penalties",
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "229.60",
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
        "lower_value_credible_alternative_signal": {},
    }
    payload = build_payload(
        selection_summary={
            "selection_mode": "targeted",
            "selected_subject_count": 1,
            "candidate_universe_mode": "bounded_proxy",
            "candidate_universe_limit": 500,
            "cohort_note": "A bounded candidate-universe proxy was used for runtime diagnostics; this is not true full-pool reranking.",
        },
        subject_rows=[row],
        variant_definitions=variant_definitions,
        all_variant_keys=list(variant_definitions.keys()),
        executed_variant_keys=["all_penalties"],
        runtime_notes=["partial"],
        variants=[variant_definitions["all_penalties"]],
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert payload["selection_summary"]["candidate_universe_mode"] == "bounded_proxy"
    assert payload["selection_summary"]["candidate_universe_limit"] == 500
    assert "not true full-pool reranking" in payload["selection_summary"]["cohort_note"]


def test_run_subject_experiment_reuses_prefetched_universe_for_current_and_smart(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []
    variants = [build_variant_configurations(ExperimentalRerankingConfig())["all_penalties"]]

    def fake_prepare_subject_snapshot_and_universe(
        service,
        conn,
        *,
        request,
        candidate_universe_limit=None,
    ):
        return (
            {
                "county_id": "harris",
                "neighborhood_code": "229.60",
                "subdivision_name": "Oak",
                "appraised_value": 300000.0,
                "living_area_sf": 2000.0,
                "bedrooms": 4,
            },
            [
                {
                    "parcel_id": "p1",
                    "county_id": "harris",
                    "tax_year": 2026,
                    "account_number": "C1",
                    "neighborhood_code": "229.60",
                    "subdivision_name": "Oak",
                    "appraised_value": 290000.0,
                    "living_area_sf": 1950.0,
                    "bedrooms": 4,
                }
            ],
            None,
        )

    def fake_replay(
        service,
        conn,
        *,
        county,
        account,
        requested_tax_year,
        strategy,
        selection_override=None,
        subject_snapshot_override=None,
    ):
        calls.append((strategy, selection_override, subject_snapshot_override))
        return {
            "replay_status": "completed",
            "support_status": "supported_with_review",
            "final_value_status": "supported_with_review",
            "value_interpretation": "final_model_value",
            "safe_requested_roll_value": 250000.0,
            "safe_requested_reduction_amount": 50000.0,
            "safe_requested_reduction_pct": 0.1667,
            "final_value_detail_json": {"included_comp_rows": []},
        }

    monkeypatch.setattr(
        "infra.scripts.run_unequal_roll_full_reranking_experiment.prepare_subject_snapshot_and_universe",
        fake_prepare_subject_snapshot_and_universe,
    )
    monkeypatch.setattr(
        "infra.scripts.run_unequal_roll_full_reranking_experiment.replay",
        fake_replay,
    )
    monkeypatch.setattr(
        "infra.scripts.run_unequal_roll_full_reranking_experiment.evaluate_lower_value_signal",
        lambda *args, **kwargs: None,
    )

    subject = type(
        "SelectedSubject",
        (),
        {
            "county_id": "harris",
            "account_number": "A1",
            "neighborhood_code": "229.60",
            "selection_source": "holdout",
        },
    )()

    rows = run_subject_experiment(
        service=object(),
        tiebreak_service=object(),
        conn=object(),
        subject=subject,
        requested_tax_year=2026,
        variants=variants,
    )

    assert [call[0] for call in calls] == [
        "current_order_cap_100",
        "similarity_top_100",
        "experimental_full_reranking_v1",
    ]
    assert all(call[1] is not None for call in calls)
    assert all(call[2] is not None for call in calls)
    assert rows[0]["comparison_ready"] is True


def test_penalty_contribution_summary_compares_against_all_penalties() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 20000.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 40.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 60.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 18000.0},
            },
            "without_adjustment_burden": {
                "variant_key": "without_adjustment_burden",
                "variant_label": "Without Adjustment Burden",
                "disabled_families": ["adjustment_burden"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 10000.0,
                    "final_status_drift_count": 1,
                    "final_status_true_downgrade_count": 0,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.01,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 35.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 55.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 18500.0},
            },
        }
    )
    assert summary[0]["variant_key"] == "without_adjustment_burden"
    assert summary[0]["taxpayer_delta_vs_all_penalties"] == -10000.0
    assert summary[0]["model_backed_taxpayer_delta_vs_all_penalties"] == 500.0
    assert summary[0]["county_net_deltas_vs_all_penalties"] == {"fort_bend": -5.0, "harris": -5.0}
    assert summary[0]["recommended_posture"] == "candidate_weaken"


def test_penalty_contribution_summary_supports_remove_branch() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 20000.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 2,
                    "true_transition_to_unsupported_count": 1,
                    "material_loss_count": 1,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 20000.0}},
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 18000.0},
            },
            "without_land_mismatch": {
                "variant_key": "without_land_mismatch",
                "variant_label": "Without Land Mismatch",
                "disabled_families": ["land_mismatch"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 32000.0,
                    "final_status_drift_count": 1,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.018,
                },
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 32000.0}},
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 28000.0},
            },
        }
    )
    assert summary[0]["recommended_posture"] == "segment_specific_tuning"


def test_penalty_contribution_summary_supports_weaken_branch() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 20000.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 2,
                    "true_transition_to_unsupported_count": 1,
                    "material_loss_count": 1,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 20000.0}},
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 18000.0},
            },
            "without_bedroom_mismatch": {
                "variant_key": "without_bedroom_mismatch",
                "variant_label": "Without Bedroom Mismatch",
                "disabled_families": ["bedroom_mismatch"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 14000.0,
                    "final_status_drift_count": 1,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 14000.0}},
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 19000.0},
            },
        }
    )
    assert summary[0]["recommended_posture"] == "candidate_weaken"


def test_penalty_contribution_summary_supports_segment_specific_tuning_branch() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 20000.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.03,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 11000.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 9000.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 17000.0},
            },
            "without_subdivision_micro_location": {
                "variant_key": "without_subdivision_micro_location",
                "variant_label": "Without Subdivision / Micro-Location",
                "disabled_families": ["subdivision_micro_location"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 35000.0,
                    "final_status_drift_count": 3,
                    "final_status_true_downgrade_count": 2,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.028,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 26000.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 9000.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 25000.0},
            },
        }
    )
    assert summary[0]["recommended_posture"] == "segment_specific_tuning"


def test_penalty_contribution_summary_uses_low_signal_inconclusive_for_tiny_positive_delta() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 100.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 60.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 40.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 100.0},
            },
            "without_bedroom_mismatch": {
                "variant_key": "without_bedroom_mismatch",
                "variant_label": "Without Bedroom Mismatch",
                "disabled_families": ["bedroom_mismatch"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 561.5,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 340.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 221.5},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 561.5},
            },
        }
    )
    assert summary[0]["recommended_posture"] == "low_signal_inconclusive"


def test_penalty_contribution_summary_keeps_value_psf_price_tier_when_removal_loses_savings_without_risk_improvement() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 50000.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 26000.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 24000.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 47000.0},
            },
            "without_value_psf_price_tier": {
                "variant_key": "without_value_psf_price_tier",
                "variant_label": "Without Value-PSF / Price Tier",
                "disabled_families": ["value_psf_price_tier"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 5000.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.021,
                },
                "county_summaries": {
                    "harris": {"net_taxpayer_delta_rerank_vs_smart": 3000.0},
                    "fort_bend": {"net_taxpayer_delta_rerank_vs_smart": 2000.0},
                },
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 4000.0},
            },
        }
    )
    assert summary[0]["recommended_posture"] == "keep"


def test_penalty_contribution_summary_candidate_remove_or_disable_when_savings_and_risk_improve() -> None:
    summary = summarize_penalty_contribution(
        {
            "all_penalties": {
                "variant_key": "all_penalties",
                "variant_label": "All Penalties",
                "disabled_families": [],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 100.0,
                    "final_status_drift_count": 3,
                    "final_status_true_downgrade_count": 2,
                    "true_transition_to_unsupported_count": 1,
                    "material_loss_count": 1,
                    "average_similarity_delta_rerank_vs_smart": 0.02,
                },
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 100.0}},
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 100.0},
            },
            "soft_adjustment_burden": {
                "variant_key": "soft_adjustment_burden",
                "variant_label": "Soft Adjustment Burden",
                "disabled_families": ["adjustment_burden_softened"],
                "overall_summary": {
                    "net_taxpayer_delta_rerank_vs_smart": 12500.0,
                    "final_status_drift_count": 2,
                    "final_status_true_downgrade_count": 1,
                    "true_transition_to_unsupported_count": 0,
                    "material_loss_count": 0,
                    "average_similarity_delta_rerank_vs_smart": 0.0195,
                },
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 12500.0}},
                "model_backed_only_summary": {"net_taxpayer_delta_rerank_vs_smart": 12000.0},
            },
        }
    )
    assert summary[0]["recommended_posture"] == "candidate_remove_or_disable"


def test_segment_posture_labels_cover_insufficient_harmful_and_promising() -> None:
    posture, reasons = classify_segment_posture(
        {
            "comparison_ready_count": 5,
            "net_taxpayer_delta_rerank_vs_smart": 100.0,
            "material_loss_count": 0,
            "material_gain_count": 1,
            "final_status_true_downgrade_count": 0,
            "true_transition_to_unsupported_count": 0,
            "unsupported_result_count": 0,
            "final_status_manual_or_unsupported_result_count": 0,
            "average_similarity_delta_rerank_vs_smart": 0.0,
        },
        model_backed_summary={"comparison_ready_count": 5, "net_taxpayer_delta_rerank_vs_smart": 100.0},
    )
    assert posture == "insufficient_sample"
    assert reasons == ["comparison_ready_below_10"]

    posture, reasons = classify_segment_posture(
        {
            "comparison_ready_count": 12,
            "net_taxpayer_delta_rerank_vs_smart": -1.0,
            "material_loss_count": 2,
            "material_gain_count": 1,
            "final_status_true_downgrade_count": 0,
            "true_transition_to_unsupported_count": 0,
            "unsupported_result_count": 0,
            "final_status_manual_or_unsupported_result_count": 0,
            "average_similarity_delta_rerank_vs_smart": 0.0,
        },
        model_backed_summary={"comparison_ready_count": 12, "net_taxpayer_delta_rerank_vs_smart": -1.0},
    )
    assert posture == "harmful"
    assert "non_positive_net" in reasons

    posture, reasons = classify_segment_posture(
        {
            "comparison_ready_count": 12,
            "net_taxpayer_delta_rerank_vs_smart": 5000.0,
            "material_loss_count": 0,
            "material_gain_count": 3,
            "final_status_true_downgrade_count": 0,
            "true_transition_to_unsupported_count": 0,
            "unsupported_result_count": 0,
            "final_status_manual_or_unsupported_result_count": 1,
            "average_similarity_delta_rerank_vs_smart": 0.01,
        },
        model_backed_summary={"comparison_ready_count": 12, "net_taxpayer_delta_rerank_vs_smart": 4000.0},
    )
    assert posture == "promising"
    assert "positive_model_backed_net" in reasons


def test_build_segment_posture_table_emits_auditable_fields() -> None:
    rows = [
        {
            "variant_key": "all_penalties",
            "comparison_ready": True,
            "county_id": "harris",
            "subject_account": "A1",
            "neighborhood_code": "8309.06",
            "smart_vs_current_taxpayer_delta": 0.0,
            "rerank_vs_current_taxpayer_delta": 2000.0,
            "rerank_vs_smart_taxpayer_delta": 2000.0,
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
            "lower_value_credible_alternative_signal": {},
        }
        for _ in range(10)
    ]
    table = build_segment_posture_table(rows)
    assert table[0]["neighborhood"] == "8309.06"
    assert "posture_label" in table[0]
    assert "posture_reason_codes" in table[0]
    assert "unsupported_result_count" in table[0]
    assert "true_transition_to_unsupported_count" in table[0]


def test_build_chunk_comparability_summary_proves_matching_cohort_and_baseline() -> None:
    payload_a = {
        "subject_rows": [
            {"variant_key": "all_penalties", "county_id": "harris", "subject_account": "A1", "neighborhood_code": "8309.06"},
            {"variant_key": "all_penalties", "county_id": "fort_bend", "subject_account": "B1", "neighborhood_code": "5902-00"},
        ],
        "variant_summaries": {
            "all_penalties": {
                "overall_summary": {"net_taxpayer_delta_rerank_vs_smart": 100.0},
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 60.0}},
            }
        },
    }
    payload_b = {
        "subject_rows": [
            {"variant_key": "all_penalties", "county_id": "fort_bend", "subject_account": "B1", "neighborhood_code": "5902-00"},
            {"variant_key": "all_penalties", "county_id": "harris", "subject_account": "A1", "neighborhood_code": "8309.06"},
        ],
        "variant_summaries": {
            "all_penalties": {
                "overall_summary": {"net_taxpayer_delta_rerank_vs_smart": 100.0},
                "county_summaries": {"harris": {"net_taxpayer_delta_rerank_vs_smart": 60.0}},
            }
        },
    }
    summary = build_chunk_comparability_summary([payload_a, payload_b])
    assert summary["subject_sets_match_exactly"] is True
    assert summary["all_penalties_baseline_matches_exactly"] is True
    assert summary["combined_interpretation_status"] == "comparable"
    assert build_subject_cohort_fingerprint(payload_a["subject_rows"]) == build_subject_cohort_fingerprint(payload_b["subject_rows"])


def test_build_chunk_comparability_summary_downgrades_when_chunks_do_not_match() -> None:
    payload_a = {
        "subject_rows": [
            {"variant_key": "all_penalties", "county_id": "harris", "subject_account": "A1", "neighborhood_code": "8309.06"},
        ],
        "variant_summaries": {
            "all_penalties": {
                "overall_summary": {"net_taxpayer_delta_rerank_vs_smart": 100.0},
                "county_summaries": {},
            }
        },
    }
    payload_b = {
        "subject_rows": [
            {"variant_key": "all_penalties", "county_id": "harris", "subject_account": "A2", "neighborhood_code": "8309.06"},
        ],
        "variant_summaries": {
            "all_penalties": {
                "overall_summary": {"net_taxpayer_delta_rerank_vs_smart": 105.0},
                "county_summaries": {},
            }
        },
    }
    summary = build_chunk_comparability_summary([payload_a, payload_b])
    assert summary["subject_sets_match_exactly"] is False
    assert summary["all_penalties_baseline_matches_exactly"] is False
    assert summary["combined_interpretation_status"] == "downgraded_non_identical_chunks"


def test_variant_summary_includes_county_summaries_and_segment_posture() -> None:
    variant = build_variant_configurations(ExperimentalRerankingConfig())["all_penalties"]
    rows = []
    for county, neighborhood in [("harris", "8309.06"), ("fort_bend", "5902-00")]:
        for index in range(10):
            rows.append(
                {
                    "variant_key": "all_penalties",
                    "variant_label": "All Penalties",
                    "comparison_ready": True,
                    "county_id": county,
                    "subject_account": f"{county}-{index}",
                    "neighborhood_code": neighborhood,
                    "smart_vs_current_taxpayer_delta": 0.0,
                    "rerank_vs_current_taxpayer_delta": 2000.0,
                    "rerank_vs_smart_taxpayer_delta": 2000.0,
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
                    "lower_value_credible_alternative_signal": {},
                }
            )
    from infra.scripts.run_unequal_roll_full_reranking_experiment import build_variant_summary

    summary = build_variant_summary(rows, variant=variant)
    assert set(summary["county_summaries"].keys()) == {"harris", "fort_bend"}
    assert summary["segment_posture_table"][0]["comparison_ready_count"] == 10


def test_build_payload_balanced_mode_discloses_seeded_discovery_bias() -> None:
    variant_definitions = build_variant_configurations(ExperimentalRerankingConfig())
    row = {
        "variant_key": "all_penalties",
        "variant_label": "All Penalties",
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
        "lower_value_credible_alternative_signal": {},
    }
    payload = build_payload(
        selection_summary={
            "selection_mode": "balanced",
            "selected_subject_count": 1,
            "excluded_harris_neighborhoods": ["8309.06"],
            "excluded_fort_bend_neighborhoods": ["5902-00"],
            "cohort_note": (
                "Balanced validation cohort for no-persist reranking generalization. "
                "This remains bounded and seeded/discovery-biased rather than countywide representative: "
                "Harris starts from seeded priority/control neighborhoods plus discovered neighborhoods, "
                "and Fort Bend uses discovered neighborhoods that remain land-repaired/high-coverage oriented. "
                "When exclusions are provided, this becomes a holdout-style bounded cohort rather than a countywide sample."
            ),
        },
        subject_rows=[row],
        variant_definitions=variant_definitions,
        all_variant_keys=list(variant_definitions.keys()),
        executed_variant_keys=["all_penalties"],
        runtime_notes=["Runtime-aware partial matrix: only the requested variant subset was executed in this run."],
        variants=[variant_definitions["all_penalties"]],
        experiment_config=ExperimentalRerankingConfig(),
    )
    assert "seeded/discovery-biased" in payload["selection_summary"]["cohort_note"]
    assert "holdout-style bounded cohort" in payload["selection_summary"]["cohort_note"]
