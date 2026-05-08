from __future__ import annotations

from pathlib import Path

from infra.scripts.run_unequal_roll_broader_smart_harvest_validation import (
    EXPERIMENT_STRATEGIES,
    SelectedSubject,
    build_group_summary,
    build_guardrail_summary,
    build_payload,
    build_subject_validation_row,
    build_value_spread_order,
    derive_trigger_labels,
    interleave_neighborhood_subjects,
    merge_balanced_subjects,
    should_trigger_strategy,
    summarize_strategy_collection,
    write_md,
)


def _strategy_row(key: str, *, delta: float, automation: str = "manual_review_only") -> dict[str, object]:
    return {
        "strategy_key": key,
        "strategy_report_label": "Label",
        "triggered": delta != 0.0,
        "taxpayer_delta_vs_smart": delta,
        "support_status_drift": False,
        "final_status_drift": False,
        "included_comp_count_delta_vs_smart": 0,
        "review_heavy_delta_vs_smart": 0,
        "likely_exclude_delta_vs_smart": 0,
        "automation_assessment": {"automation_status": automation},
        "recovery_source_explanation": "lower_value_credible_swap" if delta != 0.0 else "no_strategy_trigger",
        "subject_account": "A1",
        "neighborhood_code": "229.60",
    }


def test_derive_trigger_labels_flags_expected_signals() -> None:
    current = {
        "median_raw_appraised_value_per_sf": 200.0,
        "median_adjusted_value_per_sf": 205.0,
        "median_distance_from_neighborhood_median_psf": 8.0,
        "high_value_per_sf_outlier_count": 0,
    }
    smart = {
        "median_raw_appraised_value_per_sf": 208.0,
        "median_adjusted_value_per_sf": 214.0,
        "median_distance_from_neighborhood_median_psf": 15.0,
        "high_value_per_sf_outlier_count": 1,
    }
    labels = derive_trigger_labels(
        current_included=current,
        smart_included=smart,
        adjusted_median_delta=6000.0,
        similarity_delta=0.01,
        lower_value_available=True,
    )
    assert "value_per_sf_outlier_risk" in labels
    assert "possible_price_tier_drift" in labels
    assert "marginal_similarity_high_value_tradeoff" in labels
    assert "lower_value_credible_available" in labels


def test_should_trigger_strategy_is_gated_by_lower_value_signal() -> None:
    strategy = next(s for s in EXPERIMENT_STRATEGIES if s.key == "value_per_sf_outlier_penalty")
    triggered, matched = should_trigger_strategy(
        labels=["value_per_sf_outlier_risk"],
        lower_value_available=False,
        strategy=strategy,
    )
    assert triggered is False
    assert matched == ["value_per_sf_outlier_risk"]


def test_merge_balanced_subjects_interleaves_counties() -> None:
    merged = merge_balanced_subjects(
        [
            SelectedSubject("harris", "H1", "229.60", "seed"),
            SelectedSubject("harris", "H2", "222.02", "seed"),
        ],
        [SelectedSubject("fort_bend", "F1", "1000", "seed")],
    )
    assert [row.account_number for row in merged] == ["H1", "F1", "H2"]


def test_build_subject_validation_row_is_null_safe_for_blocked_replays() -> None:
    row = build_subject_validation_row(
        subject=SelectedSubject("harris", "123", "229.60", "seed"),
        current_result={"replay_status": "blocked"},
        smart_result={"replay_status": "blocked"},
        neighborhood_median_psf=None,
        base_swap_result=None,
        strategy_rows=[],
    )
    assert row["comparison_ready"] is False
    assert row["requested_reduction_change_smart_vs_current"] is None


def test_group_summary_and_strategy_aggregation_emit_expected_counts() -> None:
    subject_rows = [
        {
            "comparison_ready": True,
            "requested_reduction_change_smart_vs_current": -2000.0,
            "similarity_delta_smart_vs_current": 0.03,
            "included_comp_count_delta": 1,
            "review_heavy_delta": 0,
            "likely_exclude_delta": 0,
            "current_support_status": "supported_with_review",
            "smart_support_status": "supported_with_review",
            "support_status_drift": False,
            "current_final_value_status": "manual_review_required",
            "smart_final_value_status": "supported_with_review",
            "final_status_drift": True,
            "no_reduction_change_flag": True,
            "strategy_results": [_strategy_row("lower_value_credible_candidate_review_rule", delta=1500.0)],
        }
    ]
    summary = build_group_summary(subject_rows, label="harris")
    assert summary["material_loss_count"] == 1
    assert summary["post_selection_recovery_amount"] == 1500.0
    assert summary["final_status_transition_counts"] == {
        "manual_review_required -> supported_with_review": 1
    }
    strategy_summary = summarize_strategy_collection(
        next(s for s in EXPERIMENT_STRATEGIES if s.key == "lower_value_credible_candidate_review_rule"),
        subject_rows[0]["strategy_results"],
    )
    assert strategy_summary["automation_assessment_counts"]["manual_review_only"] == 1


def test_build_payload_keeps_guardrails_and_wording_honest() -> None:
    subject_row = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "229.60",
        "comparison_ready": True,
        "requested_reduction_change_smart_vs_current": -2000.0,
        "similarity_delta_smart_vs_current": 0.03,
        "included_comp_count_delta": 1,
        "review_heavy_delta": 0,
        "likely_exclude_delta": 0,
        "current_support_status": "supported_with_review",
        "smart_support_status": "supported_with_review",
        "support_status_drift": False,
        "current_final_value_status": "manual_review_required",
        "smart_final_value_status": "supported_with_review",
        "final_status_drift": True,
        "no_reduction_change_flag": False,
        "base_lower_value_classification": "manual_review_only",
        "base_lower_value_estimated_reduction_impact": 1800.0,
        "strategy_results": [_strategy_row("lower_value_credible_candidate_review_rule", delta=1500.0)],
    }
    payload = build_payload(
        selection_summary={
            "selected_subject_count": 1,
            "selection_rank_strategy": "deterministic_round_robin_with_value_spread",
            "harris_seeded_neighborhoods": [],
            "fort_bend_selected_neighborhoods": [],
            "fort_bend_selection_bias": {
                "intentionally_land_repaired_biased": True,
                "countywide_representative": False,
                "disclosure": "Fort Bend validation cohort is intentionally land-repaired biased.",
            },
        },
        subject_rows=[subject_row],
    )
    assert payload["guardrails"] == build_guardrail_summary()
    assert payload["input_contract"]["full_candidate_reranking"] is False
    assert payload["input_contract"]["post_selection_swap_only"] is True
    assert (
        payload["defensibility_support_drift_summary"]["final_status_transition_counts"]
        == {"manual_review_required -> supported_with_review": 1}
    )
    assert payload["recommendation_for_full_reranking_experiment"] in {
        "do_not_proceed_yet",
        "keep_broader_validation_only",
        "proceed_to_bounded_full_reranking_experiment",
    }


def test_build_value_spread_order_reduces_high_value_only_bias() -> None:
    rows = [
        {"account_number": "A1", "appraised_value": 100000},
        {"account_number": "A2", "appraised_value": 200000},
        {"account_number": "A3", "appraised_value": 300000},
        {"account_number": "A4", "appraised_value": 400000},
        {"account_number": "A5", "appraised_value": 500000},
    ]
    ordered = build_value_spread_order(rows)
    assert [row["account_number"] for row in ordered[:4]] == ["A3", "A4", "A1", "A5"]


def test_interleave_neighborhood_subjects_prevents_seed_starvation() -> None:
    interleaved = interleave_neighborhood_subjects(
        {
            "215.03": [
                SelectedSubject("harris", "H1", "215.03", "seed"),
                SelectedSubject("harris", "H2", "215.03", "seed"),
            ],
            "229.60": [SelectedSubject("harris", "H3", "229.60", "seed")],
            "222.02": [SelectedSubject("harris", "H4", "222.02", "seed")],
        },
        ["215.03", "229.60", "222.02"],
    )
    assert [row.account_number for row in interleaved] == ["H1", "H3", "H4", "H2"]


def test_write_md_discloses_method_boundary_and_fort_bend_bias(tmp_path: Path) -> None:
    payload = build_payload(
        selection_summary={
            "selected_subject_count": 1,
            "selection_rank_strategy": "deterministic_round_robin_with_value_spread",
            "harris_seeded_neighborhoods": ["215.03"],
            "fort_bend_selected_neighborhoods": ["5922-00"],
            "fort_bend_selection_bias": {
                "intentionally_land_repaired_biased": True,
                "countywide_representative": False,
                "disclosure": "Fort Bend neighborhoods are intentionally selected for strong land_sf coverage.",
            },
        },
        subject_rows=[
            {
                "county_id": "harris",
                "subject_account": "A1",
                "neighborhood_code": "215.03",
                "comparison_ready": True,
                "requested_reduction_change_smart_vs_current": 1000.0,
                "similarity_delta_smart_vs_current": 0.05,
                "included_comp_count_delta": 1,
                "review_heavy_delta": 0,
                "likely_exclude_delta": 0,
                "current_support_status": "supported_with_review",
                "smart_support_status": "supported_with_review",
                "support_status_drift": False,
                "current_final_value_status": "manual_review_required",
                "smart_final_value_status": "supported_with_review",
                "final_status_drift": True,
                "no_reduction_change_flag": False,
                "base_lower_value_classification": "no_safe_opportunity",
                "base_lower_value_estimated_reduction_impact": 0.0,
                "strategy_results": [],
            }
        ],
    )
    output = tmp_path / "report.md"
    write_md(output, payload)
    text = output.read_text()
    assert "Not full candidate reranking" in text
    assert "Post-selection swap only" in text
    assert "Fort Bend cohort disclosure" in text
