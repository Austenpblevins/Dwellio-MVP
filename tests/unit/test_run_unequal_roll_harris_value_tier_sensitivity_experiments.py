from __future__ import annotations

from infra.scripts.run_unequal_roll_harris_value_tier_sensitivity_experiments import (
    EXPERIMENT_STRATEGIES,
    ExperimentStrategy,
    build_guardrail_summary,
    build_payload,
    recommend_strategy,
    should_trigger_strategy,
    summarize_strategy_collection,
)


def _case(
    *,
    labels: list[str],
    lower_value_class: str = "safe_automated_candidate",
) -> dict[str, object]:
    value_fairness = {
        "delta_smart_minus_current": {
            "avg_similarity_score": 0.01 if "marginal_similarity_high_value_tradeoff" in labels else 0.05,
            "median_adjusted_value": 8000.0 if "possible_price_tier_drift" in labels else 2000.0,
            "median_appraised_value_per_sf": 7.0 if "value_per_sf_outlier_risk" in labels else 1.0,
            "median_adjusted_value_per_sf": 8.0 if "value_per_sf_outlier_risk" in labels else 1.0,
        },
        "current_included": {
            "median_neighborhood_value_percentile": 0.77,
            "median_distance_from_neighborhood_median_value_per_sf": 14.0,
        },
        "smart_included": {
            "median_neighborhood_value_percentile": 0.89 if "possible_price_tier_drift" in labels else 0.78,
            "median_distance_from_neighborhood_median_value_per_sf": 25.0 if "possible_price_tier_drift" in labels else 14.5,
            "high_value_per_sf_outlier_count": 1 if "value_per_sf_outlier_risk" in labels else 0,
        },
    }
    smart_rows = [
        {"source_features": {"subdivision_name": "SUB B" if "possible_micro_location_proxy" in labels else "SUB A"}}
    ]
    return {
        "county": "harris",
        "account": "A1",
        "neighborhood_code": "229.60",
        "cohort_role": "priority_taxpayer_loss",
        "subject_features": {"subdivision_name": "SUB A"},
        "comparison_summary": {"adjusted_median_change": value_fairness["delta_smart_minus_current"]["median_adjusted_value"]},
        "value_fairness_outlier_report": value_fairness,
        "equally_credible_lower_value_alternative_report": {
            "safe_manual_or_no_safe": lower_value_class,
            "opportunity_class": lower_value_class,
            "estimated_reduction_impact": 2500.0,
        },
        "smart_included_comp_rows": smart_rows,
    }


def _strategy(key: str) -> ExperimentStrategy:
    for strategy in EXPERIMENT_STRATEGIES:
        if strategy.key == key:
            return strategy
    raise AssertionError(f"Unknown strategy {key}")


def test_should_trigger_strategy_uses_labels_and_lower_value_gate() -> None:
    case = _case(labels=["value_per_sf_outlier_risk", "possible_price_tier_drift"])
    triggered, matched = should_trigger_strategy(case, _strategy("value_per_sf_outlier_penalty"))
    assert triggered is True
    assert matched == ["value_per_sf_outlier_risk"]

    no_alt_case = _case(labels=["value_per_sf_outlier_risk"], lower_value_class="no_safe_opportunity")
    triggered, matched = should_trigger_strategy(no_alt_case, _strategy("value_per_sf_outlier_penalty"))
    assert triggered is False
    assert matched == ["value_per_sf_outlier_risk"]


def test_lower_value_review_rule_triggers_without_extra_labels() -> None:
    case = _case(labels=[], lower_value_class="manual_review_only")
    triggered, matched = should_trigger_strategy(case, _strategy("lower_value_credible_candidate_review_rule"))
    assert triggered is True
    assert matched == []


def test_strategy_summary_and_recommendation_logic() -> None:
    rows = [
        {
            "strategy_key": "combined_conservative_sensitivity_strategy",
            "strategy_report_label": "Combined Conservative Triggered Swap",
            "neighborhood_code": "229.60",
            "triggered": True,
            "taxpayer_delta_vs_smart": 12000.0,
            "similarity_delta_vs_smart": -0.002,
            "adjusted_median_delta_vs_smart": -4500.0,
            "included_comp_count_delta_vs_smart": 0,
            "review_heavy_delta_vs_smart": 0,
            "likely_exclude_delta_vs_smart": 0,
            "support_status_drift": False,
            "final_status_drift": False,
            "recovered_lower_value_credible_amount": 12000.0,
            "subject_account": "A1",
            "automation_assessment": {"automation_status": "manual_review_only"},
            "recovery_source_explanation": "combined_triggered_swap",
        },
        {
            "strategy_key": "combined_conservative_sensitivity_strategy",
            "strategy_report_label": "Combined Conservative Triggered Swap",
            "neighborhood_code": "222.02",
            "triggered": False,
            "taxpayer_delta_vs_smart": 0.0,
            "similarity_delta_vs_smart": 0.0,
            "adjusted_median_delta_vs_smart": 0.0,
            "included_comp_count_delta_vs_smart": 0,
            "review_heavy_delta_vs_smart": 0,
            "likely_exclude_delta_vs_smart": 0,
            "support_status_drift": False,
            "final_status_drift": False,
            "recovered_lower_value_credible_amount": 0.0,
            "subject_account": "A2",
            "automation_assessment": {"automation_status": "no_safe_opportunity"},
            "recovery_source_explanation": "no_strategy_trigger",
        },
    ]
    summary = summarize_strategy_collection(_strategy("combined_conservative_sensitivity_strategy"), rows)
    assert summary["cases_triggered"] == 1
    assert summary["net_taxpayer_impact"] == 12000.0
    assert summary["recommendation"] == "manual_review_candidate"
    assert summary["neighborhoods_helped"][0]["neighborhood_code"] == "229.60"
    assert summary["automation_assessment_counts"]["manual_review_only"] == 1
    assert summary["top_recovered_cases"][0]["subject_account"] == "A1"


def test_build_payload_keeps_guardrails_false() -> None:
    payload = build_payload(
        source_artifact="/tmp/source.json",
        input_generated_at="2026-05-08T14:00:00",
        input_resolution={"input_artifact_kind": "clarification_wrapper"},
        requested_tax_year=2026,
        case_results=[
            {
                "strategy_key": "value_per_sf_outlier_penalty",
                "strategy_report_label": "Value-per-SF Outlier Triggered Swap",
                "subject_account": "A1",
                "cohort_role": "priority_taxpayer_loss",
                "neighborhood_code": "229.60",
                "triggered": True,
                "taxpayer_delta_vs_smart": 5000.0,
                "similarity_delta_vs_smart": -0.001,
                "adjusted_median_delta_vs_smart": -3000.0,
                "included_comp_count_delta_vs_smart": 0,
                "review_heavy_delta_vs_smart": 0,
                "likely_exclude_delta_vs_smart": 0,
                "support_status_drift": False,
                "final_status_drift": False,
                "recovered_lower_value_credible_amount": 5000.0,
                "automation_assessment": {"automation_status": "manual_review_only"},
                "recovery_source_explanation": "value_per_sf_outlier_triggered_swap",
            }
            for _ in range(len(EXPERIMENT_STRATEGIES))
        ],
    )
    guardrails = build_guardrail_summary()
    assert payload["guardrails"] == guardrails
    assert payload["input_contract"]["input_artifact_kind"] == "clarification_wrapper"
    assert payload["input_contract"]["script_mode"] == "post_selection_swap_sensitivity_experiment_runner"
    assert payload["input_contract"]["full_candidate_reranking"] is False
    assert payload["input_contract"]["production_scoring_penalty"] is False
    assert payload["cohort_summary"]["cases_reviewed"] == 1
    assert recommend_strategy(payload["per_subject_strategy_table"], 5000.0) == "keep_analysis_only"
    assert payload["comparison_baseline"] == "similarity_top_100"
    assert payload["experiment_method"] == "post_selection_swap_recompute"
    assert "does not rerank the full same-neighborhood candidate universe" in payload["experiment_limitations"][1]
    assert payload["strategy_summary"][0]["trigger_signal_only"] is True
    assert payload["strategy_summary"][0]["strategy_report_label"] == "Value-per-SF Outlier Triggered Swap"
    assert payload["strategy_summary"][0]["automation_assessment_counts"]["manual_review_only"] == len(EXPERIMENT_STRATEGIES)
    assert payload["strategy_summary"][0]["recovery_source_counts"]["value_per_sf_outlier_triggered_swap"] == len(EXPERIMENT_STRATEGIES)
    assert payload["evidence_backed_findings"][0]["comparison_baseline"] == "similarity_top_100"
    assert payload["heuristic_findings"][0]["note"].startswith("These labels trigger post-selection swap experiments")
    assert payload["hypotheses_requiring_more_validation"][0]["follow_up"] == "test_full_reranking_variant"
