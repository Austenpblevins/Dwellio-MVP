from __future__ import annotations

from infra.scripts.report_unequal_roll_full_reranking_governed_fallback import (
    build_governed_case,
    build_payload,
    build_view_summaries,
    classify_governed_case,
    derive_segment_sets,
    summarize_cases,
)


def _row(**overrides):
    row = {
        "comparison_ready": True,
        "variant_key": "simple_value_tier_rerank",
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "229.60",
        "current_value_interpretation": "final_model_value",
        "smart_value_interpretation": "final_model_value",
        "rerank_value_interpretation": "final_model_value",
        "smart_final_value_status": "supported_with_review",
        "rerank_final_value_status": "supported_with_review",
        "final_status_transition_smart_to_rerank": "supported_with_review -> supported_with_review",
        "smart_included_comp_count": 20,
        "rerank_included_comp_count": 20,
        "rerank_final_included_comp_count": 20,
        "rerank_vs_smart_taxpayer_delta": 2500.0,
        "rerank_vs_current_taxpayer_delta": 4000.0,
        "smart_vs_current_taxpayer_delta": 1500.0,
        "rerank_vs_smart_similarity_delta": 0.0,
        "rerank_support_status_drift_vs_smart": False,
    }
    row.update(overrides)
    return row


def test_true_unsupported_transition_is_blocked_and_falls_back() -> None:
    row = _row(
        rerank_value_interpretation="unavailable",
        rerank_final_value_status="unsupported",
        final_status_transition_smart_to_rerank="manual_review_required -> unsupported",
        smart_final_value_status="manual_review_required",
        smart_included_comp_count=8,
        rerank_included_comp_count=0,
        rerank_final_included_comp_count=0,
        rerank_vs_smart_taxpayer_delta=-45000.0,
    )

    classification, reasons = classify_governed_case(row)
    governed = build_governed_case(row)

    assert classification == "blocked_case"
    assert "true_transition_to_unsupported" in reasons
    assert governed["fallback_to_similarity_top_100"] is True
    assert governed["governed_delta_vs_smart"] == 0.0
    assert governed["governed_delta_vs_current"] == 1500.0


def test_fort_bend_4950_04_is_blocked_even_with_positive_delta() -> None:
    row = _row(
        county_id="fort_bend",
        neighborhood_code="4950-04",
        rerank_vs_smart_taxpayer_delta=195000.0,
    )

    classification, reasons = classify_governed_case(row)

    assert classification == "blocked_case"
    assert "blocked_segment_4950_04" in reasons


def test_low_benefit_is_not_danger_blocked_but_falls_back() -> None:
    row = _row(rerank_vs_smart_taxpayer_delta=250.0)

    classification, reasons = classify_governed_case(row)
    governed = build_governed_case(row)

    assert classification == "not_eligible_low_benefit"
    assert reasons == ["taxpayer_delta_below_material_threshold"]
    assert governed["fallback_to_similarity_top_100"] is True


def test_unchanged_unsupported_result_is_insufficient_evidence_not_danger_blocked() -> None:
    row = _row(
        smart_final_value_status="unsupported",
        rerank_final_value_status="unsupported",
        final_status_transition_smart_to_rerank="unsupported -> unsupported",
        rerank_vs_smart_taxpayer_delta=61000.0,
    )

    classification, reasons = classify_governed_case(row)
    governed = build_governed_case(row)

    assert classification == "insufficient_evidence"
    assert reasons == ["rerank_result_unsupported"]
    assert governed["fallback_to_similarity_top_100"] is True
    assert governed["governed_delta_vs_smart"] == 0.0


def test_manual_review_case_retains_rerank_for_analysis() -> None:
    row = _row(
        rerank_vs_smart_taxpayer_delta=30000.0,
        rerank_vs_smart_similarity_delta=-0.021,
    )

    classification, reasons = classify_governed_case(row)
    governed = build_governed_case(row)

    assert classification == "manual_review_required"
    assert "large_outlier_gain" in reasons
    assert "material_similarity_decline" in reasons
    assert governed["fallback_to_similarity_top_100"] is False
    assert governed["governed_delta_vs_smart"] == 30000.0


def test_summary_reports_governed_and_raw_retention() -> None:
    cases = [
        build_governed_case(_row(subject_account="A1", rerank_vs_smart_taxpayer_delta=2000.0)),
        build_governed_case(_row(subject_account="A2", rerank_vs_smart_taxpayer_delta=500.0)),
        build_governed_case(
            _row(
                subject_account="A3",
                rerank_value_interpretation="unavailable",
                rerank_final_value_status="unsupported",
                final_status_transition_smart_to_rerank="manual_review_required -> unsupported",
                smart_final_value_status="manual_review_required",
                smart_included_comp_count=8,
                rerank_included_comp_count=0,
                rerank_final_included_comp_count=0,
                rerank_vs_smart_taxpayer_delta=-10000.0,
            )
        ),
    ]

    summary = summarize_cases(cases, label="test")

    assert summary["raw_ungated_net_vs_smart"] == -7500.0
    assert summary["governed_net_vs_smart"] == 2000.0
    assert summary["material_gain_count"] == 1
    assert summary["true_transition_to_unsupported_count"] == 0
    assert summary["raw_true_transition_to_unsupported_count"] == 1
    assert summary["retained_governed_true_transition_to_unsupported_count"] == 0
    assert summary["raw_included_comp_count_collapse_count"] == 1
    assert summary["caught_prevented_included_comp_count_collapse_count"] == 1
    assert summary["retained_governed_included_comp_count_collapse_count"] == 0
    assert summary["fallback_count"] == 2
    assert summary["fallback_prevented_harm_count"] == 1


def test_view_summaries_separate_automation_from_analyst_assisted() -> None:
    cases = [
        build_governed_case(_row(subject_account="A1", rerank_vs_smart_taxpayer_delta=2000.0)),
        build_governed_case(
            _row(
                subject_account="A2",
                rerank_vs_smart_taxpayer_delta=30000.0,
                rerank_vs_smart_similarity_delta=-0.021,
            )
        ),
        build_governed_case(_row(subject_account="A3", rerank_vs_smart_taxpayer_delta=250.0)),
    ]

    views = build_view_summaries(cases)

    assert views["automated_safe"]["overall"]["case_count"] == 1
    assert views["automated_safe"]["overall"]["governed_net_vs_smart"] == 2000.0
    assert views["analyst_assisted"]["overall"]["case_count"] == 2
    assert views["analyst_assisted"]["overall"]["governed_net_vs_smart"] == 32000.0
    assert views["fallback_blocked"]["overall"]["case_count"] == 1
    assert views["fallback_blocked"]["overall"]["fallback_count"] == 1


def test_segment_sets_are_derived_from_governance_artifact() -> None:
    segment_sets = derive_segment_sets(
        {
            "segment_governance_recommendations": {
                "193.09": {"governance_posture": "eligible_candidate"},
                "1347.00": {"governance_posture": "manual_review_required"},
                "4950-04": {"governance_posture": "blocked_segment"},
            }
        }
    )

    assert segment_sets["source"] == "governance_artifact"
    assert segment_sets["promising_segments"] == ["193.09"]
    assert segment_sets["manual_review_segments"] == ["1347.00"]
    assert segment_sets["blocked_segments_for_reporting"] == ["4950-04"]
    assert segment_sets["danger_blocked_segment_keys"] == ["fort_bend:4950-04"]


def test_build_payload_uses_true_full_pool_contract() -> None:
    payload = build_payload(
        source_payload={
            "selection_summary": {
                "candidate_universe_mode": "true_full_pool_requested",
                "candidate_universe_limit": None,
            },
            "execution_matrix": {
                "executed_variant_keys": ["simple_value_tier_rerank"],
            },
            "subject_rows": [_row()],
        },
        source_path=__file__,
        governance_path=None,
        governance_payload={
            "segment_governance_recommendations": {
                "229.60": {"governance_posture": "eligible_candidate"}
            }
        },
    )

    assert payload["input_contract"]["candidate_universe_mode"] == "true_full_pool_requested"
    assert payload["input_contract"]["bounded_proxy_used_for_conclusions"] is False
    assert payload["guardrails"]["db_writes_occurred"] is False
    assert payload["segment_sets"]["source"] == "governance_artifact"
    assert "simple_value_tier_rerank" in payload["variants"]
    assert "view_summaries" in payload["variants"]["simple_value_tier_rerank"]
    assert (
        payload["variants"]["simple_value_tier_rerank"]["view_summaries"]
        ["automated_safe"]["overall"]["case_count"]
        == 1
    )
