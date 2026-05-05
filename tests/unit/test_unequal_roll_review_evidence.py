from __future__ import annotations

from app.services.unequal_roll_review_evidence import (
    classify_unsupported_value_semantics,
    evidence_completeness_grade,
    reconcile_outcome_row,
    summarize_run_state_candidates,
)


def test_reconcile_outcome_row_marks_recovered_payload_gap() -> None:
    result = reconcile_outcome_row(
        runtime_row={
            "discovery_completion_status": "completed",
            "probe_error": None,
            "final_value_status": None,
        },
        classified_row={
            "final_value_status": "supported_with_review",
            "completeness_status_family": "model_outcome",
            "completeness_status_code": "model_outcome:supported_with_review",
            "downstream_payload_attachment_status": "attached_from_producer_payload",
        },
        run_state_payload=None,
    )

    assert result["runtime_completed_flag"] is True
    assert result["runtime_final_value_status"] is None
    assert result["recovered_v14_status"] == "supported_with_review"
    assert result["final_reconciled_status"] == "supported_with_review"
    assert result["none_origin"] == "payload_gap_recovered_from_downstream_payload"


def test_reconcile_outcome_row_marks_unrecovered_payload_gap() -> None:
    result = reconcile_outcome_row(
        runtime_row={
            "discovery_completion_status": "completed",
            "probe_error": None,
            "final_value_status": None,
        },
        classified_row={
            "final_value_status": None,
            "completeness_status_family": "defect",
            "completeness_status_code": "defect:downstream_replay_payload_partial_source_emitted",
            "downstream_payload_attachment_status": "attached_from_producer_payload",
        },
        run_state_payload=None,
    )

    assert result["final_reconciled_status"] is None
    assert result["model_outcome_complete"] is False
    assert result["none_origin"] == "payload_gap_unrecovered"


def test_classify_unsupported_value_semantics_marks_positive_reduction_as_diagnostic() -> None:
    result = classify_unsupported_value_semantics(
        current_appraised_value=400000.0,
        final_value_status="unsupported",
        exposed_requested_roll_value=320000.0,
        exposed_requested_reduction_amount=80000.0,
        exposed_requested_reduction_pct=0.2,
    )

    assert result["safe_requested_roll_value"] is None
    assert result["value_interpretation"] == "diagnostic_only"


def test_classify_unsupported_value_semantics_marks_identity_value_as_suppressed() -> None:
    result = classify_unsupported_value_semantics(
        current_appraised_value=400000.0,
        final_value_status="unsupported",
        exposed_requested_roll_value=400000.0,
        exposed_requested_reduction_amount=0.0,
        exposed_requested_reduction_pct=0.0,
    )

    assert result["safe_requested_roll_value"] == 400000.0
    assert result["safe_requested_reduction_amount"] == 0.0
    assert result["value_interpretation"] == "suppressed_identity_value"


def test_evidence_completeness_grade_distinguishes_recovered_and_unrecovered_rows() -> None:
    assert (
        evidence_completeness_grade(
            final_reconciled_status="supported_with_review",
            model_outcome_complete=True,
            subject_context_present=True,
            comp_evidence_present=True,
            stability_metrics_present=False,
            none_origin="payload_gap_recovered_from_downstream_payload",
        )
        == "usable_with_minor_gaps"
    )
    assert (
        evidence_completeness_grade(
            final_reconciled_status=None,
            model_outcome_complete=False,
            subject_context_present=True,
            comp_evidence_present=False,
            stability_metrics_present=False,
            none_origin="payload_gap_unrecovered",
        )
        == "not_reviewable"
    )


def test_summarize_run_state_candidates_projects_review_signals() -> None:
    result = summarize_run_state_candidates(
        {
            "candidates": [
                {
                    "final_value_status": "included_in_final_value_with_review",
                    "chosen_comp_status": "review_chosen_comp",
                    "source_status": "mixed_with_unresolved_review_only",
                    "burden_status": "burden_governance_warning",
                    "adjusted_set_status": "usable_with_review_adjusted_comp",
                    "reason_codes": ["manual_review_recommended"],
                    "review_carry_forward_flag": True,
                    "adjustment_support_channels": {
                        "bedroom": {
                            "potential_adjustment_flag": True,
                            "difference_value": -1,
                        },
                        "gla": {
                            "difference_pct": 0.01,
                        },
                        "full_bath": {
                            "readiness_status": "review_required",
                            "basis_source_reason_code": "canonical_bathroom_count_missing",
                            "valuation_support_attachment_status": "attached",
                            "valuation_support_basis_status": "exact_supported",
                        },
                        "land_site": {
                            "readiness_status": "review_required",
                        },
                    },
                },
                {
                    "final_value_status": "excluded_review_heavy",
                    "chosen_comp_status": "not_chosen_comp",
                    "source_status": "supported",
                    "burden_status": "burden_governance_manual_review_recommended",
                    "adjusted_set_status": "review_heavy_adjusted_comp",
                    "reason_codes": ["exclude_recommended"],
                    "review_carry_forward_flag": False,
                    "adjustment_support_channels": {},
                },
            ]
        }
    )

    assert result["included_count"] == 1
    assert result["excluded_count"] == 1
    assert result["land_site_signal_present"] is True
    assert result["bedroom_signal"]["bedroom_difference"] == -1
    assert result["reason_code_counts"]["manual_review_recommended"] == 1
    assert result["fort_bend_bathroom_source_posture"][0]["bath_reason"] == "canonical_bathroom_count_missing"
