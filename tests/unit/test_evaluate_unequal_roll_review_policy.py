from __future__ import annotations

from infra.scripts.evaluate_unequal_roll_review_policy import classify_subject


def _subject(
    *,
    account: str,
    county: str = "harris",
    final_value_status: str = "manual_review_required",
    current_value: float = 250000.0,
    requested_roll: float = 250000.0,
    reduction_amount: float = 0.0,
    reduction_pct: float = 0.0,
    included_count: int = 12,
    review_heavy_count: int = 0,
    likely_exclude_count: int = 0,
    median_all: float = 250000.0,
    max_loo: float = 5000.0,
    iqr: float = 60000.0,
    max_adjustment_pct: float = 0.12,
    source_status: str = "mixed_with_unresolved_review_only",
    attachment_status: str = "not_applicable",
    conflict_rows: int = 0,
    burden_status: str = "within_thresholds",
) -> dict:
    included_rows = []
    for idx in range(included_count):
        conflict = idx < conflict_rows
        included_rows.append(
            {
                "similarity_score": 0.73,
                "source_governance_status": source_status,
                "burden_governance_status": burden_status,
                "adjustment_pct_of_raw_value": 0.08,
                "adjusted_set_governance_reason_codes": [
                    "review_carry_forward_requires_review_visibility",
                    "unresolved_review_only_channels_present",
                ],
                "conflict_divergence_governance": {
                    "raw_adjusted_divergence_flag": conflict,
                    "adjusted_conflict_indicator_flag": conflict,
                    "divergence_requires_review_flag": conflict,
                },
                "bathroom_boundary_context": {
                    "fort_bend_bathroom_modifier": {"attachment_status": attachment_status}
                },
            }
        )
    return {
        "account": account,
        "county": county,
        "address": "test",
        "final_value_status": final_value_status,
        "current_appraised_value": current_value,
        "safe_requested_roll_value": requested_roll,
        "safe_requested_reduction_amount": reduction_amount,
        "safe_requested_reduction_pct": reduction_pct,
        "evidence_completeness_grade": "complete_review_evidence",
        "compact_final_value_review_payload": {
            "final_value_set_summary": {
                "included_count": included_count,
                "excluded_review_heavy_count": review_heavy_count,
                "excluded_likely_exclude_count": likely_exclude_count,
                "all_included_review_visible_flag": True,
            },
            "stability_metrics": {
                "median_all": median_all,
                "max_leave_one_out_delta": max_loo,
                "adjusted_value_iqr": iqr,
                "max_adjustment_pct": max_adjustment_pct,
            },
            "included_comp_rows": included_rows,
        },
    }


def test_classify_subject_marks_no_reduction_candidate_as_safe() -> None:
    result = classify_subject(
        _subject(
            account="safe-no-reduction",
            included_count=16,
            review_heavy_count=2,
            likely_exclude_count=0,
            median_all=160000.0,
            max_loo=400.0,
            iqr=38000.0,
            reduction_amount=0.0,
            reduction_pct=0.0,
            county="fort_bend",
            attachment_status="missing",
            conflict_rows=2,
        )
    )

    assert result["analysis_only_proposed_status"] == "would_be_supported_with_review_candidate"
    assert result["safety_label"] == "safe"
    assert result["qualifies_base_rule"] is True


def test_classify_subject_marks_positive_reduction_candidate_as_borderline() -> None:
    result = classify_subject(
        _subject(
            account="borderline-positive",
            included_count=18,
            review_heavy_count=0,
            likely_exclude_count=0,
            median_all=228000.0,
            max_loo=11000.0,
            iqr=100000.0,
            max_adjustment_pct=0.13,
            reduction_amount=28000.0,
            reduction_pct=0.11,
            conflict_rows=2,
        )
    )

    assert result["analysis_only_proposed_status"] == "would_be_supported_with_review_candidate"
    assert result["safety_label"] == "borderline"
    assert result["qualifies_base_rule"] is True


def test_classify_subject_rejects_thin_or_review_heavy_case() -> None:
    result = classify_subject(
        _subject(
            account="reject-thin",
            final_value_status="unsupported",
            included_count=2,
            review_heavy_count=16,
            likely_exclude_count=0,
            median_all=210000.0,
            max_loo=113000.0,
            iqr=227000.0,
            reduction_amount=0.0,
            reduction_pct=0.0,
        )
    )

    assert result["analysis_only_proposed_status"] == "unsupported"
    assert result["safety_label"] == "unsafe"
    assert "current_status_manual_review_required" in result["rejected_checks"]


def test_classify_subject_rejects_positive_reduction_case_with_likely_excludes() -> None:
    result = classify_subject(
        _subject(
            account="reject-likely",
            included_count=6,
            review_heavy_count=3,
            likely_exclude_count=1,
            median_all=406000.0,
            max_loo=29000.0,
            iqr=118000.0,
            reduction_amount=13000.0,
            reduction_pct=0.03,
        )
    )

    assert result["qualifies_base_rule"] is False
    assert result["analysis_only_proposed_status"] == "manual_review_required"
    assert "likely_exclude_count_zero" in result["rejected_checks"]
