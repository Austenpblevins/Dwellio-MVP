from __future__ import annotations

from infra.scripts.evaluate_unequal_roll_governance_tiering import (
    _classify_governance_tier,
)


def _subject(
    *,
    account: str,
    county: str = "harris",
    status: str = "manual_review_required",
    reduction_amount: float | None = 0.0,
    included_count: int = 14,
    review_heavy_count: int = 1,
    likely_exclude_count: int = 0,
    median_all: float = 300000.0,
    loo_delta: float = 4000.0,
    iqr: float = 45000.0,
    max_adjustment_pct: float = 0.08,
    warning_rows: int = 2,
    conflict_rows: int = 1,
    source_status: str = "mixed_with_unresolved_review_only",
) -> dict:
    included_rows = []
    for idx in range(included_count):
        is_warning = idx < warning_rows
        is_conflict = idx < conflict_rows
        included_rows.append(
            {
                "source_governance_status": source_status,
                "burden_governance_status": "warning" if is_warning else "within_thresholds",
                "conflict_divergence_governance": {
                    "raw_adjusted_divergence_flag": is_conflict,
                    "adjusted_conflict_indicator_flag": is_conflict,
                    "divergence_requires_review_flag": is_conflict,
                },
                "bathroom_boundary_context": {
                    "valuation_bathroom_features": {
                        "attachment_status": "attached",
                        "bathroom_count_status": "exact_supported",
                    }
                },
            }
        )
    return {
        "account": account,
        "county": county,
        "final_value_status": status,
        "requested_reduction_amount": reduction_amount,
        "included_comp_count": included_count,
        "excluded_review_heavy_count": review_heavy_count,
        "excluded_likely_exclude_count": likely_exclude_count,
        "final_value_detail_json": {
            "final_value_set_summary": {
                "included_count": included_count,
                "excluded_review_heavy_count": review_heavy_count,
                "excluded_likely_exclude_count": likely_exclude_count,
            },
            "stability_metrics": {
                "median_all": median_all,
                "max_leave_one_out_delta": loo_delta,
                "adjusted_value_iqr": iqr,
                "max_adjustment_pct": max_adjustment_pct,
            },
            "included_comp_rows": included_rows,
        },
    }


def test_supported_with_review_caveat_for_stable_no_reduction() -> None:
    row = _classify_governance_tier(
        _subject(
            account="stable-no-reduction",
            reduction_amount=0.0,
            warning_rows=2,
            conflict_rows=1,
        )
    )
    assert row["recommended_tier"] == "supported_with_review_caveat"
    assert row["burden_interpretation"] == "warning_visible"


def test_manual_stop_for_positive_reduction_with_high_warning_and_conflict() -> None:
    row = _classify_governance_tier(
        _subject(
            account="pos-risk",
            reduction_amount=25000.0,
            warning_rows=12,
            conflict_rows=6,
            included_count=16,
        )
    )
    assert row["recommended_tier"] == "manual_stop"
    assert row["burden_interpretation"] == "warning_blocking"


def test_unsupported_blocking_for_current_unsupported() -> None:
    row = _classify_governance_tier(
        _subject(
            account="unsupported",
            status="unsupported",
            reduction_amount=0.0,
            included_count=2,
            review_heavy_count=4,
        )
    )
    assert row["recommended_tier"] == "unsupported_blocking"
    assert row["caveat_type"] == "unsupported_blocking_defect"
