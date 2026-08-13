from __future__ import annotations

from app.services.unequal_roll_taxpayer_favorable_tiebreak import (
    TaxpayerFavorableTieBreakConfig,
    UnequalRollTaxpayerFavorableTieBreakService,
)


def _smart_result() -> dict:
    included = [
        {
            "candidate_parcel_id": "high-1",
            "address": "A",
            "adjusted_appraised_value": 120000.0,
            "final_value_status": "included_in_final_value",
            "review_visible_flag": False,
            "similarity_score": 0.982,
            "source_governance_status": "fallback_only",
            "burden_governance_status": "within_thresholds",
            "adjusted_set_governance_status": "usable_adjusted_comp",
            "unresolved_review_only_channel_count": 0,
            "material_adjustment_count": 1,
            "adjustment_pct_of_raw_value": 0.03,
            "line_items": [],
        },
        {
            "candidate_parcel_id": "high-2",
            "address": "B",
            "adjusted_appraised_value": 121000.0,
            "final_value_status": "included_in_final_value",
            "review_visible_flag": False,
            "similarity_score": 0.981,
            "source_governance_status": "fallback_only",
            "burden_governance_status": "within_thresholds",
            "adjusted_set_governance_status": "usable_adjusted_comp",
            "unresolved_review_only_channel_count": 0,
            "material_adjustment_count": 1,
            "adjustment_pct_of_raw_value": 0.03,
            "line_items": [],
        },
        {
            "candidate_parcel_id": "mid-1",
            "address": "C",
            "adjusted_appraised_value": 110000.0,
            "final_value_status": "included_in_final_value",
            "review_visible_flag": False,
            "similarity_score": 0.98,
            "source_governance_status": "fallback_only",
            "burden_governance_status": "within_thresholds",
            "adjusted_set_governance_status": "usable_adjusted_comp",
            "unresolved_review_only_channel_count": 0,
            "material_adjustment_count": 1,
            "adjustment_pct_of_raw_value": 0.03,
            "line_items": [],
        },
        {
            "candidate_parcel_id": "mid-2",
            "address": "D",
            "adjusted_appraised_value": 111000.0,
            "final_value_status": "included_in_final_value",
            "review_visible_flag": False,
            "similarity_score": 0.979,
            "source_governance_status": "fallback_only",
            "burden_governance_status": "within_thresholds",
            "adjusted_set_governance_status": "usable_adjusted_comp",
            "unresolved_review_only_channel_count": 0,
            "material_adjustment_count": 1,
            "adjustment_pct_of_raw_value": 0.03,
            "line_items": [],
        },
        {
            "candidate_parcel_id": "low-1",
            "address": "E",
            "adjusted_appraised_value": 100000.0,
            "final_value_status": "included_in_final_value",
            "review_visible_flag": False,
            "similarity_score": 0.978,
            "source_governance_status": "fallback_only",
            "burden_governance_status": "within_thresholds",
            "adjusted_set_governance_status": "usable_adjusted_comp",
            "unresolved_review_only_channel_count": 0,
            "material_adjustment_count": 1,
            "adjustment_pct_of_raw_value": 0.03,
            "line_items": [],
        },
        {
            "candidate_parcel_id": "low-2",
            "address": "F",
            "adjusted_appraised_value": 101000.0,
            "final_value_status": "included_in_final_value",
            "review_visible_flag": False,
            "similarity_score": 0.977,
            "source_governance_status": "fallback_only",
            "burden_governance_status": "within_thresholds",
            "adjusted_set_governance_status": "usable_adjusted_comp",
            "unresolved_review_only_channel_count": 0,
            "material_adjustment_count": 1,
            "adjustment_pct_of_raw_value": 0.03,
            "line_items": [],
        },
    ]
    return {
        "current_appraised_value": 125000.0,
        "support_status": "manual_review_required",
        "final_value_status": "manual_review_required",
        "requested_roll_value": 110500.0,
        "requested_reduction_amount": 14500.0,
        "requested_reduction_pct": 0.116,
        "included_comp_count": len(included),
        "excluded_review_heavy_count": 0,
        "excluded_likely_exclude_count": 0,
        "summary_json": {"candidate_discovery_summary": {"fallback_used": False}},
        "selection_log_json": {},
        "subject_snapshot_json": {"land_sf": 8000.0, "land_acres": 0.18},
        "final_value_detail_json": {
            "included_comp_rows": included,
            "excluded_comp_rows": [],
            "carried_forward_governance": {
                "selection_governance_status": "manual_review_required",
                "final_comp_count_status": "preferred_range",
            },
        },
    }


def _current_result() -> dict:
    current = _smart_result()
    current["requested_roll_value"] = 109500.0
    current["requested_reduction_amount"] = 15500.0
    current["requested_reduction_pct"] = 0.124
    current["final_value_detail_json"] = {
        "included_comp_rows": [
            *_smart_result()["final_value_detail_json"]["included_comp_rows"][:4],
            {
                "candidate_parcel_id": "alt-1",
                "address": "ALT1",
                "adjusted_appraised_value": 98000.0,
                "final_value_status": "included_in_final_value",
                "review_visible_flag": False,
                "similarity_score": 0.977,
                "source_governance_status": "fallback_only",
                "burden_governance_status": "within_thresholds",
                "adjusted_set_governance_status": "usable_adjusted_comp",
                "unresolved_review_only_channel_count": 0,
                "material_adjustment_count": 1,
                "adjustment_pct_of_raw_value": 0.03,
                "line_items": [],
            },
            {
                "candidate_parcel_id": "alt-2",
                "address": "ALT2",
                "adjusted_appraised_value": 99000.0,
                "final_value_status": "included_in_final_value",
                "review_visible_flag": False,
                "similarity_score": 0.976,
                "source_governance_status": "fallback_only",
                "burden_governance_status": "within_thresholds",
                "adjusted_set_governance_status": "usable_adjusted_comp",
                "unresolved_review_only_channel_count": 0,
                "material_adjustment_count": 1,
                "adjustment_pct_of_raw_value": 0.03,
                "line_items": [],
            },
        ],
        "excluded_comp_rows": [],
        "carried_forward_governance": {
            "selection_governance_status": "manual_review_required",
            "final_comp_count_status": "preferred_range",
        },
    }
    return current


def test_simulate_accepts_safe_swap_and_improves_reduction() -> None:
    service = UnequalRollTaxpayerFavorableTieBreakService()
    result = service.simulate(
        current_result=_current_result(),
        smart_result=_smart_result(),
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            minimum_included_comp_count=6,
            median_movement_cap_ratio=0.05,
        ),
    )

    assert result["swapped_comp_count"] == 1
    assert result["requested_reduction_amount"] > _smart_result()["requested_reduction_amount"]
    assert result["remains_defensible"] is True
    assert result["accepted_swaps"][0]["swapped_in_candidate_parcel_id"] == "alt-1"
    assert result["automation_assessment"]["automation_status"] == "safe_automated_candidate"


def test_simulate_rejects_candidate_with_worse_burden() -> None:
    current = _current_result()
    current["final_value_detail_json"]["included_comp_rows"][-1]["burden_governance_status"] = "warning"
    current["final_value_detail_json"]["included_comp_rows"][-1]["adjusted_appraised_value"] = 97000.0
    smart = _smart_result()
    smart["final_value_detail_json"]["included_comp_rows"][0]["burden_governance_status"] = "within_thresholds"

    service = UnequalRollTaxpayerFavorableTieBreakService()
    result = service.simulate(
        current_result=current,
        smart_result=smart,
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            minimum_included_comp_count=6,
            median_movement_cap_ratio=0.05,
        ),
    )

    assert result["swapped_comp_count"] == 1
    rejected = [row for row in result["rejected_alternatives"] if row["candidate_parcel_id"] == "alt-2"]
    assert rejected
    assert "no_compatible_displaced_comp" in rejected[0]["rejection_reasons"]


def test_simulate_respects_median_movement_cap() -> None:
    service = UnequalRollTaxpayerFavorableTieBreakService()
    result = service.simulate(
        current_result=_current_result(),
        smart_result=_smart_result(),
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=2,
            minimum_included_comp_count=6,
            median_movement_cap_ratio=0.001,
        ),
    )

    assert result["swapped_comp_count"] == 0
    assert any(
        "median_movement_exceeded_cap" in row["rejection_reasons"]
        for row in result["rejected_alternatives"]
    )


def test_assessment_marks_review_visible_swap_as_manual_review_only() -> None:
    service = UnequalRollTaxpayerFavorableTieBreakService()
    current = {
        "final_value_detail_json": {
            "included_comp_rows": [
                {
                    "candidate_parcel_id": "alt-1",
                    "review_visible_flag": True,
                    "burden_governance_status": "warning",
                    "adjusted_set_governance_status": "usable_with_review_adjusted_comp",
                    "source_governance_status": "fallback_only",
                }
            ]
        },
        "final_value_status": "supported_with_review",
        "support_status": "manual_review_required",
    }
    smart = {
        "final_value_status": "supported_with_review",
        "support_status": "manual_review_required",
        "requested_reduction_amount": 1000.0,
    }
    simulated = {
        "accepted_swaps": [{"swapped_in_candidate_parcel_id": "alt-1"}],
        "requested_reduction_amount": 1500.0,
    }

    assessment = service.assess_automation(
        current_result=current,
        smart_result=smart,
        simulated_result=simulated,
        config=TaxpayerFavorableTieBreakConfig(),
    )

    assert assessment["automation_status"] == "manual_review_only"
    assert "accepted_swap_requires_review_visible_comp" in assessment["automation_reasons"]


def test_assessment_marks_zero_gain_case_as_no_safe_opportunity() -> None:
    service = UnequalRollTaxpayerFavorableTieBreakService()
    current = {
        "final_value_detail_json": {
            "included_comp_rows": [
                {
                    "candidate_parcel_id": "alt-1",
                    "review_visible_flag": False,
                    "burden_governance_status": "within_thresholds",
                    "adjusted_set_governance_status": "usable_adjusted_comp",
                    "source_governance_status": "fallback_only",
                }
            ]
        },
        "final_value_status": "supported_with_review",
        "support_status": "manual_review_required",
    }
    smart = {
        "final_value_status": "supported_with_review",
        "support_status": "manual_review_required",
        "requested_reduction_amount": 0.0,
    }
    simulated = {
        "accepted_swaps": [{"swapped_in_candidate_parcel_id": "alt-1"}],
        "requested_reduction_amount": 0.0,
    }

    assessment = service.assess_automation(
        current_result=current,
        smart_result=smart,
        simulated_result=simulated,
        config=TaxpayerFavorableTieBreakConfig(),
    )

    assert assessment["automation_status"] == "no_safe_opportunity"
    assert "reduction_gain_below_minimum" in assessment["automation_reasons"]


def test_assessment_marks_baseline_governance_drift_as_manual_review_only() -> None:
    service = UnequalRollTaxpayerFavorableTieBreakService()
    current = {
        "final_value_detail_json": {
            "included_comp_rows": [
                {
                    "candidate_parcel_id": "alt-1",
                    "review_visible_flag": False,
                    "burden_governance_status": "within_thresholds",
                    "adjusted_set_governance_status": "usable_adjusted_comp",
                    "source_governance_status": "fallback_only",
                }
            ]
        },
        "final_value_status": "manual_review_required",
        "support_status": "manual_review_required",
    }
    smart = {
        "final_value_status": "supported_with_review",
        "support_status": "manual_review_required",
        "requested_reduction_amount": 1000.0,
    }
    simulated = {
        "accepted_swaps": [{"swapped_in_candidate_parcel_id": "alt-1"}],
        "requested_reduction_amount": 1500.0,
    }

    assessment = service.assess_automation(
        current_result=current,
        smart_result=smart,
        simulated_result=simulated,
        config=TaxpayerFavorableTieBreakConfig(),
    )

    assert assessment["automation_status"] == "manual_review_only"
    assert "baseline_governance_or_support_drift" in assessment["automation_reasons"]
