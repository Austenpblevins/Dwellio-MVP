from __future__ import annotations

from infra.scripts import build_unequal_roll_governed_simple_rerank_packet_evidence as evidence


def test_baseline_candidate_requires_model_backed_material_baseline() -> None:
    raw = {
        "smart_value_interpretation": "final_model_value",
        "smart_final_value_status": "supported_with_review",
        "smart_requested_reduction_amount": 2500,
        "smart_included_comp_count": 20,
    }
    assert evidence.baseline_candidate(raw, {}) is True
    assert evidence.baseline_candidate({**raw, "smart_requested_reduction_amount": 999}, {}) is False
    assert evidence.baseline_candidate({**raw, "smart_value_interpretation": "diagnostic"}, {}) is False
    assert evidence.baseline_candidate({**raw, "smart_final_value_status": "unsupported"}, {}) is False
    assert evidence.baseline_candidate({**raw, "smart_included_comp_count": 0}, {}) is False


def test_merge_fallback_case_without_replay_preserves_packet_counts() -> None:
    raw = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "100.00",
        "smart_requested_reduction_amount": 0,
    }
    governed = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "100.00",
        "governance_view": "fallback_blocked",
        "governance_classification": "not_eligible_low_benefit",
        "governance_reasons": ["taxpayer_delta_below_material_threshold"],
        "governed_delta_vs_smart": 0,
    }
    row = evidence.merge_fallback_case(
        raw_row=raw,
        governed_row=governed,
        requested_tax_year=2026,
        baseline_result=None,
    )
    assert row["governance_view"] == "fallback_blocked"
    assert row["governance_reasons"] == "taxpayer_delta_below_material_threshold"
    assert row["requested_tax_year"] == 2026
    assert row.get("subject_parcel_id") is None


def test_merge_fallback_case_with_replayed_baseline_adds_comp_ids() -> None:
    raw = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "100.00",
        "smart_requested_reduction_amount": 2500,
    }
    governed = {
        "county_id": "harris",
        "subject_account": "A1",
        "neighborhood_code": "100.00",
        "governance_view": "fallback_blocked",
        "governance_classification": "not_eligible_low_benefit",
        "governance_reasons": ["taxpayer_delta_below_material_threshold"],
        "governed_delta_vs_smart": 0,
    }
    result = {
        "parcel_id": "subject-1",
        "requested_roll_value": 297500,
        "requested_reduction_amount": 2500,
        "final_value_status": "supported_with_review",
        "value_interpretation": "final_model_value",
        "included_comp_count": 1,
        "replay_status": "completed",
        "final_value_detail_json": {
            "included_comp_rows": [
                {"candidate_parcel_id": "comp-1"},
            ]
        },
    }
    row = evidence.merge_fallback_case(
        raw_row=raw,
        governed_row=governed,
        requested_tax_year=2026,
        baseline_result=result,
    )
    assert row["subject_parcel_id"] == "subject-1"
    assert row["smart_requested_reduction_amount"] == 2500
    assert row["smart_value_interpretation"] == "final_model_value"
    assert row["smart_full_included_comp_ids"] == "comp-1"
    assert row["rerank_full_included_comp_ids"] == "comp-1"
    assert row["complete_comp_set_recovered"] is True


def test_comp_row_preserves_adjustment_line_item_amounts() -> None:
    row = evidence.comp_row_from_detail(
        case_row={
            "county_id": "harris",
            "subject_account": "A1",
            "neighborhood_code": "100.00",
            "requested_tax_year": 2026,
        },
        detail={
            "candidate_parcel_id": "comp-1",
            "account_number": "C1",
            "raw_appraised_value": 300000,
            "adjusted_appraised_value": 295000,
            "adjusted_appraised_value_per_sf": 147.5,
            "line_items": [
                {"adjustment_type": "gla", "signed_adjustment_amount": -2500},
                {"adjustment_type": "age", "signed_adjustment_amount": 1500},
                {"adjustment_type": "full_bath", "signed_adjustment_amount": None},
                {"adjustment_type": "pool", "signed_adjustment_amount": 0},
            ],
        },
        membership="overlap",
    )

    assert row["living_area_adjustment"] == -2500
    assert row["age_effective_age_adjustment"] == 1500
    assert row["full_bath_adjustment"] == "not_applicable"
    assert row["pool_adjustment"] == 0
    assert row["total_adjustment_amount"] == -1000
    assert row["total_abs_adjustment"] == 4000
    assert row["adjustment_percent"] == -0.003333
    assert row["adjustment_source_status"] == "line_items_available"
    assert row["line_item_count"] == 4
    assert "signed_adjustment_amount" in row["adjustment_line_items_json"]
