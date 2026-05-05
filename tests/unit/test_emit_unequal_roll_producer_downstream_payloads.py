from __future__ import annotations

from infra.scripts.emit_unequal_roll_producer_downstream_payloads import (
    _build_producer_downstream_payload,
    _compact_final_value_review_payload,
    _normalize_chunk_row,
)


def test_build_payload_prefers_runtime_values_over_chunk_values() -> None:
    runtime_row = {
        "subject_identifier": "acct-1",
        "county": "harris",
        "neighborhood": "229.60",
        "current_appraised_value": 300000.0,
        "final_value_status": "unsupported",
        "requested_roll_value": 300000.0,
        "requested_reduction_amount": 0.0,
        "requested_reduction_pct": 0.0,
        "included_comp_count": 2,
        "excluded_review_heavy_count": 1,
        "excluded_likely_exclude_count": 0,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "source_chunk": 1,
    }
    payload = _build_producer_downstream_payload(
        runtime_row,
        chunk_subject_map={
            "acct-1": {
                "final_value_status": "supported_with_review",
                "requested_roll_value": 280000.0,
            }
        },
    )
    assert payload["final_value_status"] == "unsupported"
    assert payload["requested_roll_value"] == 300000.0
    assert payload["downstream_payload_attachment_status"] == "producer_full_payload_emitted"


def test_build_payload_uses_chunk_values_when_runtime_missing() -> None:
    runtime_row = {
        "subject_identifier": "acct-2",
        "county": "harris",
        "neighborhood": "229.60",
        "current_appraised_value": 300000.0,
        "final_value_status": None,
        "requested_roll_value": None,
        "requested_reduction_amount": None,
        "requested_reduction_pct": None,
        "included_comp_count": None,
        "excluded_review_heavy_count": None,
        "excluded_likely_exclude_count": None,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "source_chunk": 1,
    }
    payload = _build_producer_downstream_payload(
        runtime_row,
        chunk_subject_map={
            "acct-2": {
                "final_value_status": "supported_with_review",
                "requested_roll_value": 285000.0,
                "requested_reduction_amount": 15000.0,
                "requested_reduction_pct": 0.05,
                "included_comp_count": 8,
                "excluded_review_heavy_count": 1,
                "excluded_likely_exclude_count": 0,
                "__source_artifact": "/tmp/chunk.json",
            }
        },
    )
    assert payload["final_value_status"] == "supported_with_review"
    assert payload["requested_roll_value"] == 285000.0
    assert payload["lineage_source_artifact"] == "/tmp/chunk.json"
    assert payload["downstream_payload_attachment_status"] == "producer_full_payload_emitted"


def test_normalize_chunk_row_supports_legacy_subject_result_shape() -> None:
    row = {
        "subject": {"account_number": "legacy-1"},
        "result": {
            "final_value_status": "manual_review_required",
            "requested_roll_value": 210000.0,
            "included_comp_count": 5,
        },
    }
    normalized = _normalize_chunk_row(row)
    assert normalized["subject_identifier"] == "legacy-1"
    assert normalized["final_value_status"] == "manual_review_required"
    assert normalized["requested_roll_value"] == 210000.0
    assert normalized["included_comp_count"] == 5


def test_build_payload_emits_compact_review_payload_from_final_value_detail() -> None:
    runtime_row = {
        "subject_identifier": "acct-rich",
        "county": "harris",
        "neighborhood": "229.60",
        "current_appraised_value": 300000.0,
        "final_value_status": "supported_with_review",
        "requested_roll_value": 285000.0,
        "requested_reduction_amount": 15000.0,
        "requested_reduction_pct": 0.05,
        "included_comp_count": 2,
        "excluded_review_heavy_count": 1,
        "excluded_likely_exclude_count": 0,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "source_chunk": 1,
        "final_value_detail_json": {
            "final_value_status": "supported_with_review",
            "final_value_set_summary": {"included_count": 2},
            "median_calculation": {"requested_roll_value": 285000.0},
            "ordered_adjusted_values": [
                {
                    "final_value_position": 1,
                    "candidate_parcel_id": "parcel-1",
                    "address": "1 MAIN ST",
                    "adjusted_appraised_value": 280000.0,
                    "adjusted_appraised_value_per_sf": 140.0,
                }
            ],
            "stability_metrics": {
                "median_all": 285000.0,
                "median_minus_high_low": 285000.0,
                "max_leave_one_out_delta": 0.0,
                "adjusted_value_iqr": 10000.0,
            },
            "qa_flags": {"leave_one_out_review_flag": False},
            "methodology_guardrails": {
                "final_requested_value_formula": "median_of_adjusted_appraised_values"
            },
            "carried_forward_governance": {"support_status": "supported_with_review"},
            "included_comp_rows": [
                {
                    "unequal_roll_candidate_id": "cand-1",
                    "candidate_parcel_id": "parcel-1",
                    "address": "1 MAIN ST",
                    "chosen_comp_status": "chosen_comp",
                    "final_value_status": "included_in_final_value",
                    "raw_appraised_value": 270000.0,
                    "adjusted_appraised_value": 280000.0,
                    "adjustment_math_status": "adjusted",
                    "adjusted_set_governance_status": "usable_adjusted_comp",
                    "burden_governance_status": "within_thresholds",
                    "source_governance_status": "fallback_only",
                    "line_items": [
                        {"adjustment_type": "gla", "signed_adjustment_amount": 10000.0}
                    ],
                }
            ],
            "excluded_comp_rows": [
                {
                    "unequal_roll_candidate_id": "cand-2",
                    "candidate_parcel_id": "parcel-2",
                    "address": "2 MAIN ST",
                    "chosen_comp_status": "review_chosen_comp",
                    "final_value_status": "excluded_review_heavy",
                    "raw_appraised_value": 290000.0,
                    "adjusted_appraised_value": 310000.0,
                    "adjustment_math_status": "adjusted",
                    "adjusted_set_governance_status": "review_heavy_adjusted_comp",
                    "burden_governance_status": "manual_review_recommended",
                    "source_governance_status": "supported",
                    "exclusion_reason_code": "adjusted_set_review_heavy",
                }
            ],
        },
    }
    payload = _build_producer_downstream_payload(runtime_row, chunk_subject_map={})
    compact = payload["compact_final_value_review_payload"]
    assert compact["payload_status"] == "full_from_final_value_detail"
    assert compact["stability_metrics"]["median_all"] == 285000.0
    assert compact["included_comp_rows"][0]["candidate_parcel_id"] == "parcel-1"
    assert compact["included_comp_rows"][0]["line_item_summary"]["line_item_count"] == 1
    assert compact["excluded_comp_rows"][0]["exclusion_reason_code"] == "adjusted_set_review_heavy"


def test_compact_review_payload_can_fall_back_to_selection_log_detail() -> None:
    payload = _compact_final_value_review_payload(
        {
            "selection_log_json": {
                "final_value": {
                    "final_value_status": "manual_review_required",
                    "final_value_set_summary": {"included_count": 6},
                    "median_calculation": {"requested_roll_value": 300000.0},
                    "stability_metrics": {"median_all": 300000.0},
                    "qa_flags": {"leave_one_out_review_flag": True},
                    "methodology_guardrails": {"similarity_score_selection_only_flag": True},
                }
            }
        },
        None,
    )
    assert payload is not None
    assert payload["payload_status"] == "partial_from_selection_log"
    assert payload["stability_metrics"]["median_all"] == 300000.0
    assert payload["availability"]["included_comp_rows_available"] is False
