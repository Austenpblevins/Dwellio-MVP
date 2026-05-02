from __future__ import annotations

from infra.scripts.emit_unequal_roll_producer_downstream_payloads import (
    _build_producer_downstream_payload,
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
