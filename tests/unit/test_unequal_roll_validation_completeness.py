from __future__ import annotations

from app.services.unequal_roll_validation_completeness import (
    classify_subject_output,
    summarize_completeness,
)
from infra.scripts.report_unequal_roll_validation_completeness import (
    _attach_downstream_replay_payload,
    _build_canonical_downstream_summary,
)


def test_classify_subject_output_marks_missing_downstream_payload_explicitly() -> None:
    row = {
        "subject_identifier": "acct-1",
        "county": "fort_bend",
        "current_appraised_value": 123456.0,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "final_value_status": None,
    }

    classification = classify_subject_output(row)

    assert classification.status_family == "defect"
    assert classification.status_code == "defect:missing_downstream_replay_payload"
    assert classification.defect_category == "missing_downstream_replay_payload"
    assert classification.completeness_gate_pass is False


def test_classify_subject_output_marks_runtime_failure_explicitly() -> None:
    row = {
        "subject_identifier": "acct-2",
        "county": "harris",
        "current_appraised_value": 250000.0,
        "discovery_completion_status": "failed",
        "probe_error": "statement timeout",
        "final_value_status": None,
    }

    classification = classify_subject_output(row)

    assert classification.status_code == "defect:runtime_or_discovery_failure"
    assert classification.defect_category == "runtime_or_discovery_failure"


def test_classify_subject_output_marks_missing_replay_source_explicitly() -> None:
    row = {
        "subject_identifier": "acct-2b",
        "county": "harris",
        "current_appraised_value": 250000.0,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "final_value_status": None,
        "downstream_payload_attachment_status": "missing_in_replay_source",
    }

    classification = classify_subject_output(row)

    assert classification.status_code == "defect:downstream_replay_payload_not_generated"
    assert classification.defect_category == "downstream_replay_payload_not_generated"


def test_classify_subject_output_marks_partial_source_payload_explicitly() -> None:
    row = {
        "subject_identifier": "acct-2c",
        "county": "harris",
        "current_appraised_value": 250000.0,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "final_value_status": None,
        "downstream_payload_attachment_status": "emitted_partial_source_payload",
    }

    classification = classify_subject_output(row)

    assert (
        classification.status_code
        == "defect:downstream_replay_payload_partial_source_emitted"
    )
    assert (
        classification.defect_category
        == "downstream_replay_payload_partial_source_emitted"
    )


def test_classify_subject_output_marks_complete_model_outcome() -> None:
    row = {
        "subject_identifier": "acct-3",
        "county": "harris",
        "current_appraised_value": 300000.0,
        "final_value_status": "unsupported",
        "requested_roll_value": 300000.0,
        "requested_reduction_amount": 0.0,
        "requested_reduction_pct": 0.0,
        "included_comp_count": 2,
        "excluded_review_heavy_count": 1,
        "excluded_likely_exclude_count": 0,
        "discovery_completion_status": "completed",
    }

    classification = classify_subject_output(row)

    assert classification.status_code == "model_outcome:unsupported"
    assert classification.status_family == "model_outcome"
    assert classification.completeness_gate_pass is True
    assert classification.defect_category is None
    assert classification.missing_required_fields == ()


def test_summarize_completeness_counts_defects_and_passes() -> None:
    rows = [
        {
            "subject_identifier": "acct-pass",
            "county": "harris",
            "current_appraised_value": 200000.0,
            "final_value_status": "unsupported",
            "requested_roll_value": 200000.0,
            "requested_reduction_amount": 0.0,
            "requested_reduction_pct": 0.0,
            "included_comp_count": 1,
            "excluded_review_heavy_count": 0,
            "excluded_likely_exclude_count": 0,
            "discovery_completion_status": "completed",
        },
        {
            "subject_identifier": "acct-defect",
            "county": "fort_bend",
            "current_appraised_value": 210000.0,
            "final_value_status": None,
            "discovery_completion_status": "completed",
            "probe_error": None,
        },
    ]

    summary = summarize_completeness(rows)

    assert summary["total_subject_rows"] == 2
    assert summary["completeness_gate_pass_count"] == 1
    assert summary["completeness_gate_fail_count"] == 1
    assert (
        summary["defect_category_distribution"]["missing_downstream_replay_payload"] == 1
    )


def test_attach_downstream_replay_payload_from_run_state() -> None:
    row = {
        "subject_identifier": "acct-attach",
        "current_appraised_value": 300000.0,
        "final_value_status": None,
    }
    run_state_map = {
        "acct-attach": {
            "summary": {
                "final_value_status": "unsupported",
                "requested_reduction_amount": 0.0,
                "included_count": 2,
                "excluded_review_heavy_count": 1,
                "excluded_likely_exclude_count": 0,
            }
        }
    }

    _attach_downstream_replay_payload(
        row,
        run_state_map=run_state_map,
        chunked_state_map={},
        fallback_subject_map={},
    )

    assert row["final_value_status"] == "unsupported"
    assert row["requested_roll_value"] == 300000.0
    assert row["requested_reduction_pct"] == 0.0
    assert row["included_comp_count"] == 2
    assert row["excluded_review_heavy_count"] == 1
    assert row["excluded_likely_exclude_count"] == 0
    assert row["downstream_payload_attachment_status"] == "attached_from_run_state"


def test_attach_downstream_replay_payload_from_chunked_state_when_run_state_missing() -> None:
    row = {
        "subject_identifier": "acct-chunked",
        "current_appraised_value": 310000.0,
        "final_value_status": None,
    }

    _attach_downstream_replay_payload(
        row,
        run_state_map={},
        chunked_state_map={
            "acct-chunked": {
                "status": "supported_with_review",
                "included": 12,
            }
        },
        fallback_subject_map={},
    )

    assert row["final_value_status"] == "supported_with_review"
    assert row["included_comp_count"] == 12
    assert row["downstream_payload_attachment_status"] == "attached_from_chunked_state"


def test_attach_downstream_replay_payload_marks_chunked_source_error_when_status_missing() -> None:
    row = {
        "subject_identifier": "acct-chunked-error",
        "current_appraised_value": 310000.0,
        "final_value_status": None,
    }

    _attach_downstream_replay_payload(
        row,
        run_state_map={},
        chunked_state_map={
            "acct-chunked-error": {
                "status": None,
                "included": None,
            }
        },
        fallback_subject_map={},
    )

    assert row["final_value_status"] is None
    assert row["downstream_payload_attachment_status"] == "replay_source_error"


def test_attach_downstream_replay_payload_reconstructs_from_fallback_artifact() -> None:
    row = {
        "subject_identifier": "acct-fallback",
        "current_appraised_value": 325000.0,
        "final_value_status": None,
    }

    _attach_downstream_replay_payload(
        row,
        run_state_map={},
        chunked_state_map={
            "acct-fallback": {
                "status": None,
                "included": None,
            }
        },
        fallback_subject_map={
            "acct-fallback": {
                "final_value_status": "supported",
                "requested_roll_value": 300000.0,
                "requested_reduction_amount": 25000.0,
                "requested_reduction_pct": 0.076923,
                "included_comp_count": 8,
                "excluded_review_heavy_count": 2,
                "excluded_likely_exclude_count": 0,
            }
        },
    )

    assert row["final_value_status"] == "supported"
    assert row["requested_roll_value"] == 300000.0
    assert row["requested_reduction_amount"] == 25000.0
    assert row["requested_reduction_pct"] == 0.076923
    assert row["included_comp_count"] == 8
    assert row["excluded_review_heavy_count"] == 2
    assert row["excluded_likely_exclude_count"] == 0
    assert (
        row["downstream_payload_attachment_status"]
        == "reconstructed_from_runtime_artifact"
    )


def test_attach_downstream_replay_payload_emits_partial_source_payload() -> None:
    row = {
        "subject_identifier": "acct-partial",
        "county": "harris",
        "current_appraised_value": 315000.0,
        "discovery_completion_status": "completed",
        "probe_error": None,
        "final_value_status": None,
    }

    _attach_downstream_replay_payload(
        row,
        run_state_map={},
        chunked_state_map={},
        fallback_subject_map={},
    )

    assert row["downstream_payload_attachment_status"] == "emitted_partial_source_payload"


def test_attach_downstream_replay_payload_emits_chunked_error_state() -> None:
    row = {
        "subject_identifier": "acct-chunked-fail",
        "current_appraised_value": 320000.0,
        "final_value_status": None,
        "discovery_completion_status": "completed",
        "probe_error": None,
    }

    _attach_downstream_replay_payload(
        row,
        run_state_map={},
        chunked_state_map={
            "acct-chunked-fail": {
                "ok": False,
                "error": "the connection is lost",
                "status": None,
                "included": None,
            }
        },
        fallback_subject_map={},
    )

    assert row["discovery_completion_status"] == "failed"
    assert row["probe_error"] == "the connection is lost"
    assert row["downstream_payload_attachment_status"] == "attached_from_chunked_state_error"


def test_build_canonical_downstream_summary_contains_defect_state() -> None:
    row = {
        "subject_identifier": "acct-canon",
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
        "discovery_completion_status": "failed",
        "probe_error": "the connection is lost",
        "downstream_payload_attachment_status": "attached_from_chunked_state_error",
        "completeness_status_code": "defect:runtime_or_discovery_failure",
        "completeness_defect_category": "runtime_or_discovery_failure",
    }

    summary = _build_canonical_downstream_summary(row)

    assert summary["subject_identifier"] == "acct-canon"
    assert summary["discovery_completion_status"] == "failed"
    assert summary["probe_error"] == "the connection is lost"
    assert summary["completeness_status_code"] == "defect:runtime_or_discovery_failure"
