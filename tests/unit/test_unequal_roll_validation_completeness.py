from __future__ import annotations

from app.services.unequal_roll_validation_completeness import (
    classify_subject_output,
    summarize_completeness,
)
from infra.scripts.report_unequal_roll_validation_completeness import (
    _attach_downstream_replay_payload,
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

    _attach_downstream_replay_payload(row, run_state_map=run_state_map)

    assert row["final_value_status"] == "unsupported"
    assert row["requested_roll_value"] == 300000.0
    assert row["requested_reduction_pct"] == 0.0
    assert row["included_comp_count"] == 2
    assert row["excluded_review_heavy_count"] == 1
    assert row["excluded_likely_exclude_count"] == 0
    assert row["downstream_payload_attachment_status"] == "attached_from_run_state"
