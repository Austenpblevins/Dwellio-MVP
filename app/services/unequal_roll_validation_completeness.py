from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any


MODEL_OUTCOME_STATUSES = {
    "supported",
    "supported_with_review",
    "manual_review_required",
    "unsupported",
}


@dataclass(frozen=True)
class ValidationOutputClassification:
    status_code: str
    status_family: str
    completeness_gate_pass: bool
    defect_category: str | None
    missing_required_fields: tuple[str, ...]


def classify_subject_output(row: dict[str, Any]) -> ValidationOutputClassification:
    final_value_status = row.get("final_value_status")
    missing_fields = _missing_required_fields(row, final_value_status=final_value_status)

    if final_value_status in MODEL_OUTCOME_STATUSES:
        return ValidationOutputClassification(
            status_code=f"model_outcome:{final_value_status}",
            status_family="model_outcome",
            completeness_gate_pass=len(missing_fields) == 0,
            defect_category=(
                "incomplete_model_outcome_payload"
                if len(missing_fields) > 0
                else None
            ),
            missing_required_fields=missing_fields,
        )

    defect_category = _classify_defect_category(row)
    return ValidationOutputClassification(
        status_code=f"defect:{defect_category}",
        status_family="defect",
        completeness_gate_pass=False,
        defect_category=defect_category,
        missing_required_fields=missing_fields,
    )


def summarize_completeness(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counter: Counter[str] = Counter()
    family_counter: Counter[str] = Counter()
    defect_counter: Counter[str] = Counter()
    missing_counter: Counter[str] = Counter()
    pass_count = 0

    for row in rows:
        classification = classify_subject_output(row)
        status_counter[classification.status_code] += 1
        family_counter[classification.status_family] += 1
        if classification.defect_category is not None:
            defect_counter[classification.defect_category] += 1
        for field in classification.missing_required_fields:
            missing_counter[field] += 1
        if classification.completeness_gate_pass:
            pass_count += 1

    total = len(rows)
    return {
        "total_subject_rows": total,
        "completeness_gate_pass_count": pass_count,
        "completeness_gate_fail_count": total - pass_count,
        "completeness_gate_pass_rate": (pass_count / total) if total else 0.0,
        "status_code_distribution": dict(status_counter),
        "status_family_distribution": dict(family_counter),
        "defect_category_distribution": dict(defect_counter),
        "missing_required_field_counts": dict(missing_counter),
    }


def _missing_required_fields(
    row: dict[str, Any], *, final_value_status: str | None
) -> tuple[str, ...]:
    required = ["subject_identifier", "county", "current_appraised_value"]
    if final_value_status in MODEL_OUTCOME_STATUSES:
        required.extend(
            [
                "requested_roll_value",
                "requested_reduction_amount",
                "requested_reduction_pct",
                "included_comp_count",
                "excluded_review_heavy_count",
                "excluded_likely_exclude_count",
            ]
        )
    missing = [field for field in required if row.get(field) is None]
    return tuple(missing)


def _classify_defect_category(row: dict[str, Any]) -> str:
    if row.get("discovery_completion_status") != "completed":
        return "runtime_or_discovery_failure"
    if row.get("probe_error"):
        return "runtime_or_discovery_failure"
    attachment_status = row.get("downstream_payload_attachment_status")
    if attachment_status == "missing_in_replay_source":
        return "downstream_replay_payload_not_generated"
    if attachment_status == "emitted_partial_source_payload":
        return "downstream_replay_payload_partial_source_emitted"
    if attachment_status == "replay_tables_unavailable":
        return "downstream_replay_tables_unavailable"
    if attachment_status == "replay_source_error":
        return "downstream_replay_source_error"
    if row.get("subject_identifier") is None:
        return "missing_subject_identifier"
    if row.get("county") is None or row.get("current_appraised_value") is None:
        return "subject_snapshot_incomplete"
    return "missing_downstream_replay_payload"
