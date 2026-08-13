from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path
from typing import Any


MODEL_OUTCOME_STATUSES = {
    "supported",
    "supported_with_review",
    "manual_review_required",
    "unsupported",
}
FINAL_VALUE_REVIEW_PAYLOAD_VERSION = "unequal_roll_final_value_review_payload_v1"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Emit a canonical producer-side downstream payload per subject row "
            "for unequal-roll runtime probe artifacts."
        )
    )
    parser.add_argument("--input", required=True, help="Input runtime probe JSON path.")
    parser.add_argument(
        "--output", required=True, help="Output JSON path with producer payloads emitted."
    )
    parser.add_argument(
        "--chunk-artifacts-glob",
        default="",
        help=(
            "Optional glob of chunk validation artifacts. When provided, producer "
            "payload emission uses same-lineage chunk rows as first-party per-subject "
            "resolution sources."
        ),
    )
    args = parser.parse_args()

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    chunk_subject_map = _load_chunk_subject_map(args.chunk_artifacts_glob)
    subjects = payload.get("subjects", [])
    for row in subjects:
        row["producer_downstream_payload"] = _build_producer_downstream_payload(
            row, chunk_subject_map=chunk_subject_map
        )

    payload["producer_payload_contract_version"] = "unequal_roll_downstream_payload_v2"
    Path(args.output).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_producer_downstream_payload(
    row: dict[str, Any],
    *,
    chunk_subject_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    account = row.get("subject_identifier")
    chunk_row = chunk_subject_map.get(str(account)) if account is not None else None

    final_value_status = _field_value("final_value_status", row, chunk_row)
    downstream_attachment_status = (
        "producer_full_payload_emitted"
        if final_value_status in MODEL_OUTCOME_STATUSES
        else "producer_partial_payload_emitted"
    )
    explicit_defect_status = (
        None
        if final_value_status in MODEL_OUTCOME_STATUSES
        else "downstream_replay_payload_partial_source_emitted"
    )

    return {
        "subject_identifier": account,
        "county": row.get("county"),
        "neighborhood": row.get("neighborhood"),
        "current_appraised_value": row.get("current_appraised_value"),
        "final_value_status": final_value_status,
        "explicit_defect_status": explicit_defect_status,
        "requested_roll_value": _field_value("requested_roll_value", row, chunk_row),
        "requested_reduction_amount": _field_value(
            "requested_reduction_amount", row, chunk_row
        ),
        "requested_reduction_pct": _field_value("requested_reduction_pct", row, chunk_row),
        "included_comp_count": _field_value("included_comp_count", row, chunk_row),
        "excluded_review_heavy_count": _field_value(
            "excluded_review_heavy_count", row, chunk_row
        ),
        "excluded_likely_exclude_count": _field_value(
            "excluded_likely_exclude_count", row, chunk_row
        ),
        "discovery_completion_status": row.get("discovery_completion_status"),
        "probe_error": row.get("probe_error"),
        "compact_final_value_review_payload": _compact_final_value_review_payload(
            row, chunk_row
        ),
        "downstream_payload_attachment_status": downstream_attachment_status,
        "lineage_source_chunk": row.get("source_chunk"),
        "lineage_source_artifact": chunk_row.get("__source_artifact") if chunk_row else None,
    }


def _load_chunk_subject_map(artifacts_glob: str) -> dict[str, dict[str, Any]]:
    if not artifacts_glob:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for artifact_path in sorted(glob.glob(artifacts_glob)):
        path = Path(artifact_path)
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("subjects", []):
            normalized = _normalize_chunk_row(row)
            account = normalized.get("subject_identifier")
            if account is None:
                continue
            account_key = str(account)
            normalized["__source_artifact"] = str(path)
            existing = mapping.get(account_key, {})
            if existing.get("final_value_status") is None and normalized.get(
                "final_value_status"
            ) is not None:
                mapping[account_key] = normalized
            elif account_key not in mapping:
                mapping[account_key] = normalized
    return mapping


def _normalize_chunk_row(row: dict[str, Any]) -> dict[str, Any]:
    result = row.get("result") or {}
    subject = row.get("subject") or {}
    return {
        "subject_identifier": row.get("subject_identifier", subject.get("account_number")),
        "final_value_status": row.get("final_value_status", result.get("final_value_status")),
        "requested_roll_value": row.get("requested_roll_value", result.get("requested_roll_value")),
        "requested_reduction_amount": row.get(
            "requested_reduction_amount", result.get("requested_reduction_amount")
        ),
        "requested_reduction_pct": row.get(
            "requested_reduction_pct", result.get("requested_reduction_pct")
        ),
        "included_comp_count": row.get("included_comp_count", result.get("included_comp_count")),
        "excluded_review_heavy_count": row.get(
            "excluded_review_heavy_count", result.get("excluded_review_heavy_count")
        ),
        "excluded_likely_exclude_count": row.get(
            "excluded_likely_exclude_count", result.get("excluded_likely_exclude_count")
        ),
        "final_value_detail_json": row.get(
            "final_value_detail_json", result.get("final_value_detail_json")
        ),
        "selection_log_json": row.get("selection_log_json", result.get("selection_log_json")),
        "summary_json": row.get("summary_json", result.get("summary_json")),
        "compact_final_value_review_payload": row.get(
            "compact_final_value_review_payload",
            result.get("compact_final_value_review_payload"),
        ),
    }


def _field_value(
    key: str, runtime_row: dict[str, Any], chunk_row: dict[str, Any] | None
) -> Any:
    runtime_value = runtime_row.get(key)
    if runtime_value is not None:
        return runtime_value
    if chunk_row is not None:
        return chunk_row.get(key)
    return None


def _compact_final_value_review_payload(
    runtime_row: dict[str, Any],
    chunk_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    for source_name, source_row in (
        ("runtime_row", runtime_row),
        ("chunk_row", chunk_row or {}),
    ):
        payload = source_row.get("compact_final_value_review_payload")
        if isinstance(payload, dict):
            compact = dict(payload)
            compact.setdefault("detail_source", f"{source_name}.compact_final_value_review_payload")
            compact.setdefault("review_payload_version", FINAL_VALUE_REVIEW_PAYLOAD_VERSION)
            return compact

        detail_json = source_row.get("final_value_detail_json")
        if isinstance(detail_json, dict):
            return _compact_from_final_value_detail_json(
                detail_json, detail_source=f"{source_name}.final_value_detail_json"
            )

        selection_log_json = source_row.get("selection_log_json")
        if isinstance(selection_log_json, dict):
            final_value_log = dict(selection_log_json.get("final_value") or {})
            if final_value_log:
                return _compact_from_selection_log_final_value(
                    final_value_log,
                    detail_source=f"{source_name}.selection_log_json.final_value",
                )
    return None


def _compact_from_final_value_detail_json(
    detail_json: dict[str, Any],
    *,
    detail_source: str,
) -> dict[str, Any]:
    included_rows = [
        _compact_final_value_comp_row(row)
        for row in list(detail_json.get("included_comp_rows") or [])
    ]
    excluded_rows = [
        _compact_final_value_comp_row(row)
        for row in list(detail_json.get("excluded_comp_rows") or [])
    ]
    ordered_adjusted_values = [
        _compact_ordered_adjusted_value_row(row)
        for row in list(detail_json.get("ordered_adjusted_values") or [])
    ]
    stability_metrics = dict(detail_json.get("stability_metrics") or {})

    similarity_present = any(
        row.get("similarity_score") is not None
        for row in included_rows + excluded_rows
    ) or any(
        row.get("similarity_score") is not None for row in ordered_adjusted_values
    )
    line_item_summary_present = any(
        row.get("line_item_summary") is not None for row in included_rows + excluded_rows
    )

    return {
        "review_payload_version": FINAL_VALUE_REVIEW_PAYLOAD_VERSION,
        "payload_status": "full_from_final_value_detail",
        "detail_source": detail_source,
        "final_value_status": detail_json.get("final_value_status"),
        "final_value_set_summary": dict(detail_json.get("final_value_set_summary") or {}),
        "median_calculation": dict(detail_json.get("median_calculation") or {}),
        "ordered_adjusted_values": ordered_adjusted_values,
        "included_comp_rows": included_rows,
        "excluded_comp_rows": excluded_rows,
        "stability_metrics": stability_metrics,
        "qa_flags": dict(detail_json.get("qa_flags") or {}),
        "methodology_guardrails": dict(detail_json.get("methodology_guardrails") or {}),
        "carried_forward_governance": dict(
            detail_json.get("carried_forward_governance") or {}
        ),
        "availability": {
            "included_comp_rows_available": bool(included_rows),
            "excluded_comp_rows_available": bool(excluded_rows),
            "ordered_adjusted_values_available": bool(ordered_adjusted_values),
            "stability_metrics_available": bool(stability_metrics),
            "comp_identity_available": any(
                bool(row.get("candidate_parcel_id") or row.get("unequal_roll_candidate_id"))
                for row in included_rows + excluded_rows
            ),
            "comp_address_available": any(
                row.get("address") is not None for row in included_rows + excluded_rows
            ),
            "similarity_score_available": similarity_present,
            "raw_appraised_value_available": any(
                row.get("raw_appraised_value") is not None
                for row in included_rows + excluded_rows
            ),
            "adjusted_appraised_value_available": any(
                row.get("adjusted_appraised_value") is not None
                for row in included_rows + excluded_rows
            ),
            "line_item_summary_available": line_item_summary_present,
        },
        "missing_fields": [
            name
            for name, present in {
                "similarity_score": similarity_present,
                "line_item_summary": line_item_summary_present,
            }.items()
            if not present
        ],
    }


def _compact_from_selection_log_final_value(
    final_value_log: dict[str, Any],
    *,
    detail_source: str,
) -> dict[str, Any]:
    stability_metrics = dict(final_value_log.get("stability_metrics") or {})
    return {
        "review_payload_version": FINAL_VALUE_REVIEW_PAYLOAD_VERSION,
        "payload_status": "partial_from_selection_log",
        "detail_source": detail_source,
        "final_value_status": final_value_log.get("final_value_status"),
        "final_value_set_summary": dict(final_value_log.get("final_value_set_summary") or {}),
        "median_calculation": dict(final_value_log.get("median_calculation") or {}),
        "ordered_adjusted_values": [],
        "included_comp_rows": [],
        "excluded_comp_rows": [],
        "stability_metrics": stability_metrics,
        "qa_flags": dict(final_value_log.get("qa_flags") or {}),
        "methodology_guardrails": dict(final_value_log.get("methodology_guardrails") or {}),
        "carried_forward_governance": {},
        "availability": {
            "included_comp_rows_available": False,
            "excluded_comp_rows_available": False,
            "ordered_adjusted_values_available": False,
            "stability_metrics_available": bool(stability_metrics),
            "comp_identity_available": False,
            "comp_address_available": False,
            "similarity_score_available": False,
            "raw_appraised_value_available": False,
            "adjusted_appraised_value_available": False,
            "line_item_summary_available": False,
        },
        "missing_fields": [
            "included_comp_rows",
            "excluded_comp_rows",
            "ordered_adjusted_values",
            "similarity_score",
            "raw_appraised_value",
            "adjusted_appraised_value",
            "line_item_summary",
        ],
    }


def _compact_final_value_comp_row(row: dict[str, Any]) -> dict[str, Any]:
    line_items = list(row.get("line_items") or [])
    return {
        "unequal_roll_candidate_id": row.get("unequal_roll_candidate_id"),
        "candidate_parcel_id": row.get("candidate_parcel_id"),
        "address": row.get("address"),
        "chosen_comp_status": row.get("chosen_comp_status"),
        "chosen_comp_position": row.get("chosen_comp_position"),
        "final_comp_status": row.get("final_value_status"),
        "review_visible_flag": row.get("review_visible_flag"),
        "acceptable_zone_admitted_flag": bool(
            (row.get("acceptable_zone_governance") or {}).get(
                "acceptable_zone_admitted_flag"
            )
        ),
        "similarity_score": row.get("similarity_score"),
        "raw_appraised_value": row.get("raw_appraised_value"),
        "raw_appraised_value_per_sf": row.get("raw_appraised_value_per_sf"),
        "adjusted_appraised_value": row.get("adjusted_appraised_value"),
        "adjusted_appraised_value_per_sf": row.get("adjusted_appraised_value_per_sf"),
        "adjustment_math_status": row.get("adjustment_math_status"),
        "adjusted_set_governance_status": row.get("adjusted_set_governance_status"),
        "adjusted_set_governance_reason_codes": list(
            row.get("adjusted_set_governance_reason_codes") or []
        ),
        "burden_governance_status": row.get("burden_governance_status"),
        "burden_governance_reason_codes": list(
            row.get("burden_governance_reason_codes") or []
        ),
        "source_governance_status": row.get("source_governance_status"),
        "review_carry_forward_flag": bool(row.get("review_carry_forward_flag")),
        "hybrid_supported_source_flag": bool(row.get("hybrid_supported_source_flag")),
        "unresolved_review_only_channel_count": row.get(
            "unresolved_review_only_channel_count"
        ),
        "material_adjustment_count": row.get("material_adjustment_count"),
        "adjustment_pct_of_raw_value": row.get("adjustment_pct_of_raw_value"),
        "dominant_adjustment_channel": row.get("dominant_adjustment_channel"),
        "exclusion_reason_code": row.get("exclusion_reason_code"),
        "conflict_divergence_governance": dict(
            row.get("conflict_divergence_governance") or {}
        ),
        "bathroom_boundary_context": dict(row.get("bathroom_boundary_context") or {}),
        "line_item_summary": _line_item_summary(line_items),
    }


def _compact_ordered_adjusted_value_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "final_value_position": row.get("final_value_position"),
        "unequal_roll_candidate_id": row.get("unequal_roll_candidate_id"),
        "candidate_parcel_id": row.get("candidate_parcel_id"),
        "address": row.get("address"),
        "chosen_comp_status": row.get("chosen_comp_status"),
        "review_visible_flag": row.get("review_visible_flag"),
        "acceptable_zone_admitted_flag": row.get("acceptable_zone_admitted_flag"),
        "similarity_score": row.get("similarity_score"),
        "adjusted_appraised_value": row.get("adjusted_appraised_value"),
        "adjusted_appraised_value_per_sf": row.get("adjusted_appraised_value_per_sf"),
    }


def _line_item_summary(line_items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not line_items:
        return None
    channels: dict[str, int] = {}
    total_absolute_adjustment = 0.0
    for line_item in line_items:
        adjustment_type = str(line_item.get("adjustment_type") or "")
        if adjustment_type:
            channels[adjustment_type] = channels.get(adjustment_type, 0) + 1
        amount = line_item.get("signed_adjustment_amount")
        if isinstance(amount, (int, float)):
            total_absolute_adjustment += abs(float(amount))
    return {
        "line_item_count": len(line_items),
        "total_absolute_adjustment": round(total_absolute_adjustment, 2),
        "channel_counts": channels,
    }


if __name__ == "__main__":
    main()
