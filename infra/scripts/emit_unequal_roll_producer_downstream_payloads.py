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

    payload["producer_payload_contract_version"] = "unequal_roll_downstream_payload_v1"
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


if __name__ == "__main__":
    main()
