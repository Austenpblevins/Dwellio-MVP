from __future__ import annotations

import argparse
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
    args = parser.parse_args()

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    subjects = payload.get("subjects", [])
    for row in subjects:
        row["producer_downstream_payload"] = _build_producer_downstream_payload(row)

    payload["producer_payload_contract_version"] = "unequal_roll_downstream_payload_v1"
    Path(args.output).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_producer_downstream_payload(row: dict[str, Any]) -> dict[str, Any]:
    final_value_status = row.get("final_value_status")
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
        "subject_identifier": row.get("subject_identifier"),
        "county": row.get("county"),
        "neighborhood": row.get("neighborhood"),
        "current_appraised_value": row.get("current_appraised_value"),
        "final_value_status": final_value_status,
        "explicit_defect_status": explicit_defect_status,
        "requested_roll_value": row.get("requested_roll_value"),
        "requested_reduction_amount": row.get("requested_reduction_amount"),
        "requested_reduction_pct": row.get("requested_reduction_pct"),
        "included_comp_count": row.get("included_comp_count"),
        "excluded_review_heavy_count": row.get("excluded_review_heavy_count"),
        "excluded_likely_exclude_count": row.get("excluded_likely_exclude_count"),
        "discovery_completion_status": row.get("discovery_completion_status"),
        "probe_error": row.get("probe_error"),
        "downstream_payload_attachment_status": downstream_attachment_status,
        "lineage_source_chunk": row.get("source_chunk"),
    }


if __name__ == "__main__":
    main()
