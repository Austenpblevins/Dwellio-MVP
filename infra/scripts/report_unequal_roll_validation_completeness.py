from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.unequal_roll_validation_completeness import (
    classify_subject_output,
    summarize_completeness,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Classify unequal-roll replay outputs so silent unavailable rows become "
            "explicit defect categories for completeness-gated validation."
        )
    )
    parser.add_argument(
        "--artifacts",
        nargs="+",
        required=True,
        help="One or more chunk artifact JSON paths to classify.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSON path for the classified completeness report.",
    )
    parser.add_argument(
        "--run-state",
        default="/private/tmp/unequal_roll_chunk_state.json",
        help=(
            "Optional run-state JSON path used to attach downstream replay payloads "
            "by account when subject rows are missing final-value fields."
        ),
    )
    args = parser.parse_args()

    subject_rows: list[dict[str, Any]] = []
    source_artifacts: list[str] = []
    run_state_map = _load_run_state_map(Path(args.run_state))

    for artifact in args.artifacts:
        path = Path(artifact)
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_artifacts.append(str(path))
        chunk_number = payload.get("chunk_metadata", {}).get("chunk_number")
        for row in payload.get("subjects", []):
            enriched = dict(row)
            _attach_downstream_replay_payload(enriched, run_state_map=run_state_map)
            classification = classify_subject_output(enriched)
            enriched["completeness_status_code"] = classification.status_code
            enriched["completeness_status_family"] = classification.status_family
            enriched["completeness_gate_pass"] = classification.completeness_gate_pass
            enriched["completeness_defect_category"] = classification.defect_category
            enriched["missing_required_fields"] = list(
                classification.missing_required_fields
            )
            enriched["source_chunk_number"] = chunk_number
            enriched["source_artifact"] = str(path)
            subject_rows.append(enriched)

    report = {
        "source_artifacts": source_artifacts,
        "summary": summarize_completeness(subject_rows),
        "subjects": subject_rows,
    }
    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")


def _load_run_state_map(run_state_path: Path) -> dict[str, dict[str, Any]]:
    if not run_state_path.exists():
        return {}
    payload = json.loads(run_state_path.read_text(encoding="utf-8"))
    mapping: dict[str, dict[str, Any]] = {}
    for run in payload.get("runs", []):
        summary = run.get("summary", {})
        account = summary.get("account")
        if account:
            mapping[str(account)] = run
    return mapping


def _attach_downstream_replay_payload(
    row: dict[str, Any], *, run_state_map: dict[str, dict[str, Any]]
) -> None:
    if row.get("final_value_status") is not None:
        row["downstream_payload_attachment_status"] = "already_attached"
        return

    account = row.get("subject_identifier")
    run_payload = run_state_map.get(str(account)) if account is not None else None
    if run_payload is None:
        row["downstream_payload_attachment_status"] = "missing_in_replay_source"
        return

    summary = run_payload.get("summary", {})
    row["final_value_status"] = summary.get("final_value_status")
    row["requested_reduction_amount"] = summary.get("requested_reduction_amount")
    row["included_comp_count"] = summary.get("included_count")
    row["excluded_review_heavy_count"] = summary.get("excluded_review_heavy_count")
    row["excluded_likely_exclude_count"] = summary.get(
        "excluded_likely_exclude_count", row.get("excluded_likely_exclude_count")
    )

    if row.get("requested_reduction_amount") is not None and row.get(
        "current_appraised_value"
    ) is not None:
        current_value = float(row["current_appraised_value"])
        reduction_amount = float(row["requested_reduction_amount"])
        row["requested_roll_value"] = round(current_value - reduction_amount, 2)
        row["requested_reduction_pct"] = (
            round(reduction_amount / current_value, 6) if current_value else None
        )

    row["downstream_payload_attachment_status"] = (
        "attached_from_run_state"
        if row.get("final_value_status") is not None
        else "missing_in_replay_source"
    )


if __name__ == "__main__":
    main()
