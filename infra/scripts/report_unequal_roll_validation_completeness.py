from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any
import glob

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
    parser.add_argument(
        "--chunked-state",
        default="/private/tmp/unequal_roll_stage21_chunked_100_validation.json",
        help=(
            "Optional chunked validation JSON path used as a secondary replay payload "
            "source when run-state payloads are unavailable."
        ),
    )
    parser.add_argument(
        "--fallback-runtime-artifacts-glob",
        default="",
        help=(
            "Optional glob for per-subject runtime replay artifacts whose subject rows "
            "can be used as a tertiary payload reconstruction source."
        ),
    )
    parser.add_argument(
        "--canonical-store-input",
        default="",
        help=(
            "Optional canonical per-subject payload store JSON path. When provided, "
            "this source is consulted before run-state/chunked/fallback lookups."
        ),
    )
    parser.add_argument(
        "--canonical-store-output",
        default="",
        help=(
            "Optional output path to persist the canonical per-subject payload store "
            "generated during this classification run."
        ),
    )
    args = parser.parse_args()

    subject_rows: list[dict[str, Any]] = []
    source_artifacts: list[str] = []
    run_state_map = _load_run_state_map(Path(args.run_state))
    chunked_state_map = _load_chunked_state_map(Path(args.chunked_state))
    fallback_subject_map = _load_fallback_subject_map(
        args.fallback_runtime_artifacts_glob
    )
    canonical_store_map = _load_canonical_store_map(args.canonical_store_input)

    for artifact in args.artifacts:
        path = Path(artifact)
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_artifacts.append(str(path))
        chunk_number = payload.get("chunk_metadata", {}).get("chunk_number")
        for row in payload.get("subjects", []):
            enriched = dict(row)
            _attach_downstream_replay_payload(
                enriched,
                canonical_store_map=canonical_store_map,
                run_state_map=run_state_map,
                chunked_state_map=chunked_state_map,
                fallback_subject_map=fallback_subject_map,
            )
            classification = classify_subject_output(enriched)
            enriched["completeness_status_code"] = classification.status_code
            enriched["completeness_status_family"] = classification.status_family
            enriched["completeness_gate_pass"] = classification.completeness_gate_pass
            enriched["completeness_defect_category"] = classification.defect_category
            enriched["missing_required_fields"] = list(
                classification.missing_required_fields
            )
            enriched["canonical_downstream_summary"] = _build_canonical_downstream_summary(
                enriched
            )
            enriched["source_chunk_number"] = chunk_number
            enriched["source_artifact"] = str(path)
            subject_rows.append(enriched)
            _update_canonical_store_map(canonical_store_map, enriched)

    report = {
        "source_artifacts": source_artifacts,
        "summary": summarize_completeness(subject_rows),
        "subjects": subject_rows,
    }
    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    if args.canonical_store_output:
        Path(args.canonical_store_output).write_text(
            json.dumps({"subjects": list(canonical_store_map.values())}, indent=2),
            encoding="utf-8",
        )


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
    row: dict[str, Any],
    *,
    canonical_store_map: dict[str, dict[str, Any]],
    run_state_map: dict[str, dict[str, Any]],
    chunked_state_map: dict[str, dict[str, Any]],
    fallback_subject_map: dict[str, dict[str, Any]],
) -> None:
    if row.get("final_value_status") is not None:
        row["downstream_payload_attachment_status"] = "already_attached"
        return

    producer_payload = row.get("producer_downstream_payload")
    if isinstance(producer_payload, dict):
        for key in (
            "final_value_status",
            "requested_roll_value",
            "requested_reduction_amount",
            "requested_reduction_pct",
            "included_comp_count",
            "excluded_review_heavy_count",
            "excluded_likely_exclude_count",
            "discovery_completion_status",
            "probe_error",
            "downstream_payload_attachment_status",
        ):
            if row.get(key) is None and producer_payload.get(key) is not None:
                row[key] = producer_payload[key]
        if row.get("final_value_status") is not None:
            row["downstream_payload_attachment_status"] = "attached_from_producer_payload"
            return
        if row.get("downstream_payload_attachment_status") is None:
            row["downstream_payload_attachment_status"] = "attached_from_producer_payload"

    account = row.get("subject_identifier")
    canonical_payload = (
        canonical_store_map.get(str(account)) if account is not None else None
    )
    if canonical_payload is not None:
        for key in (
            "final_value_status",
            "requested_roll_value",
            "requested_reduction_amount",
            "requested_reduction_pct",
            "included_comp_count",
            "excluded_review_heavy_count",
            "excluded_likely_exclude_count",
            "discovery_completion_status",
            "probe_error",
        ):
            if row.get(key) is None and canonical_payload.get(key) is not None:
                row[key] = canonical_payload[key]
        if row.get("final_value_status") is not None:
            row["downstream_payload_attachment_status"] = "attached_from_canonical_store"
            return

    run_payload = run_state_map.get(str(account)) if account is not None else None
    if run_payload is not None:
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
        if row["downstream_payload_attachment_status"] == "attached_from_run_state":
            return

    chunk_payload = chunked_state_map.get(str(account)) if account is not None else None
    saw_chunk_source_error = False
    if chunk_payload is not None:
        chunk_ok = chunk_payload.get("ok")
        chunk_error = chunk_payload.get("error")
        status = chunk_payload.get("status")
        included = chunk_payload.get("included")
        if status is not None:
            row["final_value_status"] = status
        if included is not None:
            row["included_comp_count"] = included
        if chunk_ok is False:
            if not row.get("probe_error") and chunk_error:
                row["probe_error"] = chunk_error
            if row.get("discovery_completion_status") == "completed":
                row["discovery_completion_status"] = "failed"
            row["downstream_payload_attachment_status"] = "attached_from_chunked_state_error"
            return
        row["downstream_payload_attachment_status"] = "attached_from_chunked_state"
        if row["downstream_payload_attachment_status"] == "attached_from_chunked_state":
            if row.get("final_value_status") is not None:
                return
            row["downstream_payload_attachment_status"] = "replay_source_error"
            saw_chunk_source_error = True
            # Continue into tertiary fallback reconstruction path.

    account = row.get("subject_identifier")
    fallback_payload = (
        fallback_subject_map.get(str(account)) if account is not None else None
    )
    if fallback_payload is not None:
        for key in (
            "final_value_status",
            "requested_roll_value",
            "requested_reduction_amount",
            "requested_reduction_pct",
            "included_comp_count",
            "excluded_review_heavy_count",
            "excluded_likely_exclude_count",
        ):
            if row.get(key) is None and fallback_payload.get(key) is not None:
                row[key] = fallback_payload[key]
        if row.get("final_value_status") is not None:
            row["downstream_payload_attachment_status"] = "reconstructed_from_runtime_artifact"
            return

    if saw_chunk_source_error:
        row["downstream_payload_attachment_status"] = "replay_source_error"
        return

    # Source-time probe rows can still carry useful downstream context even if
    # full model payload fields were not produced in that run lineage.
    if _can_emit_partial_source_payload(row):
        row["downstream_payload_attachment_status"] = "emitted_partial_source_payload"
        return

    row["downstream_payload_attachment_status"] = "missing_in_replay_source"


def _load_chunked_state_map(chunked_state_path: Path) -> dict[str, dict[str, Any]]:
    if not chunked_state_path.exists():
        return {}
    payload = json.loads(chunked_state_path.read_text(encoding="utf-8"))
    mapping: dict[str, dict[str, Any]] = {}
    for chunk in payload.get("chunk_execution", {}).get("chunks", []):
        for row in chunk.get("rows", []):
            account = row.get("account")
            if account:
                mapping[str(account)] = {
                    "status": row.get("status"),
                    "included": row.get("included"),
                    "ok": row.get("ok"),
                    "error": row.get("error"),
                }
    return mapping


def _load_fallback_subject_map(
    artifacts_glob: str,
) -> dict[str, dict[str, Any]]:
    if not artifacts_glob:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for artifact_path in sorted(glob.glob(artifacts_glob)):
        path = Path(artifact_path)
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("subjects", []):
            candidate = _extract_fallback_candidate_payload(row)
            account = candidate.get("subject_identifier")
            if account is None:
                continue
            account_key = str(account)
            existing = mapping.get(account_key, {})
            # Prefer the first payload that has a concrete final status.
            if existing.get("final_value_status") is None and candidate.get(
                "final_value_status"
            ) is not None:
                mapping[account_key] = candidate
            elif account_key not in mapping:
                mapping[account_key] = candidate
    return mapping


def _extract_fallback_candidate_payload(row: dict[str, Any]) -> dict[str, Any]:
    account = row.get("subject_identifier")
    if account is None:
        account = (row.get("subject") or {}).get("account_number")

    result = row.get("result") or {}
    return {
        "subject_identifier": account,
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


def _build_canonical_downstream_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "subject_identifier": row.get("subject_identifier"),
        "county": row.get("county"),
        "neighborhood": row.get("neighborhood"),
        "current_appraised_value": row.get("current_appraised_value"),
        "final_value_status": row.get("final_value_status"),
        "requested_roll_value": row.get("requested_roll_value"),
        "requested_reduction_amount": row.get("requested_reduction_amount"),
        "requested_reduction_pct": row.get("requested_reduction_pct"),
        "included_comp_count": row.get("included_comp_count"),
        "excluded_review_heavy_count": row.get("excluded_review_heavy_count"),
        "excluded_likely_exclude_count": row.get("excluded_likely_exclude_count"),
        "discovery_completion_status": row.get("discovery_completion_status"),
        "probe_error": row.get("probe_error"),
        "downstream_payload_attachment_status": row.get(
            "downstream_payload_attachment_status"
        ),
        "completeness_status_code": row.get("completeness_status_code"),
        "completeness_defect_category": row.get("completeness_defect_category"),
    }


def _can_emit_partial_source_payload(row: dict[str, Any]) -> bool:
    return (
        row.get("subject_identifier") is not None
        and row.get("county") is not None
        and row.get("current_appraised_value") is not None
        and row.get("discovery_completion_status") is not None
    )


def _load_canonical_store_map(path_value: str) -> dict[str, dict[str, Any]]:
    if not path_value:
        return {}
    path = Path(path_value)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    mapping: dict[str, dict[str, Any]] = {}
    for row in payload.get("subjects", []):
        sid = row.get("subject_identifier")
        if sid is not None:
            mapping[str(sid)] = dict(row)
    return mapping


def _update_canonical_store_map(
    store_map: dict[str, dict[str, Any]],
    row: dict[str, Any],
) -> None:
    sid = row.get("subject_identifier")
    if sid is None:
        return
    key = str(sid)
    existing = store_map.get(key, {})
    candidate = _build_canonical_downstream_summary(row)
    existing_status = existing.get("final_value_status")
    candidate_status = candidate.get("final_value_status")
    if existing_status is None and candidate_status is not None:
        store_map[key] = candidate
    elif key not in store_map:
        store_map[key] = candidate


if __name__ == "__main__":
    main()
