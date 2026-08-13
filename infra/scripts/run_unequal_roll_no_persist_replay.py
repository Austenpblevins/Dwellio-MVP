from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from app.services.unequal_roll_no_persist_replay import (
    UnequalRollNoPersistReplayService,
    UnequalRollReplayRequest,
    subject_requests_from_runtime_artifact,
)


HIGH_PRIORITY_SLICE = [
    ("harris", "0411050000081"),
    ("harris", "0411050000080"),
    ("harris", "0411050000070"),
    ("harris", "0411050000071"),
    ("harris", "0411050000077"),
    ("harris", "0642370000003"),
    ("fort_bend", "0226-00-000-0010-906"),
    ("fort_bend", "0226-00-000-0470-906"),
    ("fort_bend", "0226-00-000-0050-906"),
    ("fort_bend", "4850-00-014-2300-907"),
    ("fort_bend", "0044-00-000-0280-901"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run no-persist unequal-roll replay against Stage 21 source data."
    )
    parser.add_argument(
        "--database-url",
        required=True,
        help="Stage 21 database URL. This script uses read-only source reads only.",
    )
    parser.add_argument(
        "--requested-tax-year",
        type=int,
        default=2025,
        help="Requested tax year for the replay cohort.",
    )
    parser.add_argument(
        "--full100-artifact",
        default="/private/tmp/unequal_roll_stage21_full100_runtime_probe_timeout120s_foundation.json",
        help="Frozen full-100 cohort artifact used to source the subject list.",
    )
    parser.add_argument(
        "--output-dir",
        default="/private/tmp",
        help="Directory for replay and review artifacts.",
    )
    parser.add_argument(
        "--statement-timeout",
        default="120s",
        help="Per-subject statement_timeout.",
    )
    parser.add_argument(
        "--max-parallel-workers-per-gather",
        type=int,
        default=0,
        help="Per-subject max_parallel_workers_per_gather.",
    )
    parser.add_argument(
        "--skip-full100",
        action="store_true",
        help="Only run the high-priority slice.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_dir = Path(args.output_dir)
    service = UnequalRollNoPersistReplayService()

    slice_requests = [
        UnequalRollReplayRequest(
            county_id=county,
            account_number=account,
            requested_tax_year=args.requested_tax_year,
        )
        for county, account in HIGH_PRIORITY_SLICE
    ]
    slice_result = run_request_batch(
        service=service,
        database_url=args.database_url,
        requests=slice_requests,
        artifact_prefix=output_dir / f"unequal_roll_stage21_no_persist_slice_{timestamp}",
        statement_timeout=args.statement_timeout,
        max_parallel_workers_per_gather=args.max_parallel_workers_per_gather,
        source_label="high_priority_slice",
    )

    full_result = None
    if not args.skip_full100 and slice_result["summary"]["compact_review_payload_rows_available"] > 0:
        full_requests = subject_requests_from_runtime_artifact(
            args.full100_artifact,
            requested_tax_year=args.requested_tax_year,
        )
        full_result = run_request_batch(
            service=service,
            database_url=args.database_url,
            requests=full_requests,
            artifact_prefix=output_dir / f"unequal_roll_stage21_no_persist_full100_{timestamp}",
            statement_timeout=args.statement_timeout,
            max_parallel_workers_per_gather=args.max_parallel_workers_per_gather,
            source_label="frozen_full100_cohort",
        )

    manifest = {
        "timestamp": timestamp,
        "slice_artifacts": slice_result["artifact_paths"],
        "full100_artifacts": full_result["artifact_paths"] if full_result is not None else None,
        "slice_summary": slice_result["summary"],
        "full100_summary": full_result["summary"] if full_result is not None else None,
    }
    manifest_path = output_dir / f"unequal_roll_stage21_no_persist_manifest_{timestamp}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(manifest_path)


def run_request_batch(
    *,
    service: UnequalRollNoPersistReplayService,
    database_url: str,
    requests: list[UnequalRollReplayRequest],
    artifact_prefix: Path,
    statement_timeout: str,
    max_parallel_workers_per_gather: int,
    source_label: str,
) -> dict[str, Any]:
    output_dir = artifact_prefix.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    replay_json_path = Path(f"{artifact_prefix}_replay.json")
    review_json_path = Path(f"{artifact_prefix}_review_evidence.json")
    review_csv_path = Path(f"{artifact_prefix}_review_evidence.csv")
    review_md_path = Path(f"{artifact_prefix}_review_summary.md")

    subjects: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    repeated_blocker_code: str | None = None
    repeated_blocker_count = 0

    connection = service.connect_read_only(database_url)
    try:
        for request in requests:
            try:
                connection.execute("BEGIN READ ONLY")
                with connection.cursor() as cursor:
                    row = service.replay_subject(
                        cursor,
                        request=request,
                        statement_timeout=statement_timeout,
                        max_parallel_workers_per_gather=max_parallel_workers_per_gather,
                    )
                connection.rollback()
                subjects.append(row)
                if row.get("replay_status") == "blocked":
                    blocker_code = str(row.get("failure_code") or "")
                    if blocker_code == repeated_blocker_code:
                        repeated_blocker_count += 1
                    else:
                        repeated_blocker_code = blocker_code
                        repeated_blocker_count = 1
                else:
                    repeated_blocker_code = None
                    repeated_blocker_count = 0
            except Exception as exc:
                connection.rollback()
                failure = {
                    "account": request.account_number,
                    "county": request.county_id,
                    "requested_tax_year": request.requested_tax_year,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
                failures.append(failure)
                subjects.append(
                    {
                        "account": request.account_number,
                        "county": request.county_id,
                        "requested_tax_year": request.requested_tax_year,
                        "replay_status": "error",
                        "failure_code": type(exc).__name__,
                        "failure_reason": str(exc),
                        "evidence_completeness_grade": "not_reviewable",
                        "compact_final_value_review_payload": None,
                        "stability_metrics_available": False,
                    }
                )
                repeated_blocker_code = None
                repeated_blocker_count = 0
            write_artifacts(
                subjects=subjects,
                failures=failures,
                replay_json_path=replay_json_path,
                review_json_path=review_json_path,
                review_csv_path=review_csv_path,
                review_md_path=review_md_path,
                source_label=source_label,
                statement_timeout=statement_timeout,
                max_parallel_workers_per_gather=max_parallel_workers_per_gather,
            )
            if repeated_blocker_code == "subject_snapshot_query_timeout" and repeated_blocker_count >= 2:
                failures.append(
                    {
                        "account": None,
                        "county": None,
                        "requested_tax_year": None,
                        "error_type": "RepeatedBlockerStop",
                        "error_message": (
                            "stopped early after repeated subject_snapshot_query_timeout "
                            "failures under the configured read-only profile"
                        ),
                    }
                )
                write_artifacts(
                    subjects=subjects,
                    failures=failures,
                    replay_json_path=replay_json_path,
                    review_json_path=review_json_path,
                    review_csv_path=review_csv_path,
                    review_md_path=review_md_path,
                    source_label=source_label,
                    statement_timeout=statement_timeout,
                    max_parallel_workers_per_gather=max_parallel_workers_per_gather,
                )
                break
    finally:
        connection.close()

    review_evidence = build_review_evidence(subjects=subjects, failures=failures, source_label=source_label)
    return {
        "summary": review_evidence["summary"],
        "artifact_paths": {
            "replay_json": str(replay_json_path),
            "review_json": str(review_json_path),
            "review_csv": str(review_csv_path),
            "review_md": str(review_md_path),
        },
    }


def write_artifacts(
    *,
    subjects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    replay_json_path: Path,
    review_json_path: Path,
    review_csv_path: Path,
    review_md_path: Path,
    source_label: str,
    statement_timeout: str,
    max_parallel_workers_per_gather: int,
) -> None:
    replay_payload = {
        "meta": {
            "source_label": source_label,
            "subject_count_attempted": len(subjects),
            "statement_timeout": statement_timeout,
            "max_parallel_workers_per_gather": max_parallel_workers_per_gather,
            "db_read_only_mode": True,
            "db_writes_permitted": False,
        },
        "subjects": subjects,
        "failures": failures,
    }
    replay_json_path.write_text(json.dumps(replay_payload, indent=2))

    review_evidence = build_review_evidence(
        subjects=subjects,
        failures=failures,
        source_label=source_label,
    )
    review_json_path.write_text(json.dumps(review_evidence, indent=2))
    write_csv(subjects=review_evidence["subjects"], output_path=review_csv_path)
    review_md_path.write_text(build_markdown_summary(review_evidence))


def build_review_evidence(
    *,
    subjects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    source_label: str,
) -> dict[str, Any]:
    compact_rows = [
        row
        for row in subjects
        if isinstance(row.get("compact_final_value_review_payload"), dict)
    ]
    stability_rows = [
        row for row in subjects if bool(row.get("stability_metrics_available"))
    ]
    summary = {
        "source_label": source_label,
        "subject_count": len(subjects),
        "completed_count": sum(1 for row in subjects if row.get("replay_status") == "completed"),
        "blocked_count": sum(1 for row in subjects if row.get("replay_status") == "blocked"),
        "error_count": sum(1 for row in subjects if row.get("replay_status") == "error"),
        "compact_review_payload_rows_available": len(compact_rows),
        "stability_metrics_recovered_count": len(stability_rows),
        "comp_identities_available": any(
            (
                (row.get("compact_final_value_review_payload") or {})
                .get("availability", {})
                .get("comp_identity_available")
            )
            for row in compact_rows
        ),
        "similarity_scores_available": any(
            (
                (row.get("compact_final_value_review_payload") or {})
                .get("availability", {})
                .get("similarity_score_available")
            )
            for row in compact_rows
        ),
        "adjusted_values_available": any(
            (
                (row.get("compact_final_value_review_payload") or {})
                .get("availability", {})
                .get("adjusted_appraised_value_available")
            )
            for row in compact_rows
        ),
        "unsupported_diagnostic_only_count": sum(
            1 for row in subjects if row.get("value_interpretation") == "diagnostic_only"
        ),
        "evidence_grade_counts": count_values(
            row.get("evidence_completeness_grade") for row in subjects
        ),
        "failure_count": len(failures),
    }
    return {
        "summary": summary,
        "subjects": subjects,
        "focused_packets": build_focused_packets(subjects),
        "failures": failures,
    }


def build_focused_packets(subjects: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    subjects_by_account = {str(row.get("account")): row for row in subjects if row.get("account")}
    packets: dict[str, list[dict[str, Any]]] = {}
    high_priority_accounts = [account for _, account in HIGH_PRIORITY_SLICE]
    packets["high_priority_slice"] = [
        subjects_by_account[account]
        for account in high_priority_accounts
        if account in subjects_by_account
    ]
    return packets


def write_csv(*, subjects: list[dict[str, Any]], output_path: Path) -> None:
    fieldnames = [
        "account",
        "county",
        "requested_tax_year",
        "served_tax_year",
        "parcel_id",
        "address",
        "neighborhood",
        "replay_status",
        "final_value_status",
        "safe_requested_roll_value",
        "requested_roll_value",
        "safe_requested_reduction_amount",
        "requested_reduction_amount",
        "safe_requested_reduction_pct",
        "requested_reduction_pct",
        "fallback_used",
        "same_neighborhood_count",
        "fallback_count",
        "included_comp_count",
        "excluded_review_heavy_count",
        "excluded_likely_exclude_count",
        "value_interpretation",
        "evidence_completeness_grade",
        "stability_metrics_available",
        "failure_code",
    ]
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in subjects:
            writer.writerow({name: row.get(name) for name in fieldnames})


def build_markdown_summary(review_evidence: dict[str, Any]) -> str:
    summary = review_evidence["summary"]
    lines = [
        "# Unequal-Roll No-Persist Replay Review Summary",
        "",
        f"- Source label: `{summary['source_label']}`",
        f"- Subject count: `{summary['subject_count']}`",
        f"- Completed: `{summary['completed_count']}`",
        f"- Blocked: `{summary['blocked_count']}`",
        f"- Errors: `{summary['error_count']}`",
        f"- Compact review payload rows available: `{summary['compact_review_payload_rows_available']}`",
        f"- Stability metrics recovered count: `{summary['stability_metrics_recovered_count']}`",
        f"- Similarity scores available: `{summary['similarity_scores_available']}`",
        f"- Adjusted values available: `{summary['adjusted_values_available']}`",
        f"- Unsupported diagnostic-only count: `{summary['unsupported_diagnostic_only_count']}`",
        "",
        "## Evidence Grade Counts",
        "",
    ]
    for grade, count in sorted((summary.get("evidence_grade_counts") or {}).items()):
        lines.append(f"- `{grade}`: `{count}`")
    if review_evidence["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in review_evidence["failures"]:
            lines.append(
                f"- `{failure['county']}` `{failure['account']}`: `{failure['error_type']}` - {failure['error_message']}"
            )
    return "\n".join(lines) + "\n"


def count_values(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


if __name__ == "__main__":
    main()
