from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


CONFLICT_FLAG_KEYS = {
    "raw_adjusted_divergence_flag",
    "adjusted_conflict_indicator_flag",
    "divergence_requires_review_flag",
    "adjusted_outlier_conflict_flag",
    "adjusted_value_outlier_flag",
    "adjusted_value_per_sf_outlier_flag",
    "strong_divergence_flag",
    "unresolved_review_only_conflict_escalation_flag",
    "review_carry_forward_unresolved_escalation_flag",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analysis-only evaluator for manual_review_required to "
            "supported_with_review governance refinement candidates."
        )
    )
    parser.add_argument(
        "--input-artifact",
        action="append",
        default=[],
        help="Review-evidence JSON artifact path. May be provided multiple times.",
    )
    parser.add_argument(
        "--scan-dir",
        default="/private/tmp",
        help="Optional directory to scan for additional review-evidence artifacts.",
    )
    parser.add_argument(
        "--output-dir",
        default="/private/tmp",
        help="Directory for analysis outputs.",
    )
    return parser.parse_args()


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_count(items: list[bool]) -> int:
    return sum(1 for item in items if item)


def _summarize_scores(included_rows: list[dict[str, Any]]) -> dict[str, float | None]:
    scores = [
        _as_float(row.get("similarity_score"))
        for row in included_rows
        if _as_float(row.get("similarity_score")) is not None
    ]
    if not scores:
        return {"min": None, "avg": None, "max": None}
    return {
        "min": round(min(scores), 4),
        "avg": round(sum(scores) / len(scores), 4),
        "max": round(max(scores), 4),
    }


def _source_counter(included_rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(row.get("source_governance_status") for row in included_rows))


def _burden_counter(included_rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(row.get("burden_governance_status") for row in included_rows))


def _reason_counter(included_rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in included_rows:
        for reason in row.get("adjusted_set_governance_reason_codes") or []:
            counter[reason] += 1
    return dict(counter)


def _fort_bend_attachment_counter(included_rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in included_rows:
        attachment_status = (
            ((row.get("bathroom_boundary_context") or {}).get("fort_bend_bathroom_modifier") or {})
        ).get("attachment_status")
        if attachment_status is not None:
            counter[attachment_status] += 1
    return dict(counter)


def _conflict_metrics(included_rows: list[dict[str, Any]]) -> dict[str, Any]:
    affected_flags: Counter[str] = Counter()
    affected_comp_count = 0
    for row in included_rows:
        governance = row.get("conflict_divergence_governance") or {}
        row_affected = False
        for key in CONFLICT_FLAG_KEYS:
            if governance.get(key) is True:
                affected_flags[key] += 1
                row_affected = True
        if row_affected:
            affected_comp_count += 1
    included_count = len(included_rows)
    affected_ratio = (affected_comp_count / included_count) if included_count else None
    return {
        "affected_comp_count": affected_comp_count,
        "affected_ratio": affected_ratio,
        "flag_counts": dict(affected_flags),
    }


def _mostly_within_thresholds(included_rows: list[dict[str, Any]]) -> tuple[bool, float | None]:
    included_count = len(included_rows)
    if not included_count:
        return False, None
    within_count = sum(
        1 for row in included_rows if row.get("burden_governance_status") == "within_thresholds"
    )
    ratio = within_count / included_count
    return ratio >= 0.8, ratio


def _fort_bend_exceptionally_stable(
    *,
    median_all: float | None,
    max_leave_one_out_delta: float | None,
    adjusted_value_iqr: float | None,
) -> bool:
    if median_all is None or max_leave_one_out_delta is None or adjusted_value_iqr is None:
        return False
    loo_threshold = min(median_all * 0.03, 7500.0)
    iqr_threshold = median_all * 0.35
    return max_leave_one_out_delta <= loo_threshold and adjusted_value_iqr <= iqr_threshold


def classify_subject(subject: dict[str, Any]) -> dict[str, Any]:
    compact_payload = subject.get("compact_final_value_review_payload") or {}
    set_summary = compact_payload.get("final_value_set_summary") or {}
    stability = compact_payload.get("stability_metrics") or {}
    included_rows = list(compact_payload.get("included_comp_rows") or [])
    current_status = subject.get("final_value_status")
    included_count = int(set_summary.get("included_count") or 0)
    review_heavy_count = int(set_summary.get("excluded_review_heavy_count") or 0)
    likely_exclude_count = int(set_summary.get("excluded_likely_exclude_count") or 0)
    current_value = _as_float(subject.get("current_appraised_value"))
    safe_requested_roll_value = _as_float(subject.get("safe_requested_roll_value"))
    safe_requested_reduction_amount = _as_float(subject.get("safe_requested_reduction_amount"))
    safe_requested_reduction_pct = _as_float(subject.get("safe_requested_reduction_pct"))
    median_all = _as_float(stability.get("median_all"))
    max_leave_one_out_delta = _as_float(stability.get("max_leave_one_out_delta"))
    adjusted_value_iqr = _as_float(stability.get("adjusted_value_iqr"))
    max_adjustment_pct = _as_float(stability.get("max_adjustment_pct"))
    no_reduction_requested = (safe_requested_reduction_amount or 0.0) <= 0.0
    positive_reduction_requested = (safe_requested_reduction_amount or 0.0) > 0.0
    iqr_ratio = (
        (adjusted_value_iqr / median_all)
        if adjusted_value_iqr is not None and median_all not in {None, 0.0}
        else None
    )
    loo_threshold = (
        min(median_all * 0.06, 15000.0)
        if median_all not in {None, 0.0}
        else None
    )
    review_heavy_ratio = (review_heavy_count / included_count) if included_count else None
    conflict_metrics = _conflict_metrics(included_rows)
    burden_ok, burden_within_ratio = _mostly_within_thresholds(included_rows)
    source_counts = _source_counter(included_rows)
    burden_counts = _burden_counter(included_rows)
    reason_counts = _reason_counter(included_rows)
    fort_bend_attachment_counts = _fort_bend_attachment_counter(included_rows)
    all_review_visible = bool(set_summary.get("all_included_review_visible_flag"))
    unresolved_source_present = any(
        key and "unresolved_review_only" in key for key in source_counts.keys()
    )
    fort_bend_exceptional = _fort_bend_exceptionally_stable(
        median_all=median_all,
        max_leave_one_out_delta=max_leave_one_out_delta,
        adjusted_value_iqr=adjusted_value_iqr,
    )

    checks: dict[str, bool] = {
        "current_status_manual_review_required": current_status == "manual_review_required",
        "included_comp_count_gte_12": included_count >= 12,
        "likely_exclude_count_zero": likely_exclude_count == 0,
        "review_heavy_count_lte_2": review_heavy_count <= 2,
        "review_heavy_ratio_lte_15pct": (
            review_heavy_ratio is not None and review_heavy_ratio <= 0.15
        ),
        "burden_mostly_within_thresholds": burden_ok,
        "max_leave_one_out_delta_within_limit": (
            loo_threshold is not None
            and max_leave_one_out_delta is not None
            and max_leave_one_out_delta <= loo_threshold
        ),
        "adjusted_value_iqr_within_limit": (
            iqr_ratio is not None and iqr_ratio <= 0.55
        ),
        "max_adjustment_pct_within_limit_or_no_reduction": (
            no_reduction_requested
            or (
                max_adjustment_pct is not None
                and max_adjustment_pct <= 0.15
            )
        ),
        "conflict_divergence_within_limit": (
            conflict_metrics["affected_ratio"] is not None
            and conflict_metrics["affected_ratio"] <= 0.20
            and conflict_metrics["affected_comp_count"] <= 3
        ),
        "source_uncertainty_visible": all_review_visible and unresolved_source_present,
    }

    if subject.get("county") == "fort_bend":
        checks["fort_bend_caveat_gate"] = no_reduction_requested or fort_bend_exceptional
    else:
        checks["fort_bend_caveat_gate"] = True

    rejected_checks = [name for name, passed in checks.items() if not passed]
    qualifies = all(checks.values())

    review_driver_flags = {
        "source_uncertainty": all_review_visible and unresolved_source_present,
        "review_carry_forward": reason_counts.get("review_carry_forward_requires_review_visibility", 0)
        > 0,
        "burden": not burden_ok or review_heavy_count > 2 or likely_exclude_count > 0,
        "conflict_divergence": conflict_metrics["affected_comp_count"] > 0,
        "instability": not checks["max_leave_one_out_delta_within_limit"]
        or not checks["adjusted_value_iqr_within_limit"],
        "thin_support": included_count < 12,
    }
    primary_drivers = [name for name, flagged in review_driver_flags.items() if flagged]

    if not qualifies:
        safety_label = "unsafe"
        proposed_status = current_status
        reason = "rule_rejected"
    elif positive_reduction_requested:
        safety_label = "borderline"
        proposed_status = "would_be_supported_with_review_candidate"
        reason = "qualifies_base_rule_but_positive_reduction_requires_stricter_review"
    else:
        safety_label = "safe"
        proposed_status = "would_be_supported_with_review_candidate"
        reason = "qualifies_base_rule_for_no_reduction_or_exceptionally_stable_case"

    return {
        "account": subject.get("account"),
        "county": subject.get("county"),
        "address": subject.get("address"),
        "current_status": current_status,
        "analysis_only_proposed_status": proposed_status,
        "qualifies_base_rule": qualifies,
        "qualification_outcome": reason,
        "safety_label": safety_label,
        "no_reduction_case": no_reduction_requested,
        "positive_reduction_case": positive_reduction_requested,
        "current_appraised_value": current_value,
        "safe_requested_roll_value": safe_requested_roll_value,
        "safe_requested_reduction_amount": safe_requested_reduction_amount,
        "safe_requested_reduction_pct": safe_requested_reduction_pct,
        "included_comp_count": included_count,
        "review_heavy_exclusion_count": review_heavy_count,
        "review_heavy_exclusion_ratio": review_heavy_ratio,
        "likely_exclude_count": likely_exclude_count,
        "median_all": median_all,
        "max_leave_one_out_delta": max_leave_one_out_delta,
        "max_leave_one_out_threshold": loo_threshold,
        "adjusted_value_iqr": adjusted_value_iqr,
        "adjusted_value_iqr_ratio": iqr_ratio,
        "max_adjustment_pct": max_adjustment_pct,
        "similarity_score_summary": _summarize_scores(included_rows),
        "burden_status_counts": burden_counts,
        "source_posture_counts": source_counts,
        "reason_code_counts": reason_counts,
        "conflict_metrics": conflict_metrics,
        "all_included_review_visible_flag": all_review_visible,
        "fort_bend_bathroom_attachment_counts": fort_bend_attachment_counts,
        "fort_bend_exceptionally_stable": fort_bend_exceptional,
        "rule_checks": checks,
        "rejected_checks": rejected_checks,
        "primary_review_driver_flags": review_driver_flags,
        "primary_review_drivers": primary_drivers,
        "source_bathroom_caveat": _source_bathroom_caveat(
            county=subject.get("county"),
            source_counts=source_counts,
            fort_bend_attachment_counts=fort_bend_attachment_counts,
        ),
        "artifact_evidence_grade": subject.get("evidence_completeness_grade"),
    }


def _source_bathroom_caveat(
    *,
    county: str | None,
    source_counts: dict[str, int],
    fort_bend_attachment_counts: dict[str, int],
) -> str:
    unresolved = any(key and "unresolved_review_only" in key for key in source_counts.keys())
    if county == "fort_bend":
        if unresolved and fort_bend_attachment_counts:
            return "fort_bend_unresolved_bathroom_or_source_posture_remains_review_visible"
        if unresolved:
            return "fort_bend_unresolved_source_posture_remains_review_visible"
    if unresolved:
        return "unresolved_source_posture_remains_review_visible"
    return "no_special_source_bathroom_caveat_detected"


def evaluate_artifact(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    subjects = list(payload.get("subjects") or [])
    compatible_rows = [
        row
        for row in subjects
        if isinstance(row.get("compact_final_value_review_payload"), dict)
        and row.get("final_value_status") in {"manual_review_required", "unsupported"}
    ]
    evaluations = [classify_subject(row) for row in compatible_rows]
    moved = [row for row in evaluations if row["analysis_only_proposed_status"] != row["current_status"]]
    safe = [row for row in moved if row["safety_label"] == "safe"]
    borderline = [row for row in moved if row["safety_label"] == "borderline"]
    unsafe = [row for row in evaluations if row["safety_label"] == "unsafe"]
    return {
        "artifact_path": str(path),
        "artifact_subject_count": len(subjects),
        "compatible_subject_count": len(compatible_rows),
        "manual_review_subject_count": sum(
            1 for row in subjects if row.get("final_value_status") == "manual_review_required"
        ),
        "unsupported_subject_count": sum(
            1 for row in subjects if row.get("final_value_status") == "unsupported"
        ),
        "moved_count": len(moved),
        "moved_no_reduction_count": sum(1 for row in moved if row["no_reduction_case"]),
        "moved_positive_reduction_count": sum(
            1 for row in moved if row["positive_reduction_case"]
        ),
        "safe_candidate_count": len(safe),
        "borderline_candidate_count": len(borderline),
        "unsafe_or_rejected_count": len(unsafe),
        "evaluations": evaluations,
    }


def discover_artifacts(*, input_artifacts: list[str], scan_dir: str) -> list[Path]:
    discovered = [Path(path) for path in input_artifacts]
    scan_root = Path(scan_dir)
    if scan_root.exists():
        discovered.extend(scan_root.glob("unequal_roll*review_evidence*.json"))
    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in discovered:
        resolved = path.resolve()
        if resolved not in seen and path.exists():
            unique_paths.append(path)
            seen.add(resolved)
    return sorted(unique_paths)


def build_summary(report: dict[str, Any]) -> dict[str, Any]:
    all_evaluations = [
        evaluation
        for artifact in report["artifact_results"]
        for evaluation in artifact["evaluations"]
    ]
    moved = [
        evaluation
        for evaluation in all_evaluations
        if evaluation["analysis_only_proposed_status"] != evaluation["current_status"]
    ]
    return {
        "artifact_count_scanned": len(report["artifact_results"]),
        "artifact_count_with_compatible_rows": sum(
            1 for artifact in report["artifact_results"] if artifact["compatible_subject_count"] > 0
        ),
        "manual_review_rows_evaluated": len(all_evaluations),
        "would_move_count": len(moved),
        "would_move_no_reduction_count": sum(1 for row in moved if row["no_reduction_case"]),
        "would_move_positive_reduction_count": sum(
            1 for row in moved if row["positive_reduction_case"]
        ),
        "safe_candidate_count": sum(1 for row in moved if row["safety_label"] == "safe"),
        "borderline_candidate_count": sum(
            1 for row in moved if row["safety_label"] == "borderline"
        ),
        "unsafe_or_rejected_count": sum(
            1 for row in all_evaluations if row["safety_label"] == "unsafe"
        ),
    }


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    fieldnames = [
        "artifact_path",
        "account",
        "county",
        "current_status",
        "analysis_only_proposed_status",
        "qualification_outcome",
        "safety_label",
        "no_reduction_case",
        "positive_reduction_case",
        "included_comp_count",
        "review_heavy_exclusion_count",
        "review_heavy_exclusion_ratio",
        "likely_exclude_count",
        "median_all",
        "max_leave_one_out_delta",
        "max_leave_one_out_threshold",
        "adjusted_value_iqr",
        "adjusted_value_iqr_ratio",
        "max_adjustment_pct",
        "source_bathroom_caveat",
        "rejected_checks",
        "primary_review_drivers",
    ]
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            flat = dict(row)
            flat["rejected_checks"] = json.dumps(row.get("rejected_checks") or [])
            flat["primary_review_drivers"] = json.dumps(row.get("primary_review_drivers") or [])
            writer.writerow({name: flat.get(name) for name in fieldnames})


def build_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Unequal Roll Governance Refinement Analysis",
        "",
        "## Summary",
        f"- Artifacts scanned: `{summary['artifact_count_scanned']}`",
        f"- Artifacts with compatible rows: `{summary['artifact_count_with_compatible_rows']}`",
        f"- Manual-review rows evaluated: `{summary['manual_review_rows_evaluated']}`",
        f"- Would move to `supported_with_review` candidate: `{summary['would_move_count']}`",
        f"- Safe no-reduction candidates: `{summary['would_move_no_reduction_count']}`",
        f"- Borderline positive-reduction candidates: `{summary['would_move_positive_reduction_count']}`",
        "",
        "## Candidate Outcomes",
    ]
    for artifact in report["artifact_results"]:
        lines.append(f"### `{artifact['artifact_path']}`")
        if not artifact["evaluations"]:
            lines.append("- No compatible `manual_review_required` rows with compact payload metrics.")
            lines.append("")
            continue
        for row in artifact["evaluations"]:
            lines.append(
                f"- `{row['account']}`: `{row['current_status']}` -> "
                f"`{row['analysis_only_proposed_status']}` | `{row['safety_label']}` | "
                f"inc `{row['included_comp_count']}` | RH `{row['review_heavy_exclusion_count']}` | "
                f"likely `{row['likely_exclude_count']}` | loo `{row['max_leave_one_out_delta']}` | "
                f"iqr% `{None if row['adjusted_value_iqr_ratio'] is None else round(row['adjusted_value_iqr_ratio'] * 100, 2)}` | "
                f"reason `{row['qualification_outcome']}`"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    artifact_paths = discover_artifacts(
        input_artifacts=args.input_artifact,
        scan_dir=args.scan_dir,
    )
    artifact_results = [evaluate_artifact(path) for path in artifact_paths]
    report = {
        "generated_at": datetime.now().isoformat(),
        "artifact_results": artifact_results,
    }
    report["summary"] = build_summary(report)

    flattened_rows: list[dict[str, Any]] = []
    for artifact in artifact_results:
        for row in artifact["evaluations"]:
            flattened_rows.append({"artifact_path": artifact["artifact_path"], **row})

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"unequal_roll_review_policy_analysis_{timestamp}.json"
    csv_path = output_dir / f"unequal_roll_review_policy_analysis_{timestamp}.csv"
    md_path = output_dir / f"unequal_roll_review_policy_analysis_{timestamp}.md"

    json_path.write_text(json.dumps(report, indent=2))
    write_csv(flattened_rows, csv_path)
    md_path.write_text(build_markdown(report))

    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "md": str(md_path)}, indent=2))


if __name__ == "__main__":
    main()
