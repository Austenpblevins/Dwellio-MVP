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
            "Analysis-only governance tiering evaluator for unequal-roll no-persist "
            "review artifacts."
        )
    )
    parser.add_argument(
        "--input-artifact",
        action="append",
        default=[],
        help="Path to replay/review evidence JSON artifact. May be provided multiple times.",
    )
    parser.add_argument(
        "--output-dir",
        default="/private/tmp",
        help="Directory for analysis outputs.",
    )
    parser.add_argument(
        "--focus-account",
        action="append",
        default=[],
        help="Optional account filters for reporting.",
    )
    return parser.parse_args()


def _as_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_subjects(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text())
    if isinstance(payload.get("subjects"), list):
        return list(payload.get("subjects") or [])
    if isinstance(payload.get("rows"), list):
        return list(payload.get("rows") or [])
    return []


def _conflict_metrics(included_rows: list[dict[str, Any]]) -> dict[str, Any]:
    affected_comp_count = 0
    flag_counter: Counter[str] = Counter()
    for row in included_rows:
        governance = dict(row.get("conflict_divergence_governance") or {})
        affected = False
        for key in CONFLICT_FLAG_KEYS:
            if governance.get(key) is True:
                flag_counter[key] += 1
                affected = True
        if affected:
            affected_comp_count += 1
    included_count = len(included_rows)
    return {
        "affected_comp_count": affected_comp_count,
        "affected_ratio": (
            affected_comp_count / included_count if included_count else None
        ),
        "flag_counts": dict(flag_counter),
    }


def _source_bath_caveat(subject: dict[str, Any], included_rows: list[dict[str, Any]]) -> str:
    source_counts = Counter(
        (row.get("source_governance_status") or "unknown") for row in included_rows
    )
    unresolved_source = any("unresolved_review_only" in key for key in source_counts)
    county = subject.get("county")

    if county == "fort_bend":
        valuation_statuses = Counter()
        for row in included_rows:
            bathroom_features = (
                (row.get("bathroom_boundary_context") or {}).get("valuation_bathroom_features")
                or {}
            )
            attachment = bathroom_features.get("attachment_status")
            count_status = bathroom_features.get("bathroom_count_status")
            if attachment or count_status:
                valuation_statuses[f"{attachment}:{count_status}"] += 1
        if unresolved_source and valuation_statuses:
            return "fort_bend_mixed_source_review_visible"
        if unresolved_source:
            return "fort_bend_unresolved_source_review_visible"
        if valuation_statuses:
            return "fort_bend_clean_bath_support_present"
        return "fort_bend_source_unclear"

    if unresolved_source:
        return "unresolved_source_review_visible"
    return "no_material_source_caveat"


def _burden_metrics(included_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter((row.get("burden_governance_status") or "unknown") for row in included_rows)
    included_count = len(included_rows)
    warning_count = counts.get("warning", 0)
    warning_ratio = (warning_count / included_count) if included_count else None
    within_ratio = (counts.get("within_thresholds", 0) / included_count) if included_count else None
    return {
        "counts": dict(counts),
        "warning_count": warning_count,
        "warning_ratio": warning_ratio,
        "within_ratio": within_ratio,
    }


def _classify_caveat_type(
    *,
    current_status: str | None,
    thin_support: bool,
    likely_exclude_count: int,
    review_heavy_ratio: float | None,
    unstable: bool,
    source_bath_caveat: str,
) -> str:
    if current_status == "unsupported":
        return "unsupported_blocking_defect"
    if thin_support or likely_exclude_count > 0:
        return "unsupported_blocking_defect"
    if review_heavy_ratio is not None and review_heavy_ratio > 0.15:
        return "manual_stop_caveat"
    if unstable:
        return "manual_stop_caveat"
    if source_bath_caveat in {
        "unresolved_source_review_visible",
        "fort_bend_mixed_source_review_visible",
        "fort_bend_unresolved_source_review_visible",
    }:
        return "supported_with_review_caveat"
    return "explanation_only"


def _classify_governance_tier(subject: dict[str, Any]) -> dict[str, Any]:
    current_status = subject.get("final_value_status")
    reduction_amount = _as_float(subject.get("requested_reduction_amount"))
    positive_reduction = bool(reduction_amount is not None and reduction_amount > 0.0)
    no_reduction = bool(reduction_amount is None or reduction_amount <= 0.0)

    detail = dict(subject.get("final_value_detail_json") or {})
    set_summary = dict(detail.get("final_value_set_summary") or {})
    stability = dict(detail.get("stability_metrics") or {})
    included_rows = list(detail.get("included_comp_rows") or [])

    included_count = int(
        subject.get("included_comp_count")
        or set_summary.get("included_count")
        or len(included_rows)
        or 0
    )
    review_heavy_count = int(
        subject.get("excluded_review_heavy_count")
        or set_summary.get("excluded_review_heavy_count")
        or 0
    )
    likely_exclude_count = int(
        subject.get("excluded_likely_exclude_count")
        or set_summary.get("excluded_likely_exclude_count")
        or 0
    )
    review_heavy_ratio = (
        review_heavy_count / included_count if included_count else None
    )

    median_all = _as_float(stability.get("median_all"))
    loo_delta = _as_float(stability.get("max_leave_one_out_delta"))
    iqr = _as_float(stability.get("adjusted_value_iqr"))
    iqr_ratio = (
        (iqr / median_all) if iqr is not None and median_all not in {None, 0.0} else None
    )
    max_adjustment_pct = _as_float(stability.get("max_adjustment_pct"))
    loo_threshold = (
        min(15000.0, median_all * 0.06) if median_all not in {None, 0.0} else None
    )
    stable_median = bool(
        loo_threshold is not None
        and loo_delta is not None
        and iqr_ratio is not None
        and loo_delta <= loo_threshold
        and iqr_ratio <= 0.55
    )
    unstable = not stable_median
    thin_support = included_count < 12

    conflict = _conflict_metrics(included_rows)
    burden = _burden_metrics(included_rows)
    source_bath_caveat = _source_bath_caveat(subject, included_rows)

    high_conflict = bool(
        conflict["affected_ratio"] is not None
        and (
            conflict["affected_ratio"] > 0.20
            or conflict["affected_comp_count"] > 3
        )
    )
    high_adjustment_pct = bool(
        max_adjustment_pct is not None and max_adjustment_pct > 0.15
    )
    high_review_heavy = bool(
        review_heavy_ratio is not None and review_heavy_ratio > 0.15
    )
    burden_warning_heavy = bool(
        burden["warning_ratio"] is not None and burden["warning_ratio"] >= 0.50
    )

    if current_status == "unsupported":
        recommended_tier = "unsupported_blocking"
        tier_reason = "current_status_unsupported"
    elif thin_support or likely_exclude_count > 0 or high_review_heavy:
        recommended_tier = "manual_stop"
        tier_reason = "support_or_exclusion_pressure"
    elif no_reduction and stable_median and burden_warning_heavy:
        recommended_tier = "supported_with_review_caveat"
        tier_reason = "stable_no_reduction_but_warning_heavy_review_visible"
    elif positive_reduction and (
        current_status == "manual_review_required"
        or high_conflict
        or burden_warning_heavy
        or high_adjustment_pct
    ):
        recommended_tier = "manual_stop"
        tier_reason = "positive_reduction_conservative_default"
    elif no_reduction and stable_median and not high_adjustment_pct:
        if source_bath_caveat in {
            "unresolved_source_review_visible",
            "fort_bend_mixed_source_review_visible",
            "fort_bend_unresolved_source_review_visible",
        }:
            recommended_tier = "supported_with_review_caveat"
            tier_reason = "stable_no_reduction_with_visible_caveats"
        elif current_status == "supported_with_review":
            recommended_tier = "supported_with_review_caveat"
            tier_reason = "already_supported_with_review"
        elif burden["warning_ratio"] is not None and burden["warning_ratio"] > 0.0:
            recommended_tier = "supported_with_review_caveat"
            tier_reason = "stable_no_reduction_with_nonblocking_burden_warning"
        else:
            recommended_tier = "clean_support"
            tier_reason = "stable_no_reduction_low_risk"
    elif stable_median and not high_conflict and not high_adjustment_pct:
        recommended_tier = "supported_with_review_caveat"
        tier_reason = "stable_support_with_review_visible_caveat"
    else:
        recommended_tier = "manual_stop"
        tier_reason = "residual_governance_risk"

    caveat_type = _classify_caveat_type(
        current_status=current_status,
        thin_support=thin_support,
        likely_exclude_count=likely_exclude_count,
        review_heavy_ratio=review_heavy_ratio,
        unstable=unstable,
        source_bath_caveat=source_bath_caveat,
    )

    if recommended_tier in {"supported_with_review_caveat", "clean_support"}:
        if positive_reduction:
            burden_interpretation = "warning_blocking"
        elif stable_median and not high_review_heavy and likely_exclude_count == 0:
            burden_interpretation = "warning_visible"
        else:
            burden_interpretation = "warning_blocking"
    else:
        burden_interpretation = "warning_blocking"

    return {
        "account": subject.get("account"),
        "county": subject.get("county"),
        "current_status": current_status,
        "recommended_tier": recommended_tier,
        "reduction_amount": reduction_amount,
        "included_count": included_count,
        "review_heavy_count": review_heavy_count,
        "likely_exclude_count": likely_exclude_count,
        "burden_warning_share": burden["warning_ratio"],
        "burden_status_counts": burden["counts"],
        "conflict_affected_count": conflict["affected_comp_count"],
        "conflict_affected_share": conflict["affected_ratio"],
        "conflict_flag_counts": conflict["flag_counts"],
        "max_leave_one_out_delta": loo_delta,
        "loo_threshold": loo_threshold,
        "adjusted_value_iqr": iqr,
        "adjusted_value_iqr_ratio": iqr_ratio,
        "max_adjustment_pct": max_adjustment_pct,
        "source_bath_caveat": source_bath_caveat,
        "caveat_type": caveat_type,
        "burden_interpretation": burden_interpretation,
        "stable_median": stable_median,
        "positive_reduction_case": positive_reduction,
        "no_reduction_case": no_reduction,
        "tier_reason": tier_reason,
    }


def evaluate_artifacts(paths: list[Path]) -> dict[str, Any]:
    by_account: dict[str, dict[str, Any]] = {}
    for path in paths:
        for subject in _load_subjects(path):
            account = subject.get("account")
            if not account:
                continue
            by_account[account] = subject

    rows = [_classify_governance_tier(subject) for subject in by_account.values()]
    rows.sort(key=lambda row: (row["county"] or "", row["account"] or ""))

    summary = summarize_rows(rows)
    return {"rows": rows, "summary": summary}


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "case_count": len(rows),
        "tier_counts": dict(Counter(row["recommended_tier"] for row in rows)),
        "current_status_counts": dict(Counter(row["current_status"] for row in rows)),
        "caveat_type_counts": dict(Counter(row["caveat_type"] for row in rows)),
        "burden_interpretation_counts": dict(
            Counter(row["burden_interpretation"] for row in rows)
        ),
    }


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fieldnames = [
        "account",
        "county",
        "current_status",
        "recommended_tier",
        "reduction_amount",
        "included_count",
        "review_heavy_count",
        "likely_exclude_count",
        "burden_warning_share",
        "conflict_affected_share",
        "max_leave_one_out_delta",
        "adjusted_value_iqr_ratio",
        "max_adjustment_pct",
        "source_bath_caveat",
        "caveat_type",
        "burden_interpretation",
        "tier_reason",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def build_markdown(report: dict[str, Any], focus_accounts: set[str]) -> str:
    lines = [
        "# Unequal Roll Governance Tiering Analysis",
        "",
        "## Summary",
        f"- Cases evaluated: `{report['summary']['case_count']}`",
        f"- Tier counts: `{report['summary']['tier_counts']}`",
        f"- Current status counts: `{report['summary']['current_status_counts']}`",
        f"- Caveat type counts: `{report['summary']['caveat_type_counts']}`",
        f"- Burden interpretation counts: `{report['summary']['burden_interpretation_counts']}`",
        "",
        "## Case Table",
        "| Account | County | Current | Recommended Tier | Reduction | Included | RH | Likely | Burden Warn Share | Conflict Share | LOO | IQR Ratio | Max Adj Pct | Source/Bath Caveat | Caveat Type | Burden Mode | Reason |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|",
    ]
    for row in report["rows"]:
        marker = " *" if row["account"] in focus_accounts else ""
        lines.append(
            f"| {row['account']}{marker} | {row['county']} | {row['current_status']} | "
            f"{row['recommended_tier']} | {row['reduction_amount']} | {row['included_count']} | "
            f"{row['review_heavy_count']} | {row['likely_exclude_count']} | "
            f"{None if row['burden_warning_share'] is None else round(row['burden_warning_share'], 3)} | "
            f"{None if row['conflict_affected_share'] is None else round(row['conflict_affected_share'], 3)} | "
            f"{row['max_leave_one_out_delta']} | "
            f"{None if row['adjusted_value_iqr_ratio'] is None else round(row['adjusted_value_iqr_ratio'], 3)} | "
            f"{row['max_adjustment_pct']} | {row['source_bath_caveat']} | "
            f"{row['caveat_type']} | {row['burden_interpretation']} | {row['tier_reason']} |"
        )

    lines.extend(
        [
            "",
            "## Caveat Taxonomy",
            "| Caveat Type | Meaning |",
            "|---|---|",
            "| explanation_only | Caveat can remain in explanation without changing support tier. |",
            "| supported_with_review_caveat | Caveat should remain review-visible but not force manual stop by itself. |",
            "| manual_stop_caveat | Caveat pattern indicates human stop risk. |",
            "| unsupported_blocking_defect | Support is not viable or blocked. |",
            "",
            "## Burden Interpretation Matrix",
            "| Burden Mode | Intended Use |",
            "|---|---|",
            "| warning_visible | Keep warning in narrative but allow supported-with-review when stability/support is strong. |",
            "| warning_blocking | Treat warning posture as stop-driving when paired with instability, thin support, exclusion pressure, or positive-reduction risk. |",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    paths = [Path(path) for path in args.input_artifact if Path(path).exists()]
    if not paths:
        raise SystemExit("No valid input artifacts provided.")

    report = evaluate_artifacts(paths)
    focus_accounts = set(args.focus_account)
    if focus_accounts:
        report["rows"] = [
            row for row in report["rows"] if row.get("account") in focus_accounts
        ]
        report["summary"] = summarize_rows(report["rows"])

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    json_path = output_dir / f"unequal_roll_governance_tiering_analysis_{timestamp}.json"
    csv_path = output_dir / f"unequal_roll_governance_tiering_analysis_{timestamp}.csv"
    md_path = output_dir / f"unequal_roll_governance_tiering_analysis_{timestamp}.md"

    json_path.write_text(json.dumps(report, indent=2))
    write_csv(report["rows"], csv_path)
    md_path.write_text(build_markdown(report, focus_accounts))
    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "md": str(md_path)}, indent=2))


if __name__ == "__main__":
    main()
