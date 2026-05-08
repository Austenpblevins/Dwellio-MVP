from __future__ import annotations

import argparse
import csv
import json
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from infra.scripts.report_unequal_roll_smart_harvest_harris_diagnostic import (  # noqa: E402
    resolve_diagnostic_artifact,
)

MARGINAL_SIMILARITY_IMPROVEMENT_THRESHOLD = 0.02
MATERIAL_ADJUSTED_MEDIAN_INCREASE_THRESHOLD = 5000.0
HIGH_VALUE_PER_SF_DELTA_THRESHOLD = 5.0
HIGH_NEIGHBORHOOD_PERCENTILE_DRIFT_THRESHOLD = 0.08
HIGH_NEIGHBORHOOD_DISTANCE_DRIFT_THRESHOLD = 10.0
HIGH_VALUE_OUTLIER_COUNT_THRESHOLD = 1

INPUT_CONTRACT = {
    "script_mode": "enriched_artifact_sensitivity_reporter",
    "full_diagnostic_generator": False,
    "supported_primary_input": "enriched_smart_harvest_diagnostic_json_or_clarification_wrapper",
    "supports_original_harris_artifact_directly": False,
    "notes": [
        "This script is review-only and no-persist.",
        "It reports scoring-sensitivity ideas from an already-enriched diagnostic artifact.",
        "It does not run a fresh Harris replay or mutate runtime behavior.",
    ],
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate no-persist Harris smart-harvest value-tier sensitivity reporting from "
            "an enriched diagnostic artifact or clarification wrapper."
        )
    )
    parser.add_argument(
        "--input-artifact",
        type=Path,
        required=True,
        help=(
            "Path to an enriched diagnostic JSON with per-case smart/current comparison "
            "details, or a clarification wrapper whose source_artifact points to one."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/private/tmp"))
    return parser


def build_guardrail_summary() -> dict[str, Any]:
    return {
        "db_writes_occurred": False,
        "runtime_defaults_changed": False,
        "smart_harvest_became_default": False,
        "tie_break_automation_enabled": False,
        "scoring_or_adjustment_formulas_changed": False,
        "final_values_changed": False,
        "workflow": "no_persist_analysis_only",
    }


def build_threshold_metadata() -> dict[str, Any]:
    return {
        "marginal_similarity_improvement_threshold": MARGINAL_SIMILARITY_IMPROVEMENT_THRESHOLD,
        "material_adjusted_median_increase_threshold": MATERIAL_ADJUSTED_MEDIAN_INCREASE_THRESHOLD,
        "high_value_per_sf_delta_threshold": HIGH_VALUE_PER_SF_DELTA_THRESHOLD,
        "high_neighborhood_percentile_drift_threshold": HIGH_NEIGHBORHOOD_PERCENTILE_DRIFT_THRESHOLD,
        "high_neighborhood_distance_drift_threshold": HIGH_NEIGHBORHOOD_DISTANCE_DRIFT_THRESHOLD,
        "high_value_outlier_count_threshold": HIGH_VALUE_OUTLIER_COUNT_THRESHOLD,
    }


def build_payload(
    *,
    diagnostic_artifact: dict[str, Any],
    input_resolution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_copy = deepcopy(diagnostic_artifact)
    cases = list(source_copy.get("cases") or [])
    harris_cases = [case for case in cases if str(case.get("county") or "").lower() == "harris"]
    priority_loss_cases = [
        case for case in harris_cases if str(case.get("cohort_role") or "") == "priority_taxpayer_loss"
    ]

    per_subject_rows = [build_per_subject_row(case) for case in harris_cases]
    payload = {
        "generated_at": datetime.now().isoformat(),
        "input_contract": {**deepcopy(INPUT_CONTRACT), **dict(input_resolution or {})},
        "source_artifact": source_copy.get("source_artifact"),
        "input_artifact_generated_at": source_copy.get("generated_at"),
        "guardrails": build_guardrail_summary(),
        "threshold_metadata": build_threshold_metadata(),
        "cohort_summary": build_cohort_summary(harris_cases),
        "per_subject_sensitivity_table": per_subject_rows,
        "per_neighborhood_summary": build_per_neighborhood_summary(priority_loss_cases),
        "value_per_sf_outlier_table": build_value_per_sf_outlier_table(harris_cases),
        "marginal_similarity_high_value_tradeoff_table": build_tradeoff_table(harris_cases),
        "lower_value_equally_credible_alternative_summary": build_lower_value_summary(
            priority_loss_cases
        ),
        "subdivision_micro_location_proxy_summary": build_micro_location_summary(harris_cases),
        "recommended_no_persist_scoring_sensitivity_experiments": build_recommendations(
            harris_cases
        ),
        "finding_buckets": build_finding_buckets(harris_cases),
    }
    return payload


def build_cohort_summary(cases: list[dict[str, Any]]) -> dict[str, Any]:
    role_counts: dict[str, int] = {}
    for case in cases:
        role = str(case.get("cohort_role") or "unknown")
        role_counts[role] = role_counts.get(role, 0) + 1
    return {
        "cases_reviewed": len(cases),
        "priority_taxpayer_loss_cases": role_counts.get("priority_taxpayer_loss", 0),
        "positive_control_cases": role_counts.get("positive_control", 0),
        "stable_control_cases": role_counts.get("stable_control", 0),
        "cohort_role_counts": role_counts,
    }


def build_per_subject_row(case: dict[str, Any]) -> dict[str, Any]:
    comparison = dict(case.get("comparison_summary") or {})
    harm = dict(case.get("smart_harvest_harm_explanation") or {})
    fairness = dict(case.get("value_fairness_outlier_report") or {})
    delta = dict(fairness.get("delta_smart_minus_current") or {})
    subject_context = dict(fairness.get("subject_value_context") or {})
    current_included = dict(fairness.get("current_included") or {})
    smart_included = dict(fairness.get("smart_included") or {})
    alternative = dict(case.get("equally_credible_lower_value_alternative_report") or {})

    labels = sorted(build_labels(case))
    experiments = sorted(build_experiment_flags(case))
    return {
        "subject_account": case.get("account"),
        "neighborhood_code": case.get("neighborhood_code"),
        "cohort_role": case.get("cohort_role"),
        "artifact_reduction_change_amount": case.get("artifact_reduction_change_amount"),
        "similarity_delta": delta.get("avg_similarity_score"),
        "adjusted_median_delta": comparison.get("adjusted_median_change"),
        "smart_median_appraised_value_per_sf": smart_included.get("median_appraised_value_per_sf"),
        "current_median_appraised_value_per_sf": current_included.get("median_appraised_value_per_sf"),
        "smart_median_adjusted_value_per_sf": smart_included.get("median_adjusted_value_per_sf"),
        "current_median_adjusted_value_per_sf": current_included.get("median_adjusted_value_per_sf"),
        "subject_appraised_value_per_sf": subject_context.get("subject_appraised_value_per_sf"),
        "smart_median_neighborhood_percentile": smart_included.get(
            "median_neighborhood_value_percentile"
        ),
        "current_median_neighborhood_percentile": current_included.get(
            "median_neighborhood_value_percentile"
        ),
        "smart_high_value_per_sf_outlier_count": smart_included.get(
            "high_value_per_sf_outlier_count"
        ),
        "smart_high_adjusted_value_outlier_count": smart_included.get(
            "high_adjusted_value_outlier_count"
        ),
        "lower_value_equally_credible_count": alternative.get(
            "count_lower_value_equally_credible_alternatives"
        ),
        "estimated_recoverable_value": alternative.get("estimated_reduction_impact"),
        "lower_value_alternative_classification": alternative.get("safe_manual_or_no_safe")
        or alternative.get("opportunity_class"),
        "primary_harm_classification": harm.get("primary_explanation_category"),
        "heuristic_labels": labels,
        "recommended_experiments": experiments,
    }


def build_per_neighborhood_summary(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for case in cases:
        grouped.setdefault(str(case.get("neighborhood_code") or ""), []).append(case)
    rows = []
    for neighborhood, items in sorted(grouped.items()):
        subject_rows = [build_per_subject_row(case) for case in items]
        rows.append(
            {
                "neighborhood_code": neighborhood,
                "case_count": len(items),
                "total_reduction_change_amount": round(
                    sum(_as_float(case.get("artifact_reduction_change_amount")) or 0.0 for case in items),
                    2,
                ),
                "price_tier_drift_case_count": sum(
                    1 for row in subject_rows if "possible_price_tier_drift" in row["heuristic_labels"]
                ),
                "micro_location_proxy_case_count": sum(
                    1 for row in subject_rows if "possible_micro_location_proxy" in row["heuristic_labels"]
                ),
                "marginal_tradeoff_case_count": sum(
                    1
                    for row in subject_rows
                    if "marginal_similarity_high_value_tradeoff" in row["heuristic_labels"]
                ),
                "lower_value_equally_credible_case_count": sum(
                    1
                    for row in subject_rows
                    if "lower_value_equally_credible_available" in row["heuristic_labels"]
                ),
            }
        )
    return rows


def build_value_per_sf_outlier_table(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for case in cases:
        fairness = dict(case.get("value_fairness_outlier_report") or {})
        delta = dict(fairness.get("delta_smart_minus_current") or {})
        current_included = dict(fairness.get("current_included") or {})
        smart_included = dict(fairness.get("smart_included") or {})
        subject_context = dict(fairness.get("subject_value_context") or {})
        rows.append(
            {
                "subject_account": case.get("account"),
                "neighborhood_code": case.get("neighborhood_code"),
                "cohort_role": case.get("cohort_role"),
                "delta_median_appraised_value_per_sf": delta.get("median_appraised_value_per_sf"),
                "delta_median_adjusted_value_per_sf": delta.get("median_adjusted_value_per_sf"),
                "subject_appraised_value_per_sf": subject_context.get("subject_appraised_value_per_sf"),
                "neighborhood_median_appraised_value_per_sf": subject_context.get(
                    "same_neighborhood_median_appraised_value_per_sf"
                ),
                "smart_median_neighborhood_percentile": smart_included.get(
                    "median_neighborhood_value_percentile"
                ),
                "current_median_neighborhood_percentile": current_included.get(
                    "median_neighborhood_value_percentile"
                ),
                "smart_high_value_outlier_comp_count": smart_included.get(
                    "high_value_outlier_comp_count"
                ),
                "smart_high_adjusted_value_outlier_count": smart_included.get(
                    "high_adjusted_value_outlier_count"
                ),
                "smart_high_value_per_sf_outlier_count": smart_included.get(
                    "high_value_per_sf_outlier_count"
                ),
                "value_per_sf_outlier_risk": is_value_per_sf_outlier_risk(case),
            }
        )
    return rows


def build_tradeoff_table(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for case in cases:
        comparison = dict(case.get("comparison_summary") or {})
        delta = dict((case.get("value_fairness_outlier_report") or {}).get("delta_smart_minus_current") or {})
        rows.append(
            {
                "subject_account": case.get("account"),
                "neighborhood_code": case.get("neighborhood_code"),
                "cohort_role": case.get("cohort_role"),
                "similarity_delta": delta.get("avg_similarity_score"),
                "adjusted_median_delta": comparison.get("adjusted_median_change"),
                "is_marginal_similarity_high_value_tradeoff": is_marginal_similarity_high_value_tradeoff(
                    case
                ),
            }
        )
    return rows


def build_lower_value_summary(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for case in cases:
        alternative = dict(case.get("equally_credible_lower_value_alternative_report") or {})
        rows.append(
            {
                "subject_account": case.get("account"),
                "neighborhood_code": case.get("neighborhood_code"),
                "count_lower_value_equally_credible_alternatives": alternative.get(
                    "count_lower_value_equally_credible_alternatives"
                ),
                "estimated_recoverable_value": alternative.get("estimated_reduction_impact"),
                "classification": alternative.get("safe_manual_or_no_safe")
                or alternative.get("opportunity_class"),
                "top_candidate_account_ids": list(alternative.get("top_candidate_account_ids") or []),
            }
        )
    return rows


def build_micro_location_summary(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for case in cases:
        if "possible_micro_location_proxy" not in build_labels(case):
            continue
        rows.append(
            {
                "subject_account": case.get("account"),
                "neighborhood_code": case.get("neighborhood_code"),
                "subdivision_shift_summary": build_subdivision_shift_summary(case),
                "heuristic_note": "Heuristic only; subdivision/value-tier shift is not causal proof.",
            }
        )
    return rows


def build_recommendations(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for case in cases:
        for flag in build_experiment_flags(case):
            counts[flag] = counts.get(flag, 0) + 1
    rows = []
    for flag, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        rows.append({"experiment_flag": flag, "case_count": count})
    return rows


def build_finding_buckets(cases: list[dict[str, Any]]) -> dict[str, Any]:
    evidence_backed = []
    heuristics = []
    hypotheses = []

    outlier_cases = sum(1 for case in cases if is_value_per_sf_outlier_risk(case))
    tradeoff_cases = sum(1 for case in cases if is_marginal_similarity_high_value_tradeoff(case))
    lower_value_cases = sum(
        1 for case in cases if "lower_value_equally_credible_available" in build_labels(case)
    )
    micro_location_cases = sum(
        1 for case in cases if "possible_micro_location_proxy" in build_labels(case)
    )

    evidence_backed.append(
        {
            "finding": "Smart comp sets often carry higher appraised and adjusted value-per-SF than current sets in the Harris diagnostic cohort.",
            "case_count": outlier_cases,
        }
    )
    evidence_backed.append(
        {
            "finding": "Some loss cases retain lower-value equally credible alternatives under the review-only tie-break screen.",
            "case_count": lower_value_cases,
        }
    )
    heuristics.append(
        {
            "finding": "Subdivision mix shifts may be acting as micro-location proxies when value-tier drift appears without a large similarity gain.",
            "case_count": micro_location_cases,
        }
    )
    heuristics.append(
        {
            "finding": "Marginal similarity improvements paired with material adjusted-median increases point to a high-value tradeoff worth testing in no-persist sensitivity experiments.",
            "case_count": tradeoff_cases,
        }
    )
    hypotheses.append(
        {
            "finding": "Price-tier drift penalties may reduce some Harris harms without needing new monetary adjustments, but that still needs no-persist replay validation.",
        }
    )
    return {
        "evidence_backed_findings": evidence_backed,
        "heuristic_findings": heuristics,
        "hypotheses_requiring_more_validation": hypotheses,
    }


def build_labels(case: dict[str, Any]) -> set[str]:
    labels: set[str] = set()
    if is_value_per_sf_outlier_risk(case):
        labels.add("value_per_sf_outlier_risk")
    if is_price_tier_drift(case):
        labels.add("possible_price_tier_drift")
    if is_marginal_similarity_high_value_tradeoff(case):
        labels.add("marginal_similarity_high_value_tradeoff")
    alternative_class = lower_value_alternative_class(case)
    if alternative_class == "safe_automated_candidate":
        labels.add("lower_value_equally_credible_available")
    elif alternative_class == "manual_review_only":
        labels.add("manual_review_lower_value_candidate")
    else:
        labels.add("no_safe_lower_value_alternative")
    if is_micro_location_proxy(case):
        labels.add("possible_micro_location_proxy")
    return labels


def build_experiment_flags(case: dict[str, Any]) -> set[str]:
    flags: set[str] = set()
    if is_value_per_sf_outlier_risk(case):
        flags.add("test_value_per_sf_outlier_penalty")
    if is_price_tier_drift(case):
        flags.add("test_price_tier_drift_penalty")
    if is_marginal_similarity_high_value_tradeoff(case):
        flags.add("test_marginal_similarity_high_value_guardrail")
    alternative_class = lower_value_alternative_class(case)
    if alternative_class in {"safe_automated_candidate", "manual_review_only"}:
        flags.add("test_lower_value_credible_candidate_review_rule")
    if not flags:
        flags.add("do_not_test_due_to_insufficient_signal")
    return flags


def is_value_per_sf_outlier_risk(case: dict[str, Any]) -> bool:
    fairness = dict(case.get("value_fairness_outlier_report") or {})
    delta = dict(fairness.get("delta_smart_minus_current") or {})
    smart_included = dict(fairness.get("smart_included") or {})
    appraised_delta = _as_float(delta.get("median_appraised_value_per_sf")) or 0.0
    adjusted_delta = _as_float(delta.get("median_adjusted_value_per_sf")) or 0.0
    outlier_count = _as_int(smart_included.get("high_value_per_sf_outlier_count")) or 0
    return (
        appraised_delta >= HIGH_VALUE_PER_SF_DELTA_THRESHOLD
        or adjusted_delta >= HIGH_VALUE_PER_SF_DELTA_THRESHOLD
        or outlier_count >= HIGH_VALUE_OUTLIER_COUNT_THRESHOLD
    )


def is_price_tier_drift(case: dict[str, Any]) -> bool:
    fairness = dict(case.get("value_fairness_outlier_report") or {})
    delta = dict(fairness.get("delta_smart_minus_current") or {})
    current_included = dict(fairness.get("current_included") or {})
    smart_included = dict(fairness.get("smart_included") or {})
    adjusted_delta = _as_float(delta.get("median_adjusted_value")) or 0.0
    percentile_delta = (_as_float(smart_included.get("median_neighborhood_value_percentile")) or 0.0) - (
        _as_float(current_included.get("median_neighborhood_value_percentile")) or 0.0
    )
    distance_delta = (_as_float(smart_included.get("median_distance_from_neighborhood_median_value_per_sf")) or 0.0) - (
        _as_float(current_included.get("median_distance_from_neighborhood_median_value_per_sf")) or 0.0
    )
    return (
        adjusted_delta >= MATERIAL_ADJUSTED_MEDIAN_INCREASE_THRESHOLD
        and (
            percentile_delta >= HIGH_NEIGHBORHOOD_PERCENTILE_DRIFT_THRESHOLD
            or distance_delta >= HIGH_NEIGHBORHOOD_DISTANCE_DRIFT_THRESHOLD
        )
    )


def is_marginal_similarity_high_value_tradeoff(case: dict[str, Any]) -> bool:
    delta = dict((case.get("value_fairness_outlier_report") or {}).get("delta_smart_minus_current") or {})
    comparison = dict(case.get("comparison_summary") or {})
    similarity_delta = _as_float(delta.get("avg_similarity_score")) or 0.0
    adjusted_delta = _as_float(comparison.get("adjusted_median_change")) or 0.0
    return (
        similarity_delta <= MARGINAL_SIMILARITY_IMPROVEMENT_THRESHOLD
        and adjusted_delta >= MATERIAL_ADJUSTED_MEDIAN_INCREASE_THRESHOLD
    )


def lower_value_alternative_class(case: dict[str, Any]) -> str:
    alternative = dict(case.get("equally_credible_lower_value_alternative_report") or {})
    return str(
        alternative.get("safe_manual_or_no_safe")
        or alternative.get("opportunity_class")
        or "no_safe_opportunity"
    )


def is_micro_location_proxy(case: dict[str, Any]) -> bool:
    labels = build_subdivision_shift_summary(case)
    return labels["subject_subdivision"] != labels["smart_dominant_subdivision"] and is_price_tier_drift(
        case
    )


def build_subdivision_shift_summary(case: dict[str, Any]) -> dict[str, Any]:
    subject_subdivision = str(
        ((case.get("subject_features") or {}).get("subdivision_name") or "").strip()
    )
    smart_rows = list(case.get("smart_included_comp_rows") or [])
    counts: dict[str, int] = {}
    for row in smart_rows:
        subdivision = str(((row.get("source_features") or {}).get("subdivision_name") or "").strip())
        counts[subdivision] = counts.get(subdivision, 0) + 1
    smart_dominant = ""
    if counts:
        smart_dominant = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
    return {
        "subject_subdivision": subject_subdivision,
        "smart_dominant_subdivision": smart_dominant,
        "smart_subdivision_mix": counts,
    }


def write_payload(payload: dict[str, Any], *, output_dir: Path) -> dict[str, str]:
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    stem = output_dir / f"unequal_roll_harris_value_tier_sensitivity_{timestamp}"
    json_path = f"{stem}.json"
    csv_path = f"{stem}.csv"
    md_path = f"{stem}.md"

    Path(json_path).write_text(json.dumps(payload, indent=2))
    write_csv(csv_path, payload["per_subject_sensitivity_table"])
    Path(md_path).write_text(render_markdown(payload))

    return {"json": json_path, "csv": csv_path, "md": md_path}


def write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    flattened = [flatten_row(row) for row in rows]
    fieldnames: list[str] = []
    for row in flattened:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in flattened:
            writer.writerow(row)


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Harris Value-Tier Sensitivity Report",
        "",
        "## Cohort Summary",
        f"- Cases reviewed: `{payload['cohort_summary']['cases_reviewed']}`",
        f"- Priority taxpayer-loss cases: `{payload['cohort_summary']['priority_taxpayer_loss_cases']}`",
        f"- Positive controls: `{payload['cohort_summary']['positive_control_cases']}`",
        f"- Stable controls: `{payload['cohort_summary']['stable_control_cases']}`",
        "",
        "## Threshold Metadata",
    ]
    for key, value in payload["threshold_metadata"].items():
        lines.append(f"- `{key}`: `{value}`")

    lines.extend(
        [
            "",
            "## Recommended No-Persist Scoring Sensitivity Experiments",
        ]
    )
    for row in payload["recommended_no_persist_scoring_sensitivity_experiments"]:
        lines.append(f"- `{row['experiment_flag']}`: `{row['case_count']}` cases")

    lines.extend(
        [
            "",
            "## Guardrails",
            f"- DB writes occurred: `{str(payload['guardrails']['db_writes_occurred']).lower()}`",
            f"- Runtime defaults changed: `{str(payload['guardrails']['runtime_defaults_changed']).lower()}`",
            f"- Smart harvest became default: `{str(payload['guardrails']['smart_harvest_became_default']).lower()}`",
            f"- Tie-break automation enabled: `{str(payload['guardrails']['tie_break_automation_enabled']).lower()}`",
            f"- Scoring/adjustment formulas changed: `{str(payload['guardrails']['scoring_or_adjustment_formulas_changed']).lower()}`",
            f"- Final values changed: `{str(payload['guardrails']['final_values_changed']).lower()}`",
        ]
    )
    return "\n".join(lines) + "\n"


def flatten_row(row: dict[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (list, dict)):
            flattened[key] = json.dumps(value, sort_keys=True)
        else:
            flattened[key] = value
    return flattened


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    input_artifact = json.loads(args.input_artifact.read_text())
    resolved, input_resolution = resolve_diagnostic_artifact(input_artifact)
    payload = build_payload(
        diagnostic_artifact=resolved,
        input_resolution=input_resolution,
    )
    paths = write_payload(payload, output_dir=args.output_dir)
    print(json.dumps({"artifacts": paths}, indent=2))


if __name__ == "__main__":
    main()
