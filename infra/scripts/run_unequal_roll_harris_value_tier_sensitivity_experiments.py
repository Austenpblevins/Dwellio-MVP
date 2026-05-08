from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.services.unequal_roll_no_persist_replay import (  # noqa: E402
    UnequalRollNoPersistReplayService,
    UnequalRollReplayRequest,
)
from app.services.unequal_roll_smart_harvest import (  # noqa: E402
    CURRENT_ORDER_CAP_100,
    SIMILARITY_TOP_100,
)
from app.services.unequal_roll_taxpayer_favorable_tiebreak import (  # noqa: E402
    TaxpayerFavorableTieBreakConfig,
    UnequalRollTaxpayerFavorableTieBreakService,
)
from infra.scripts.report_unequal_roll_harris_value_tier_sensitivity import (  # noqa: E402
    build_labels,
)
from infra.scripts.report_unequal_roll_smart_harvest_harris_diagnostic import (  # noqa: E402
    resolve_diagnostic_artifact,
)

INPUT_CONTRACT = {
    "script_mode": "post_selection_swap_sensitivity_experiment_runner",
    "full_diagnostic_generator": False,
    "requires_read_only_replay": True,
    "supported_primary_input": "enriched_smart_harvest_diagnostic_json_or_clarification_wrapper",
    "baseline_strategy": "similarity_top_100",
    "experiment_method": "post_selection_swap_recompute",
    "full_candidate_reranking": False,
    "production_scoring_penalty": False,
    "notes": [
        "This script is analysis-only and no-persist.",
        "It uses the enriched Harris diagnostic artifact to choose the cohort and risk labels.",
        "It reruns current and similarity_top_100 in Stage 21 read-only mode to obtain full no-persist replay detail.",
        "It applies temporary post-selection swap/recompute logic outside production scoring and does not change runtime defaults.",
        "Value-per-SF and price-tier labels are trigger signals for swap experiments, not actual production scoring penalties.",
        "This runner does not rerank the full candidate universe.",
        "No taxpayer loss claims are measured versus the similarity_top_100 smart baseline.",
    ],
}


@dataclass(frozen=True)
class ExperimentStrategy:
    key: str
    label: str
    report_label: str
    trigger_labels: tuple[str, ...]
    requires_lower_value_candidate: bool
    recovery_source: str
    config: TaxpayerFavorableTieBreakConfig


EXPERIMENT_STRATEGIES = [
    ExperimentStrategy(
        key="value_per_sf_outlier_penalty",
        label="Value-per-SF Outlier Penalty",
        report_label="Value-per-SF Outlier Triggered Swap",
        trigger_labels=("value_per_sf_outlier_risk",),
        requires_lower_value_candidate=True,
        recovery_source="value_per_sf_outlier_triggered_swap",
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            similarity_tolerance=0.03,
            median_movement_cap_ratio=0.03,
            max_avg_similarity_drop=0.015,
        ),
    ),
    ExperimentStrategy(
        key="price_tier_drift_penalty",
        label="Price-Tier Drift Penalty",
        report_label="Price-Tier Drift Triggered Swap",
        trigger_labels=("possible_price_tier_drift",),
        requires_lower_value_candidate=True,
        recovery_source="price_tier_triggered_swap",
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            similarity_tolerance=0.025,
            median_movement_cap_ratio=0.025,
            max_avg_similarity_drop=0.0125,
        ),
    ),
    ExperimentStrategy(
        key="marginal_similarity_high_value_guardrail",
        label="Marginal Similarity / High-Value Guardrail",
        report_label="Marginal Similarity / High-Value Triggered Swap",
        trigger_labels=("marginal_similarity_high_value_tradeoff",),
        requires_lower_value_candidate=True,
        recovery_source="marginal_similarity_triggered_swap",
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            similarity_tolerance=0.01,
            median_movement_cap_ratio=0.015,
            max_avg_similarity_drop=0.005,
        ),
    ),
    ExperimentStrategy(
        key="lower_value_credible_candidate_review_rule",
        label="Lower-Value Credible Candidate Review Rule",
        report_label="Lower-Value Credible Candidate Review Swap",
        trigger_labels=(),
        requires_lower_value_candidate=True,
        recovery_source="lower_value_credible_swap",
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            similarity_tolerance=0.02,
            median_movement_cap_ratio=0.02,
            max_avg_similarity_drop=0.01,
        ),
    ),
    ExperimentStrategy(
        key="combined_conservative_sensitivity_strategy",
        label="Combined Conservative Sensitivity Strategy",
        report_label="Combined Conservative Triggered Swap",
        trigger_labels=(
            "value_per_sf_outlier_risk",
            "possible_price_tier_drift",
            "marginal_similarity_high_value_tradeoff",
        ),
        requires_lower_value_candidate=True,
        recovery_source="combined_triggered_swap",
        config=TaxpayerFavorableTieBreakConfig(
            max_swaps=1,
            similarity_tolerance=0.015,
            median_movement_cap_ratio=0.015,
            max_avg_similarity_drop=0.0075,
        ),
    ),
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run no-persist Harris smart-harvest post-selection swap sensitivity experiments "
            "against Stage 21 read-only replays without changing production scoring."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--input-artifact", type=Path, required=True)
    parser.add_argument("--requested-tax-year", type=int, default=2026)
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


def build_experiment_limitations() -> list[str]:
    return [
        "This runner performs post-selection swap/recompute experiments against the similarity_top_100 smart baseline.",
        "It does not rerank the full same-neighborhood candidate universe.",
        "It does not apply true production scoring penalties.",
        "Value-per-SF and price-tier labels are trigger signals for temporary swaps, not scoring coefficients.",
        "Recovered taxpayer value is measured versus the similarity_top_100 baseline, not versus production default rollout decisions.",
        "Accepted opportunities may still land in manual_review_only rather than safe_automated_candidate.",
    ]


def resolve_harris_cases(diagnostic_artifact: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        case
        for case in list(diagnostic_artifact.get("cases") or [])
        if str(case.get("county") or "").lower() == "harris"
    ]


def lower_value_candidate_available(case: dict[str, Any]) -> bool:
    alternative = dict(case.get("equally_credible_lower_value_alternative_report") or {})
    classification = str(
        alternative.get("safe_manual_or_no_safe")
        or alternative.get("opportunity_class")
        or "no_safe_opportunity"
    )
    return classification in {"safe_automated_candidate", "manual_review_only"}


def should_trigger_strategy(case: dict[str, Any], strategy: ExperimentStrategy) -> tuple[bool, list[str]]:
    labels = set(build_labels(case))
    matched = [label for label in strategy.trigger_labels if label in labels]
    if strategy.requires_lower_value_candidate and not lower_value_candidate_available(case):
        return False, matched
    if not strategy.trigger_labels:
        return lower_value_candidate_available(case), matched
    return bool(matched), matched


def replay(
    service: UnequalRollNoPersistReplayService,
    conn: Any,
    *,
    county: str,
    account: str,
    requested_tax_year: int,
    strategy: str,
) -> dict[str, Any]:
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cur:
            result = service.replay_subject(
                cur,
                request=UnequalRollReplayRequest(
                    county_id=county,
                    account_number=account,
                    requested_tax_year=requested_tax_year,
                ),
                same_neighborhood_harvest_strategy=strategy,
                include_taxpayer_favorable_tiebreak_reporting=False,
            )
        conn.rollback()
        return result
    except Exception:
        conn.rollback()
        raise


def summarize_baseline(result: dict[str, Any]) -> dict[str, Any]:
    detail = dict(result.get("final_value_detail_json") or {})
    included_rows = list(detail.get("included_comp_rows") or [])
    return {
        "final_status": result.get("final_value_status"),
        "support_status": result.get("support_status"),
        "included_comp_count": result.get("included_comp_count"),
        "review_heavy_count": result.get("excluded_review_heavy_count"),
        "likely_exclude_count": result.get("excluded_likely_exclude_count"),
        "adjusted_median": result.get("requested_roll_value"),
        "requested_reduction_amount": result.get("requested_reduction_amount"),
        "requested_reduction_pct": result.get("requested_reduction_pct"),
        "avg_similarity_score": avg_similarity(included_rows),
        "stability_metrics": result.get("stability_metrics"),
        "replay_status": result.get("replay_status"),
    }


def summarize_experiment_result(
    *,
    strategy: ExperimentStrategy,
    case: dict[str, Any],
    current_result: dict[str, Any],
    smart_result: dict[str, Any],
    experiment_result: dict[str, Any] | None,
    triggered: bool,
    trigger_labels: list[str],
) -> dict[str, Any]:
    smart_summary = summarize_baseline(smart_result)
    current_summary = summarize_baseline(current_result)
    result = experiment_result or smart_result
    detail = dict(result.get("final_value_detail_json") or {})
    included_rows = list(detail.get("included_comp_rows") or result.get("included_comp_rows") or [])
    similarity = avg_similarity(included_rows)
    requested_reduction_amount = _as_float(result.get("requested_reduction_amount"))
    smart_reduction = _as_float(smart_result.get("requested_reduction_amount"))
    taxpayer_delta_vs_smart = round((requested_reduction_amount or 0.0) - (smart_reduction or 0.0), 2)
    adjusted_median = _as_float(result.get("requested_roll_value"))
    smart_median = _as_float(smart_result.get("requested_roll_value"))
    alternative = dict(case.get("equally_credible_lower_value_alternative_report") or {})

    return {
        "strategy_key": strategy.key,
        "strategy_label": strategy.label,
        "strategy_report_label": strategy.report_label,
        "subject_account": case.get("account"),
        "neighborhood_code": case.get("neighborhood_code"),
        "cohort_role": case.get("cohort_role"),
        "comparison_baseline": "similarity_top_100",
        "experiment_method": "post_selection_swap_recompute",
        "triggered": triggered,
        "trigger_labels": trigger_labels,
        "heuristic_labels": sorted(build_labels(case)),
        "trigger_signal_only": True,
        "lower_value_candidate_class": alternative.get("safe_manual_or_no_safe")
        or alternative.get("opportunity_class"),
        "estimated_lower_value_recovery": alternative.get("estimated_reduction_impact"),
        "final_status": result.get("final_value_status"),
        "support_status": result.get("support_status"),
        "included_comp_count": result.get("included_comp_count"),
        "review_heavy_count": result.get("excluded_review_heavy_count"),
        "likely_exclude_count": result.get("excluded_likely_exclude_count"),
        "adjusted_median": adjusted_median,
        "requested_reduction_amount": requested_reduction_amount,
        "requested_reduction_pct": result.get("requested_reduction_pct"),
        "avg_similarity_score": similarity,
        "swapped_comp_count": result.get("swapped_comp_count", 0),
        "alternatives_considered_count": result.get("alternatives_considered_count", 0),
        "taxpayer_delta_vs_smart": taxpayer_delta_vs_smart,
        "taxpayer_delta_vs_current": round(
            (requested_reduction_amount or 0.0)
            - (_as_float(current_result.get("requested_reduction_amount")) or 0.0),
            2,
        ),
        "similarity_delta_vs_smart": round((similarity or 0.0) - (smart_summary["avg_similarity_score"] or 0.0), 4),
        "adjusted_median_delta_vs_smart": round((adjusted_median or 0.0) - (smart_median or 0.0), 2),
        "included_comp_count_delta_vs_smart": (result.get("included_comp_count") or 0)
        - (smart_result.get("included_comp_count") or 0),
        "review_heavy_delta_vs_smart": (result.get("excluded_review_heavy_count") or 0)
        - (smart_result.get("excluded_review_heavy_count") or 0),
        "likely_exclude_delta_vs_smart": (result.get("excluded_likely_exclude_count") or 0)
        - (smart_result.get("excluded_likely_exclude_count") or 0),
        "support_status_drift": str(result.get("support_status") or "")
        != str(smart_result.get("support_status") or ""),
        "final_status_drift": str(result.get("final_value_status") or "")
        != str(smart_result.get("final_value_status") or ""),
        "recovered_lower_value_credible_amount": max(0.0, taxpayer_delta_vs_smart),
        "recovery_source_explanation": determine_recovery_source(strategy, triggered),
        "remains_defensible": result.get("remains_defensible"),
        "automation_assessment": dict(result.get("automation_assessment") or {}),
        "baseline_current": current_summary,
        "baseline_smart": smart_summary,
        "simulation_metadata": dict(result.get("simulation_metadata") or {}),
        "qa_flags": dict(result.get("qa_flags") or {}),
    }


def run_experiment_for_case(
    *,
    tie_service: UnequalRollTaxpayerFavorableTieBreakService,
    case: dict[str, Any],
    current_result: dict[str, Any],
    smart_result: dict[str, Any],
    strategy: ExperimentStrategy,
) -> dict[str, Any]:
    triggered, trigger_labels = should_trigger_strategy(case, strategy)
    if not triggered:
        return summarize_experiment_result(
            strategy=strategy,
            case=case,
            current_result=current_result,
            smart_result=smart_result,
            experiment_result=None,
            triggered=False,
            trigger_labels=trigger_labels,
        )

    experiment_result = tie_service.simulate(
        current_result=current_result,
        smart_result=smart_result,
        config=strategy.config,
    )
    return summarize_experiment_result(
        strategy=strategy,
        case=case,
        current_result=current_result,
        smart_result=smart_result,
        experiment_result=experiment_result,
        triggered=True,
        trigger_labels=trigger_labels,
    )


def avg_similarity(rows: list[dict[str, Any]]) -> float | None:
    values = [
        _as_float(row.get("similarity_score"))
        for row in rows
        if _as_float(row.get("similarity_score")) is not None
    ]
    return round(mean(values), 4) if values else None


def summarize_strategy_collection(strategy: ExperimentStrategy, rows: list[dict[str, Any]]) -> dict[str, Any]:
    net_taxpayer_impact = round(sum(_as_float(row.get("taxpayer_delta_vs_smart")) or 0.0 for row in rows), 2)
    neighborhoods: dict[str, float] = {}
    for row in rows:
        neighborhood = str(row.get("neighborhood_code") or "")
        neighborhoods[neighborhood] = round(
            neighborhoods.get(neighborhood, 0.0)
            + (_as_float(row.get("taxpayer_delta_vs_smart")) or 0.0),
            2,
        )
    helped = [
        {"neighborhood_code": key, "net_taxpayer_impact": value}
        for key, value in sorted(neighborhoods.items())
        if value > 0
    ]
    harmed = [
        {"neighborhood_code": key, "net_taxpayer_impact": value}
        for key, value in sorted(neighborhoods.items())
        if value < 0
    ]
    triggered_rows = [row for row in rows if row.get("triggered")]
    recommendation = recommend_strategy(rows, net_taxpayer_impact)

    return {
        "strategy_key": strategy.key,
        "strategy_label": strategy.label,
        "strategy_report_label": strategy.report_label,
        "strategy_config": asdict(strategy.config),
        "comparison_baseline": "similarity_top_100",
        "experiment_method": "post_selection_swap_recompute",
        "trigger_signal_only": True,
        "cases_evaluated": len(rows),
        "cases_triggered": len(triggered_rows),
        "taxpayer_reduction_recovered": round(
            sum(max(0.0, _as_float(row.get("taxpayer_delta_vs_smart")) or 0.0) for row in rows), 2
        ),
        "taxpayer_reduction_lost": round(
            sum(min(0.0, _as_float(row.get("taxpayer_delta_vs_smart")) or 0.0) for row in rows), 2
        ),
        "net_taxpayer_impact": net_taxpayer_impact,
        "avg_similarity_impact": _avg([row.get("similarity_delta_vs_smart") for row in rows]),
        "avg_adjusted_median_impact": _avg([row.get("adjusted_median_delta_vs_smart") for row in rows]),
        "avg_included_comp_count_impact": _avg([row.get("included_comp_count_delta_vs_smart") for row in rows]),
        "avg_review_heavy_impact": _avg([row.get("review_heavy_delta_vs_smart") for row in rows]),
        "avg_likely_exclude_impact": _avg([row.get("likely_exclude_delta_vs_smart") for row in rows]),
        "support_status_drift_count": sum(1 for row in rows if row.get("support_status_drift")),
        "final_status_drift_count": sum(1 for row in rows if row.get("final_status_drift")),
        "lower_value_credible_alternative_recovery": round(
            sum(_as_float(row.get("recovered_lower_value_credible_amount")) or 0.0 for row in rows),
            2,
        ),
        "automation_assessment_counts": build_automation_assessment_counts(rows),
        "recovery_source_counts": build_recovery_source_counts(rows),
        "top_recovered_cases": build_top_recovered_cases(rows),
        "neighborhoods_helped": helped,
        "neighborhoods_harmed": harmed,
        "recommendation": recommendation,
    }


def recommend_strategy(rows: list[dict[str, Any]], net_taxpayer_impact: float) -> str:
    support_drift = sum(1 for row in rows if row.get("support_status_drift"))
    final_drift = sum(1 for row in rows if row.get("final_status_drift"))
    if support_drift > 0 or final_drift > 0:
        return "do_not_productionize_yet"
    if net_taxpayer_impact <= 0:
        return "do_not_productionize_yet"
    if net_taxpayer_impact < 10000:
        return "keep_analysis_only"
    triggered_count = sum(1 for row in rows if row.get("triggered"))
    manual_review_count = sum(
        1
        for row in rows
        if str(((row.get("automation_assessment") or {}).get("automation_status") or "")) == "manual_review_only"
    )
    if manual_review_count >= max(1, triggered_count // 2):
        return "manual_review_candidate"
    return "broaden_no_persist_validation"


def determine_recovery_source(strategy: ExperimentStrategy, triggered: bool) -> str:
    if not triggered:
        return "no_strategy_trigger"
    return strategy.recovery_source


def build_automation_assessment_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "safe_automated_candidate": 0,
        "manual_review_only": 0,
        "no_safe_opportunity": 0,
    }
    for row in rows:
        status = str(((row.get("automation_assessment") or {}).get("automation_status") or ""))
        if status in counts:
            counts[status] += 1
        elif not row.get("triggered"):
            counts["no_safe_opportunity"] += 1
    return counts


def build_recovery_source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("recovery_source_explanation") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def build_top_recovered_cases(rows: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    ranked = sorted(
        rows,
        key=lambda row: (
            -(_as_float(row.get("taxpayer_delta_vs_smart")) or 0.0),
            str(row.get("subject_account") or ""),
        ),
    )
    output = []
    for row in ranked[:limit]:
        if (_as_float(row.get("taxpayer_delta_vs_smart")) or 0.0) <= 0:
            continue
        output.append(
            {
                "subject_account": row.get("subject_account"),
                "neighborhood_code": row.get("neighborhood_code"),
                "taxpayer_delta_vs_smart": row.get("taxpayer_delta_vs_smart"),
                "automation_status": (row.get("automation_assessment") or {}).get("automation_status"),
                "recovery_source_explanation": row.get("recovery_source_explanation"),
            }
        )
    return output


def build_payload(
    *,
    source_artifact: str | None,
    input_generated_at: str | None,
    input_resolution: dict[str, Any] | None,
    requested_tax_year: int,
    case_results: list[dict[str, Any]],
) -> dict[str, Any]:
    strategy_rows: dict[str, list[dict[str, Any]]] = {}
    for row in case_results:
        strategy_rows.setdefault(str(row.get("strategy_key") or ""), []).append(row)

    summaries = [
        summarize_strategy_collection(strategy, strategy_rows.get(strategy.key, []))
        for strategy in EXPERIMENT_STRATEGIES
    ]
    return {
        "generated_at": datetime.now().isoformat(),
        "input_contract": {**INPUT_CONTRACT, **dict(input_resolution or {})},
        "source_artifact": source_artifact,
        "input_artifact_generated_at": input_generated_at,
        "requested_tax_year": requested_tax_year,
        "guardrails": build_guardrail_summary(),
        "comparison_baseline": "similarity_top_100",
        "experiment_method": "post_selection_swap_recompute",
        "experiment_limitations": build_experiment_limitations(),
        "cohort_summary": {
            "cases_reviewed": len({str(row.get("subject_account") or "") for row in case_results}),
            "priority_taxpayer_loss_cases": sum(
                1 for row in case_results if row.get("cohort_role") == "priority_taxpayer_loss"
            )
            // len(EXPERIMENT_STRATEGIES),
            "positive_control_cases": sum(
                1 for row in case_results if row.get("cohort_role") == "positive_control"
            )
            // len(EXPERIMENT_STRATEGIES),
            "stable_control_cases": sum(
                1 for row in case_results if row.get("cohort_role") == "stable_control"
            )
            // len(EXPERIMENT_STRATEGIES),
        },
        "per_subject_strategy_table": case_results,
        "strategy_summary": summaries,
        "evidence_backed_findings": build_evidence_findings(summaries),
        "heuristic_findings": build_heuristic_findings(),
        "hypotheses_requiring_more_validation": build_hypotheses(),
    }


def build_evidence_findings(strategy_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for summary in strategy_summaries:
        rows.append(
            {
                "strategy_key": summary["strategy_key"],
                "strategy_report_label": summary["strategy_report_label"],
                "net_taxpayer_impact": summary["net_taxpayer_impact"],
                "support_status_drift_count": summary["support_status_drift_count"],
                "final_status_drift_count": summary["final_status_drift_count"],
                "recommendation": summary["recommendation"],
                "comparison_baseline": "similarity_top_100",
            }
        )
    return rows


def build_heuristic_findings() -> list[dict[str, Any]]:
    return [
        {
            "finding": "Price-tier drift and micro-location labels remain heuristic guidance from the enriched diagnostic, not causal proof.",
            "note": "These labels trigger post-selection swap experiments and should not be read as production scoring penalties.",
        }
    ]


def build_hypotheses() -> list[dict[str, Any]]:
    return [
        {
            "finding": "A future no-persist experiment may perform differently if it reranks the full same-neighborhood universe instead of applying conservative post-selection swaps.",
            "follow_up": "test_full_reranking_variant",
        }
    ]


def flatten_row(row: dict[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (list, dict)):
            flattened[key] = json.dumps(value, sort_keys=True)
        else:
            flattened[key] = value
    return flattened


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    flattened = [flatten_row(row) for row in rows]
    fieldnames: list[str] = []
    for row in flattened:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in flattened:
            writer.writerow(row)


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Harris Value-Tier Sensitivity Experiments",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Requested tax year: {payload['requested_tax_year']}",
        f"- Cases reviewed: {payload['cohort_summary']['cases_reviewed']}",
        f"- Comparison baseline: `{payload['comparison_baseline']}`",
        f"- Experiment method: `{payload['experiment_method']}`",
        "",
        "## Experiment Limitations",
    ]
    for item in payload["experiment_limitations"]:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Strategy Summary",
        ]
    )
    for summary in payload["strategy_summary"]:
        lines.extend(
            [
                f"### {summary['strategy_report_label']}",
                f"- Cases triggered: `{summary['cases_triggered']}`",
                f"- Taxpayer reduction recovered: `{summary['taxpayer_reduction_recovered']}`",
                f"- Taxpayer reduction lost: `{summary['taxpayer_reduction_lost']}`",
                f"- Net taxpayer impact: `{summary['net_taxpayer_impact']}`",
                f"- Comparison baseline: `{summary['comparison_baseline']}`",
                f"- Trigger-signal only: `{str(summary['trigger_signal_only']).lower()}`",
                f"- Support-status drift count: `{summary['support_status_drift_count']}`",
                f"- Final-status drift count: `{summary['final_status_drift_count']}`",
                f"- Recommendation: `{summary['recommendation']}`",
                f"- Automation assessment counts: `{summary['automation_assessment_counts']}`",
                f"- Recovery source counts: `{summary['recovery_source_counts']}`",
            ]
        )
        if summary["top_recovered_cases"]:
            lines.append("- Top recovered cases:")
            for row in summary["top_recovered_cases"]:
                lines.append(
                    f"  - `{row['subject_account']}` / `{row['neighborhood_code']}`: "
                    f"`{row['taxpayer_delta_vs_smart']}` via `{row['recovery_source_explanation']}` "
                    f"[{row['automation_status']}]"
                )
        lines.append("")

    lines.extend(
        [
            "## Findings",
            "- Evidence-backed findings:",
        ]
    )
    for row in payload["evidence_backed_findings"]:
        lines.append(
            f"  - `{row['strategy_report_label']}`: net `{row['net_taxpayer_impact']}`, "
            f"drift `{row['support_status_drift_count']}/{row['final_status_drift_count']}`, "
            f"recommendation `{row['recommendation']}`."
        )
    lines.append("- Heuristic findings:")
    for row in payload["heuristic_findings"]:
        lines.append(f"  - {row['finding']}")
    lines.append("- Hypotheses requiring more validation:")
    for row in payload["hypotheses_requiring_more_validation"]:
        lines.append(f"  - {row['finding']} (`{row['follow_up']}`)")
    path.write_text("\n".join(lines))


def write_payload(payload: dict[str, Any], *, output_dir: Path) -> dict[str, str]:
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    stem = output_dir / f"unequal_roll_harris_value_tier_experiments_{timestamp}"
    json_path = f"{stem}.json"
    csv_path = f"{stem}.csv"
    md_path = f"{stem}.md"
    Path(json_path).write_text(json.dumps(payload, indent=2))
    write_csv(Path(csv_path), payload["per_subject_strategy_table"])
    write_md(Path(md_path), payload)
    return {"json": json_path, "csv": csv_path, "md": md_path}


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _avg(values: list[Any]) -> float | None:
    cleaned = [_as_float(value) for value in values]
    usable = [value for value in cleaned if value is not None]
    return round(mean(usable), 4) if usable else None


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    input_artifact = json.loads(args.input_artifact.read_text())
    resolved_artifact, input_resolution = resolve_diagnostic_artifact(input_artifact)
    cases = resolve_harris_cases(resolved_artifact)

    replay_service = UnequalRollNoPersistReplayService()
    tie_service = UnequalRollTaxpayerFavorableTieBreakService()
    case_results: list[dict[str, Any]] = []

    with replay_service.connect_read_only(args.database_url) as conn:
        for case in cases:
            current_result = replay(
                replay_service,
                conn,
                county="harris",
                account=str(case.get("account") or ""),
                requested_tax_year=args.requested_tax_year,
                strategy=CURRENT_ORDER_CAP_100,
            )
            smart_result = replay(
                replay_service,
                conn,
                county="harris",
                account=str(case.get("account") or ""),
                requested_tax_year=args.requested_tax_year,
                strategy=SIMILARITY_TOP_100,
            )
            for strategy in EXPERIMENT_STRATEGIES:
                case_results.append(
                    run_experiment_for_case(
                        tie_service=tie_service,
                        case=case,
                        current_result=current_result,
                        smart_result=smart_result,
                        strategy=strategy,
                    )
                )

    payload = build_payload(
        source_artifact=resolved_artifact.get("source_artifact"),
        input_generated_at=resolved_artifact.get("generated_at"),
        input_resolution=input_resolution,
        requested_tax_year=args.requested_tax_year,
        case_results=case_results,
    )
    artifacts = write_payload(payload, output_dir=args.output_dir)
    print(json.dumps({"artifacts": artifacts, "strategy_summary": payload["strategy_summary"]}, indent=2))


if __name__ == "__main__":
    main()
