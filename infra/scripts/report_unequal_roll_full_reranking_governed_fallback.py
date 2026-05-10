from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))


MATERIAL_TAXPAYER_CHANGE_THRESHOLD = 1000.0
LARGE_OUTLIER_GAIN_THRESHOLD = 25000.0
MATERIAL_SIMILARITY_DECLINE = -0.02
SEVERE_SIMILARITY_DECLINE = -0.05
BLOCKED_SEGMENTS = {("fort_bend", "4950-04")}
DEFAULT_PROMISING_SEGMENTS = {"193.09", "229.60", "790.00", "979.00"}
DEFAULT_MANUAL_REVIEW_SEGMENTS = {
    "1347.00",
    "1524.00",
    "1628.00",
    "2337.01",
    "3850-03",
    "3850-04",
    "4850-00",
    "5670-01",
    "5907-00",
    "7153.00",
    "8502-00",
}
PRIMARY_VARIANTS = ("simple_value_tier_rerank", "value_tier_plus_micro_location")
REFERENCE_VARIANTS = ("all_penalties", "value_tier_plus_micro_location_plus_soft_land")
FINAL_STATUS_ORDER = {
    "supported": 0,
    "supported_with_review": 1,
    "manual_review_required": 2,
    "unsupported": 3,
}
FINAL_MODEL_VALUE = "final_model_value"
NON_MODEL_BACKED_VALUES = {"diagnostic_only", "provisional_or_ambiguous", "unavailable"}
GOVERNANCE_VIEWS = {
    "automated_safe": {"eligible_candidate"},
    "analyst_assisted": {"eligible_candidate", "manual_review_required"},
    "fallback_blocked": {
        "blocked_case",
        "not_eligible_low_benefit",
        "insufficient_evidence",
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Report governed fallback outcomes from a no-persist full-reranking artifact. "
            "This is analysis-only and does not run replay or write to the database."
        )
    )
    parser.add_argument("--source-artifact", type=Path, required=True)
    parser.add_argument("--governance-artifact", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("/private/tmp"))
    return parser


def as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def final_status_pair(row: dict[str, Any]) -> tuple[str, str]:
    transition = str(row.get("final_status_transition_smart_to_rerank") or "")
    if " -> " in transition:
        source, target = transition.split(" -> ", 1)
        return source, target
    return str(row.get("smart_final_value_status") or ""), str(row.get("rerank_final_value_status") or "")


def true_transition_to_unsupported(row: dict[str, Any]) -> bool:
    source, target = final_status_pair(row)
    return source != "unsupported" and target == "unsupported"


def true_final_status_downgrade(row: dict[str, Any]) -> bool:
    source, target = final_status_pair(row)
    return FINAL_STATUS_ORDER.get(target, 99) > FINAL_STATUS_ORDER.get(source, 99)


def true_final_status_upgrade(row: dict[str, Any]) -> bool:
    source, target = final_status_pair(row)
    return FINAL_STATUS_ORDER.get(target, 99) < FINAL_STATUS_ORDER.get(source, 99)


def is_model_backed(row: dict[str, Any]) -> bool:
    return (
        row.get("current_value_interpretation") == FINAL_MODEL_VALUE
        and row.get("smart_value_interpretation") == FINAL_MODEL_VALUE
        and row.get("rerank_value_interpretation") == FINAL_MODEL_VALUE
    )


def included_comp_collapse(row: dict[str, Any]) -> bool:
    smart_count = as_int(row.get("smart_included_comp_count"))
    rerank_count = as_int(row.get("rerank_final_included_comp_count"))
    if rerank_count == 0:
        rerank_count = as_int(row.get("rerank_included_comp_count"))
    return smart_count > 0 and rerank_count <= 0


def loses_final_value_support(row: dict[str, Any]) -> bool:
    smart_status, rerank_status = final_status_pair(row)
    return (
        smart_status != "unsupported"
        and rerank_status == "unsupported"
        or row.get("rerank_value_interpretation") == "unavailable"
    )


def is_blocked_segment(
    row: dict[str, Any],
    blocked_segments: set[tuple[str, str]] | None = None,
) -> bool:
    blocked_segments = blocked_segments or BLOCKED_SEGMENTS
    return (str(row.get("county_id")), str(row.get("neighborhood_code"))) in blocked_segments


def classify_governed_case(
    row: dict[str, Any],
    *,
    blocked_segments: set[tuple[str, str]] | None = None,
) -> tuple[str, list[str]]:
    if not row.get("comparison_ready"):
        return "insufficient_evidence", ["comparison_not_ready"]

    blocked_reasons: list[str] = []
    if true_transition_to_unsupported(row):
        blocked_reasons.append("true_transition_to_unsupported")
    if row.get("rerank_value_interpretation") == "unavailable":
        blocked_reasons.append("rerank_value_unavailable")
    if included_comp_collapse(row):
        blocked_reasons.append("included_comp_count_collapse")
    if loses_final_value_support(row):
        blocked_reasons.append("loses_final_value_support")
    if is_blocked_segment(row, blocked_segments=blocked_segments):
        blocked_reasons.append("blocked_segment_4950_04")
    if as_float(row.get("rerank_vs_smart_similarity_delta")) <= SEVERE_SIMILARITY_DECLINE:
        blocked_reasons.append("severe_similarity_deterioration")
    if blocked_reasons:
        return "blocked_case", blocked_reasons

    if row.get("rerank_final_value_status") == "unsupported":
        return "insufficient_evidence", ["rerank_result_unsupported"]

    taxpayer_delta = as_float(row.get("rerank_vs_smart_taxpayer_delta"))
    if taxpayer_delta < MATERIAL_TAXPAYER_CHANGE_THRESHOLD:
        return "not_eligible_low_benefit", ["taxpayer_delta_below_material_threshold"]

    review_reasons: list[str] = []
    if true_final_status_downgrade(row):
        review_reasons.append("true_final_status_downgrade")
    if row.get("rerank_value_interpretation") in NON_MODEL_BACKED_VALUES:
        review_reasons.append("diagnostic_or_provisional_rerank_value")
    if as_float(row.get("rerank_vs_smart_similarity_delta")) <= MATERIAL_SIMILARITY_DECLINE:
        review_reasons.append("material_similarity_decline")
    if taxpayer_delta >= LARGE_OUTLIER_GAIN_THRESHOLD:
        review_reasons.append("large_outlier_gain")
    if review_reasons:
        return "manual_review_required", review_reasons

    if is_model_backed(row):
        return "eligible_candidate", ["model_backed_stable_material_benefit"]
    return "insufficient_evidence", ["not_model_backed"]


def should_fallback_to_smart(classification: str) -> bool:
    return classification in {
        "blocked_case",
        "not_eligible_low_benefit",
        "insufficient_evidence",
    }


def governance_view_for_classification(classification: str) -> str:
    if classification == "eligible_candidate":
        return "automated_safe"
    if classification == "manual_review_required":
        return "analyst_assisted"
    return "fallback_blocked"


def build_governed_case(
    row: dict[str, Any],
    *,
    blocked_segments: set[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    classification, reasons = classify_governed_case(
        row,
        blocked_segments=blocked_segments,
    )
    fallback_used = should_fallback_to_smart(classification)
    raw_delta_vs_smart = as_float(row.get("rerank_vs_smart_taxpayer_delta"))
    raw_delta_vs_current = as_float(row.get("rerank_vs_current_taxpayer_delta"))
    governed_delta_vs_smart = 0.0 if fallback_used else raw_delta_vs_smart
    governed_delta_vs_current = (
        as_float(row.get("smart_vs_current_taxpayer_delta"))
        if fallback_used
        else raw_delta_vs_current
    )
    governed_final_status = (
        row.get("smart_final_value_status") if fallback_used else row.get("rerank_final_value_status")
    )
    governed_value_interpretation = (
        row.get("smart_value_interpretation") if fallback_used else row.get("rerank_value_interpretation")
    )
    return {
        "variant_key": row.get("variant_key"),
        "county_id": row.get("county_id"),
        "subject_account": row.get("subject_account"),
        "neighborhood_code": row.get("neighborhood_code"),
        "governance_classification": classification,
        "governance_view": governance_view_for_classification(classification),
        "governance_reasons": reasons,
        "fallback_to_similarity_top_100": fallback_used,
        "raw_delta_vs_smart": raw_delta_vs_smart,
        "raw_delta_vs_current": raw_delta_vs_current,
        "governed_delta_vs_smart": governed_delta_vs_smart,
        "governed_delta_vs_current": governed_delta_vs_current,
        "raw_model_backed": is_model_backed(row),
        "governed_model_backed": (
            row.get("current_value_interpretation") == FINAL_MODEL_VALUE
            and row.get("smart_value_interpretation") == FINAL_MODEL_VALUE
            and governed_value_interpretation == FINAL_MODEL_VALUE
        ),
        "smart_final_status": row.get("smart_final_value_status"),
        "rerank_final_status": row.get("rerank_final_value_status"),
        "governed_final_status": governed_final_status,
        "smart_value_interpretation": row.get("smart_value_interpretation"),
        "rerank_value_interpretation": row.get("rerank_value_interpretation"),
        "governed_value_interpretation": governed_value_interpretation,
        "true_transition_to_unsupported_raw": true_transition_to_unsupported(row),
        "true_final_status_downgrade_raw": true_final_status_downgrade(row),
        "true_final_status_upgrade_raw": true_final_status_upgrade(row),
        "included_comp_count_collapse_raw": included_comp_collapse(row),
        "support_status_drift_raw": bool(row.get("rerank_support_status_drift_vs_smart")),
        "review_heavy_delta": as_int(row.get("rerank_review_heavy_delta_vs_smart")),
        "likely_exclude_delta": as_int(row.get("rerank_likely_exclude_delta_vs_smart")),
        "similarity_delta": as_float(row.get("rerank_vs_smart_similarity_delta")),
        "smart_included_comp_count": as_int(row.get("smart_included_comp_count")),
        "rerank_included_comp_count": as_int(row.get("rerank_included_comp_count")),
        "rerank_final_included_comp_count": as_int(row.get("rerank_final_included_comp_count")),
        "smart_vs_rerank_overlap_count": as_int(row.get("smart_vs_rerank_overlap_count")),
        "primary_explanation": row.get("primary_explanation"),
    }


def summarize_cases(cases: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    raw_net = sum(as_float(case["raw_delta_vs_smart"]) for case in cases)
    governed_net = sum(as_float(case["governed_delta_vs_smart"]) for case in cases)
    raw_model_net = sum(
        as_float(case["raw_delta_vs_smart"]) for case in cases if case["raw_model_backed"]
    )
    governed_model_net = sum(
        as_float(case["governed_delta_vs_smart"]) for case in cases if case["governed_model_backed"]
    )
    gains = [
        as_float(case["governed_delta_vs_smart"])
        for case in cases
        if as_float(case["governed_delta_vs_smart"]) > MATERIAL_TAXPAYER_CHANGE_THRESHOLD
    ]
    losses = [
        as_float(case["governed_delta_vs_smart"])
        for case in cases
        if as_float(case["governed_delta_vs_smart"]) < -MATERIAL_TAXPAYER_CHANGE_THRESHOLD
    ]
    raw_similarity_values = [as_float(case["similarity_delta"]) for case in cases]
    retained_similarity_values = [
        as_float(case["similarity_delta"])
        for case in cases
        if not case["fallback_to_similarity_top_100"]
    ]
    classification_counts = Counter(case["governance_classification"] for case in cases)
    view_counts = Counter(case["governance_view"] for case in cases)
    fallback_cases = [case for case in cases if case["fallback_to_similarity_top_100"]]
    retained_cases = [case for case in cases if not case["fallback_to_similarity_top_100"]]
    raw_unsupported_transitions = [
        case for case in cases if case["true_transition_to_unsupported_raw"]
    ]
    retained_unsupported_transitions = [
        case for case in retained_cases if case["true_transition_to_unsupported_raw"]
    ]
    raw_comp_collapses = [
        case for case in cases if case["included_comp_count_collapse_raw"]
    ]
    caught_comp_collapses = [
        case
        for case in raw_comp_collapses
        if case["fallback_to_similarity_top_100"]
    ]
    retained_comp_collapses = [
        case
        for case in raw_comp_collapses
        if not case["fallback_to_similarity_top_100"]
    ]
    governed_unsupported_results = [
        case for case in cases if case["governed_final_status"] == "unsupported"
    ]
    return {
        "label": label,
        "case_count": len(cases),
        "governed_net_vs_smart": round(governed_net, 2),
        "governed_net_vs_current": round(
            sum(as_float(case["governed_delta_vs_current"]) for case in cases), 2
        ),
        "raw_ungated_net_vs_smart": round(raw_net, 2),
        "raw_ungated_model_backed_net_vs_smart": round(raw_model_net, 2),
        "governed_model_backed_net_vs_smart": round(governed_model_net, 2),
        "raw_savings_retained_pct": round(governed_net / raw_net * 100, 2) if raw_net else None,
        "model_backed_savings_retained_pct": (
            round(governed_model_net / raw_model_net * 100, 2) if raw_model_net else None
        ),
        "material_gain_count": len(gains),
        "material_loss_count": len(losses),
        "material_gain_dollars": round(sum(gains), 2),
        "material_loss_dollars": round(sum(losses), 2),
        "true_downgrade_count": sum(
            1 for case in cases if not case["fallback_to_similarity_top_100"] and case["true_final_status_downgrade_raw"]
        ),
        "raw_true_transition_to_unsupported_count": len(raw_unsupported_transitions),
        "retained_governed_true_transition_to_unsupported_count": len(retained_unsupported_transitions),
        "governed_final_status_unsupported_result_count": len(governed_unsupported_results),
        "support_status_drift_count": sum(
            1 for case in cases if not case["fallback_to_similarity_top_100"] and case["support_status_drift_raw"]
        ),
        "raw_included_comp_count_collapse_count": len(raw_comp_collapses),
        "caught_prevented_included_comp_count_collapse_count": len(caught_comp_collapses),
        "retained_governed_included_comp_count_collapse_count": len(retained_comp_collapses),
        "fallback_count": len(fallback_cases),
        "fallback_prevented_harm_count": sum(
            1 for case in fallback_cases if as_float(case["raw_delta_vs_smart"]) < 0
            or case["true_transition_to_unsupported_raw"]
            or case["included_comp_count_collapse_raw"]
        ),
        "fallback_removed_apparent_savings_count": sum(
            1 for case in fallback_cases if as_float(case["raw_delta_vs_smart"]) > 0
        ),
        "classification_counts": dict(classification_counts),
        "governance_view_counts": dict(view_counts),
        "raw_average_similarity_delta": (
            round(sum(raw_similarity_values) / len(raw_similarity_values), 4)
            if raw_similarity_values else None
        ),
        "governed_retained_average_similarity_delta": (
            round(sum(retained_similarity_values) / len(retained_similarity_values), 4)
            if retained_similarity_values else None
        ),
        # Backward-compatible aliases for older ad hoc readers. New report text uses
        # the explicit raw/retained field names above.
        "true_transition_to_unsupported_count": len(retained_unsupported_transitions),
        "unsupported_result_count": len(governed_unsupported_results),
        "included_comp_count_collapse_count": len(raw_comp_collapses),
        "average_similarity_delta": (
            round(sum(raw_similarity_values) / len(raw_similarity_values), 4)
            if raw_similarity_values else None
        ),
    }


def group_cases(
    cases: list[dict[str, Any]],
    *,
    key: str,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case in cases:
        grouped[str(case.get(key))].append(case)
    return {
        group: summarize_cases(group_cases, label=group)
        for group, group_cases in sorted(grouped.items())
    }


def build_view_summaries(cases: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for view, classifications in GOVERNANCE_VIEWS.items():
        view_cases = [
            case
            for case in cases
            if case["governance_classification"] in classifications
        ]
        summaries[view] = {
            "overall": summarize_cases(view_cases, label=view),
            "county_summaries": group_cases(view_cases, key="county_id"),
            "segment_summaries": group_cases(view_cases, key="neighborhood_code"),
        }
    return summaries


def derive_segment_sets(governance_payload: dict[str, Any] | None) -> dict[str, Any]:
    recommendations = (
        governance_payload.get("segment_governance_recommendations", {})
        if governance_payload
        else {}
    )
    if isinstance(recommendations, dict) and recommendations:
        by_posture: dict[str, set[str]] = defaultdict(set)
        for neighborhood, recommendation in recommendations.items():
            if not isinstance(recommendation, dict):
                continue
            posture = str(recommendation.get("governance_posture") or "insufficient_evidence")
            by_posture[posture].add(str(neighborhood))
        return {
            "source": "governance_artifact",
            "disclosure": (
                "Segment sets are derived from the supplied governance artifact. "
                "Only Fort Bend 4950-04 is used as a dangerous segment block in "
                "case-level fallback; other segment postures are reported for "
                "auditing and subset views."
            ),
            "danger_blocked_segment_keys": sorted(
                f"{county}:{neighborhood}" for county, neighborhood in BLOCKED_SEGMENTS
            ),
            "blocked_segments_for_reporting": sorted(by_posture.get("blocked_segment", set())),
            "promising_segments": sorted(by_posture.get("eligible_candidate", set())),
            "manual_review_segments": sorted(by_posture.get("manual_review_required", set())),
            "insufficient_evidence_segments": sorted(
                by_posture.get("insufficient_evidence", set())
            ),
        }
    return {
        "source": "artifact_specific_constants",
        "disclosure": (
            "Governance artifact segment recommendations were unavailable, so "
            "subset views use artifact-specific constants from the prior "
            "interpretation pass."
        ),
        "danger_blocked_segment_keys": sorted(
            f"{county}:{neighborhood}" for county, neighborhood in BLOCKED_SEGMENTS
        ),
        "blocked_segments_for_reporting": ["4950-04"],
        "promising_segments": sorted(DEFAULT_PROMISING_SEGMENTS),
        "manual_review_segments": sorted(DEFAULT_MANUAL_REVIEW_SEGMENTS),
        "insufficient_evidence_segments": [],
    }


def top_cases(
    cases: list[dict[str, Any]],
    *,
    delta_key: str,
    reverse: bool,
    limit: int = 10,
) -> list[dict[str, Any]]:
    sorted_cases = sorted(cases, key=lambda case: as_float(case.get(delta_key)), reverse=reverse)
    return [
        {
            "county_id": case["county_id"],
            "subject_account": case["subject_account"],
            "neighborhood_code": case["neighborhood_code"],
            "governance_classification": case["governance_classification"],
            "governed_delta_vs_smart": case["governed_delta_vs_smart"],
            "raw_delta_vs_smart": case["raw_delta_vs_smart"],
            "fallback_to_similarity_top_100": case["fallback_to_similarity_top_100"],
            "governance_reasons": case["governance_reasons"],
            "smart_final_status": case["smart_final_status"],
            "rerank_final_status": case["rerank_final_status"],
            "governed_final_status": case["governed_final_status"],
            "similarity_delta": case["similarity_delta"],
            "primary_explanation": case["primary_explanation"],
        }
        for case in sorted_cases[:limit]
    ]


def build_variant_report(
    *,
    variant: str,
    cases: list[dict[str, Any]],
    segment_sets: dict[str, Any],
) -> dict[str, Any]:
    fort_bend = [case for case in cases if case["county_id"] == "fort_bend"]
    harris = [case for case in cases if case["county_id"] == "harris"]
    promising_neighborhoods = set(segment_sets.get("promising_segments") or [])
    manual_review_only_neighborhoods = set(segment_sets.get("manual_review_segments") or [])
    blocked_neighborhoods = set(segment_sets.get("blocked_segments_for_reporting") or [])
    return {
        "variant": variant,
        "overall": summarize_cases(cases, label="overall"),
        "view_summaries": build_view_summaries(cases),
        "county_summaries": {
            "harris": summarize_cases(harris, label="harris"),
            "fort_bend": summarize_cases(fort_bend, label="fort_bend"),
            "fort_bend_excluding_4950_04": summarize_cases(
                [
                    case
                    for case in fort_bend
                    if case["neighborhood_code"] != "4950-04"
                ],
                label="fort_bend_excluding_4950_04",
            ),
        },
        "segment_summaries": group_cases(cases, key="neighborhood_code"),
        "subset_summaries": {
            "promising_segments_only": summarize_cases(
                [
                    case
                    for case in cases
                    if case["neighborhood_code"] in promising_neighborhoods
                ],
                label="promising_segments_only",
            ),
            "manual_review_segments_only": summarize_cases(
                [
                    case
                    for case in cases
                    if case["neighborhood_code"] in manual_review_only_neighborhoods
                ],
                label="manual_review_segments_only",
            ),
            "blocked_segments_for_reporting": summarize_cases(
                [
                    case
                    for case in cases
                    if case["neighborhood_code"] in blocked_neighborhoods
                ],
                label="blocked_segments_for_reporting",
            ),
        },
        "top_governed_gains": top_cases(cases, delta_key="governed_delta_vs_smart", reverse=True),
        "top_governed_losses": top_cases(cases, delta_key="governed_delta_vs_smart", reverse=False),
        "fallback_prevented_harm": [
            case
            for case in top_cases(
                [
                    case
                    for case in cases
                    if case["fallback_to_similarity_top_100"]
                    and (
                        as_float(case["raw_delta_vs_smart"]) < 0
                        or case["true_transition_to_unsupported_raw"]
                        or case["included_comp_count_collapse_raw"]
                    )
                ],
                delta_key="raw_delta_vs_smart",
                reverse=False,
            )
        ],
        "fallback_removed_apparent_savings": [
            case
            for case in top_cases(
                [
                    case
                    for case in cases
                    if case["fallback_to_similarity_top_100"]
                    and as_float(case["raw_delta_vs_smart"]) > 0
                ],
                delta_key="raw_delta_vs_smart",
                reverse=True,
            )
        ],
    }


def build_payload(
    *,
    source_payload: dict[str, Any],
    source_path: Path,
    governance_path: Path | None,
    governance_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    all_rows = list(source_payload.get("subject_rows") or [])
    variants = list(source_payload.get("execution_matrix", {}).get("executed_variant_keys") or [])
    selected_variants = [
        variant
        for variant in (*PRIMARY_VARIANTS, *REFERENCE_VARIANTS)
        if variant in variants
    ]
    segment_sets = derive_segment_sets(governance_payload)
    blocked_segment_keys = {
        tuple(key.split(":", 1))
        for key in segment_sets.get("danger_blocked_segment_keys", [])
        if ":" in key
    } or BLOCKED_SEGMENTS
    governed_cases = [
        build_governed_case(row, blocked_segments=blocked_segment_keys)
        for row in all_rows
        if row.get("variant_key") in selected_variants
    ]
    cases_by_variant: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case in governed_cases:
        cases_by_variant[str(case["variant_key"])].append(case)

    reports = {
        variant: build_variant_report(
            variant=variant,
            cases=cases_by_variant[variant],
            segment_sets=segment_sets,
        )
        for variant in selected_variants
    }
    simple = reports.get("simple_value_tier_rerank", {}).get("overall", {})
    micro = reports.get("value_tier_plus_micro_location", {}).get("overall", {})
    return {
        "input_contract": {
            "source_artifact": str(source_path),
            "governance_artifact": str(governance_path) if governance_path else None,
            "analysis_mode": "experiment_only_governed_fallback_reporting",
            "candidate_universe_mode": source_payload.get("selection_summary", {}).get("candidate_universe_mode"),
            "candidate_universe_limit": source_payload.get("selection_summary", {}).get("candidate_universe_limit"),
            "bounded_proxy_used_for_conclusions": False,
            "reranking_remains_experiment_only": True,
        },
        "guardrails": {
            "db_writes_occurred": False,
            "migrations_added": False,
            "runtime_defaults_changed": False,
            "production_scoring_adjustment_median_governance_final_value_changed": False,
            "tie_break_automation_enabled": False,
        },
        "governed_fallback_rules": {
            "view_definitions": {
                "automated_safe": (
                    "eligible_candidate only; manual-review cases are excluded and "
                    "must not be treated as automation-ready."
                ),
                "analyst_assisted": (
                    "eligible_candidate plus manual_review_required; this is governed "
                    "analysis with analyst review, not automatic application."
                ),
                "fallback_blocked": (
                    "blocked_case, not_eligible_low_benefit, and insufficient_evidence "
                    "cases sent back to similarity_top_100."
                ),
            },
            "fallback_to_similarity_top_100": [
                "true transition to unsupported",
                "rerank value unavailable",
                "zero included final comps / final comp collapse",
                "loses final-value support",
                "Fort Bend 4950-04",
                "not eligible due to low/no material benefit",
                "insufficient evidence",
            ],
            "manual_review_retains_rerank_for_analysis": [
                "true downgrade without unsupported",
                "diagnostic/provisional rerank value",
                "material similarity decline",
                "large outlier gain",
            ],
        },
        "segment_sets": segment_sets,
        "variants": reports,
        "case_rows": governed_cases,
        "production_readiness_interpretation": {
            "simple_value_tier_rerank_preferred_baseline": True,
            "micro_location_complexity_justified": False,
            "governed_reranking_closer_to_readiness": (
                simple.get("true_transition_to_unsupported_count") == 0
                and simple.get("governed_net_vs_smart", 0) > 0
            ),
            "remaining_blockers": [
                "Governed net still includes manual-review outcomes and should not be treated as automation-ready.",
                "Fort Bend has more unsupported/manual burden than Harris and needs segment-specific gating.",
                "The governed fallback rules need another true full-pool replay/report pass before any production default discussion.",
            ],
            "primary_recommendation": (
                "Keep simple_value_tier_rerank as the governed baseline; treat value_tier_plus_micro_location "
                "as secondary evidence only because added complexity is not needed for a first governance pass."
            ),
            "simple_vs_micro_governed_net_difference": round(
                as_float(micro.get("governed_net_vs_smart"))
                - as_float(simple.get("governed_net_vs_smart")),
                2,
            ),
        },
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def write_csv(path: Path, case_rows: list[dict[str, Any]]) -> None:
    fields = [
        "variant_key",
        "county_id",
        "subject_account",
        "neighborhood_code",
        "governance_classification",
        "governance_view",
        "governance_reasons",
        "fallback_to_similarity_top_100",
        "raw_delta_vs_smart",
        "governed_delta_vs_smart",
        "raw_delta_vs_current",
        "governed_delta_vs_current",
        "raw_model_backed",
        "governed_model_backed",
        "smart_final_status",
        "rerank_final_status",
        "governed_final_status",
        "smart_value_interpretation",
        "rerank_value_interpretation",
        "governed_value_interpretation",
        "true_transition_to_unsupported_raw",
        "true_final_status_downgrade_raw",
        "included_comp_count_collapse_raw",
        "similarity_delta",
        "review_heavy_delta",
        "likely_exclude_delta",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in case_rows:
            output = {field: row.get(field) for field in fields}
            output["governance_reasons"] = ";".join(row.get("governance_reasons") or [])
            writer.writerow(output)


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Governed Full-Reranking Fallback Report",
        "",
        f"Source: `{payload['input_contract']['source_artifact']}`",
        f"Candidate universe mode: `{payload['input_contract']['candidate_universe_mode']}`",
        "",
        "Manual-review cases are not automation-ready. The report separates eligible-only automation evidence from analyst-assisted governed evidence.",
        "",
        "## Variant Summary",
        "",
        "| Variant | Governed net vs smart | Model-backed governed net | Raw retained | Model retained | Gains/Losses | True downgrades | Retained unsupported transitions | Fallbacks |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for variant, report in payload["variants"].items():
        summary = report["overall"]
        lines.append(
            "| `{}` | {:,.2f} | {:,.2f} | {}% | {}% | {}/{} | {} | {} | {} |".format(
                variant,
                summary["governed_net_vs_smart"],
                summary["governed_model_backed_net_vs_smart"],
                summary["raw_savings_retained_pct"],
                summary["model_backed_savings_retained_pct"],
                summary["material_gain_count"],
                summary["material_loss_count"],
                summary["true_downgrade_count"],
                summary["retained_governed_true_transition_to_unsupported_count"],
                summary["fallback_count"],
            )
        )
    lines.extend([
        "",
        "## Governance Views",
        "",
        "| Variant | View | Cases | Net vs smart | Model-backed net | Gains/Losses | True downgrades | Retained unsupported transitions | Raw avg sim delta | Governed retained avg sim delta |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for variant, report in payload["variants"].items():
        for view in ("automated_safe", "analyst_assisted", "fallback_blocked"):
            summary = report["view_summaries"][view]["overall"]
            lines.append(
                "| `{}` | `{}` | {} | {:,.2f} | {:,.2f} | {}/{} | {} | {} | {} | {} |".format(
                    variant,
                    view,
                    summary["case_count"],
                    summary["governed_net_vs_smart"],
                    summary["governed_model_backed_net_vs_smart"],
                    summary["material_gain_count"],
                    summary["material_loss_count"],
                    summary["true_downgrade_count"],
                    summary["retained_governed_true_transition_to_unsupported_count"],
                    summary["raw_average_similarity_delta"],
                    summary["governed_retained_average_similarity_delta"],
                )
            )
    lines.extend(["", "## Metric Clarifications", ""])
    lines.extend([
        "- `raw_average_similarity_delta` measures the ungated rerank movement before fallback.",
        "- `governed_retained_average_similarity_delta` measures only cases that keep the rerank result after fallback.",
        "- `raw_included_comp_count_collapse_count` is the raw collapse signal; `caught_prevented_included_comp_count_collapse_count` is the portion caught by fallback.",
        "- `raw_true_transition_to_unsupported_count` is pre-fallback; `retained_governed_true_transition_to_unsupported_count` is the post-governance count.",
        f"- Segment-set source: `{payload['segment_sets']['source']}`. {payload['segment_sets']['disclosure']}",
    ])
    lines.extend(["", "## Key Interpretation", ""])
    lines.append(payload["production_readiness_interpretation"]["primary_recommendation"])
    lines.append("")
    lines.append(
        "Governed fallback sends dangerous, unavailable, collapsed, low-benefit, or insufficient-evidence cases back to similarity_top_100."
    )
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    args = build_parser().parse_args()
    source_payload = json.loads(args.source_artifact.read_text())
    governance_payload = (
        json.loads(args.governance_artifact.read_text())
        if args.governance_artifact
        else None
    )
    payload = build_payload(
        source_payload=source_payload,
        source_path=args.source_artifact,
        governance_path=args.governance_artifact,
        governance_payload=governance_payload,
    )
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    base = args.output_dir / f"unequal_roll_full_reranking_governed_fallback_{timestamp}"
    write_json(base.with_suffix(".json"), payload)
    write_csv(base.with_suffix(".csv"), payload["case_rows"])
    write_md(base.with_suffix(".md"), payload)
    print(base.with_suffix(".json"))
    print(base.with_suffix(".csv"))
    print(base.with_suffix(".md"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
