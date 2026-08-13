from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

import psycopg
from psycopg.rows import dict_row

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
from infra.scripts.run_unequal_roll_harris_value_tier_sensitivity_experiments import (  # noqa: E402
    EXPERIMENT_STRATEGIES,
    ExperimentStrategy,
    build_guardrail_summary,
    recommend_strategy,
)

HARRIS_PRIORITY_NEIGHBORHOODS = ["215.03", "229.60", "222.02", "7137.00", "7153.00", "790.00"]
HARRIS_CONTROL_NEIGHBORHOODS = ["8309.06", "7068.04", "2215.00"]
MATERIAL_TAXPAYER_CHANGE_THRESHOLD = 1000.0
VALUE_PER_SF_OUTLIER_THRESHOLD = 5.0
PRICE_TIER_MEDIAN_INCREASE_THRESHOLD = 5000.0
SIMILARITY_MARGIN_THRESHOLD = 0.02

INPUT_CONTRACT = {
    "script_mode": "broader_no_persist_smart_harvest_validation",
    "full_candidate_reranking": False,
    "post_selection_swap_only": True,
    "production_scoring_penalty": False,
    "baseline_strategy": "similarity_top_100",
    "notes": [
        "This script is validation-only and no-persist.",
        "It samples a bounded Harris/Fort Bend cohort with read-only selection queries.",
        "It compares current_order_cap_100 and similarity_top_100 replays, then runs post-selection swap sensitivity strategies against the smart baseline.",
        "It does not rerank the full candidate universe and does not change production scoring or runtime defaults.",
    ],
}


@dataclass(frozen=True)
class SelectedSubject:
    county_id: str
    account_number: str
    neighborhood_code: str
    selection_source: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run broader no-persist smart-harvest validation across bounded Harris and Fort Bend "
            "cohorts using current vs similarity_top_100 replay comparisons plus post-selection "
            "swap sensitivity experiments."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    parser.add_argument("--output-dir", type=Path, default=Path("/private/tmp"))
    parser.add_argument("--selection-mode", choices=["default", "targeted"], default="default")
    parser.add_argument("--harris-neighborhoods", default=None)
    parser.add_argument("--fort-bend-neighborhoods", default=None)
    parser.add_argument("--max-subjects-per-county", type=int, default=12)
    parser.add_argument("--max-subjects-per-neighborhood", type=int, default=4)
    parser.add_argument("--max-total-subjects", type=int, default=24)
    parser.add_argument("--smoke-limit", type=int, default=None)
    parser.add_argument("--fort-bend-neighborhood-limit", type=int, default=4)
    parser.add_argument("--fort-bend-min-neighborhood-count", type=int, default=20)
    return parser


def parse_neighborhood_override(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def validate_selection_options(
    *,
    selection_mode: str,
    harris_neighborhood_override: list[str],
    fort_bend_neighborhood_override: list[str],
) -> None:
    has_harris_override = bool(harris_neighborhood_override)
    has_fort_bend_override = bool(fort_bend_neighborhood_override)
    if selection_mode == "targeted":
        if not has_harris_override or not has_fort_bend_override:
            raise ValueError(
                "targeted selection mode requires both --harris-neighborhoods and --fort-bend-neighborhoods"
            )
        return
    if has_harris_override or has_fort_bend_override:
        raise ValueError(
            "explicit neighborhood overrides require --selection-mode targeted"
        )


def connect_read_only(database_url: str) -> Any:
    connection = psycopg.connect(database_url, row_factory=dict_row)
    connection.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
    connection.execute("SET SESSION max_parallel_workers_per_gather = 0")
    return connection


def _read_only_query(cursor: Any, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")
    cursor.execute(query, params)
    return list(cursor.fetchall())


def build_value_spread_order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            _as_float(row.get("appraised_value")) or 0.0,
            str(row.get("account_number") or ""),
        ),
    )
    if not sorted_rows:
        return []
    ordered: list[dict[str, Any]] = []
    used: set[int] = set()
    mid = (len(sorted_rows) - 1) // 2
    left = mid
    right = mid + 1
    low = 0
    high = len(sorted_rows) - 1
    while len(ordered) < len(sorted_rows):
        for index in (left, right, low, high):
            if 0 <= index < len(sorted_rows) and index not in used:
                ordered.append(sorted_rows[index])
                used.add(index)
        left -= 1
        right += 1
        low += 1
        high -= 1
    return ordered


def interleave_neighborhood_subjects(
    rows_by_neighborhood: dict[str, list[SelectedSubject]],
    neighborhood_order: list[str],
) -> list[SelectedSubject]:
    queues = {key: list(rows_by_neighborhood.get(key) or []) for key in neighborhood_order}
    interleaved: list[SelectedSubject] = []
    while True:
        emitted = False
        for neighborhood_code in neighborhood_order:
            queue = queues.get(neighborhood_code) or []
            if queue:
                interleaved.append(queue.pop(0))
                emitted = True
        if not emitted:
            break
    return interleaved


def select_ranked_subjects(
    cursor: Any,
    *,
    county_id: str,
    requested_tax_year: int,
    neighborhoods: list[str],
    max_subjects_per_neighborhood: int,
    limit_total: int,
    selection_source: str,
) -> list[SelectedSubject]:
    if not neighborhoods or limit_total <= 0:
        return []
    rows = _read_only_query(
        cursor,
        """
        SELECT county_id, account_number, neighborhood_code, appraised_value
        FROM parcel_summary_view
        WHERE county_id = %s
          AND tax_year = %s
          AND property_type_code = 'sfr'
          AND COALESCE(neighborhood_code, '') <> ''
          AND neighborhood_code = ANY(%s)
          AND COALESCE(appraised_value, 0) > 0
          AND COALESCE(living_area_sf, 0) > 0
        ORDER BY neighborhood_code, appraised_value NULLS LAST, account_number
        """,
        (
            county_id,
            requested_tax_year,
            neighborhoods,
        ),
    )
    rows_by_neighborhood: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_neighborhood[str(row["neighborhood_code"])].append(row)
    selected_by_neighborhood: dict[str, list[SelectedSubject]] = {}
    for neighborhood_code in neighborhoods:
        ordered_rows = build_value_spread_order(rows_by_neighborhood.get(neighborhood_code) or [])
        selected_by_neighborhood[neighborhood_code] = [
            SelectedSubject(
                county_id=str(row["county_id"]),
                account_number=str(row["account_number"]),
                neighborhood_code=str(row["neighborhood_code"]),
                selection_source=selection_source,
            )
            for row in ordered_rows[:max_subjects_per_neighborhood]
        ]
    return interleave_neighborhood_subjects(selected_by_neighborhood, neighborhoods)[:limit_total]


def discover_fort_bend_neighborhoods(
    cursor: Any,
    *,
    requested_tax_year: int,
    fort_bend_neighborhood_limit: int,
    fort_bend_min_neighborhood_count: int,
) -> list[str]:
    rows = _read_only_query(
        cursor,
        """
        SELECT
          neighborhood_code,
          COUNT(*) AS subject_count,
          COUNT(*) FILTER (WHERE COALESCE(land_sf, 0) > 0) AS land_positive_count
        FROM parcel_summary_view
        WHERE county_id = 'fort_bend'
          AND tax_year = %s
          AND property_type_code = 'sfr'
          AND COALESCE(neighborhood_code, '') <> ''
          AND COALESCE(appraised_value, 0) > 0
          AND COALESCE(living_area_sf, 0) > 0
        GROUP BY neighborhood_code
        HAVING COUNT(*) >= %s
        ORDER BY land_positive_count DESC, subject_count DESC, neighborhood_code
        LIMIT %s
        """,
        (
            requested_tax_year,
            fort_bend_min_neighborhood_count,
            fort_bend_neighborhood_limit,
        ),
    )
    return [str(row["neighborhood_code"]) for row in rows]


def select_validation_cohort(
    cursor: Any,
    *,
    selection_mode: str,
    harris_neighborhood_override: list[str],
    fort_bend_neighborhood_override: list[str],
    requested_tax_year: int,
    max_subjects_per_county: int,
    max_subjects_per_neighborhood: int,
    max_total_subjects: int,
    smoke_limit: int | None,
    fort_bend_neighborhood_limit: int,
    fort_bend_min_neighborhood_count: int,
) -> tuple[list[SelectedSubject], dict[str, Any]]:
    harris_neighborhoods = (
        harris_neighborhood_override
        if harris_neighborhood_override
        else HARRIS_PRIORITY_NEIGHBORHOODS + HARRIS_CONTROL_NEIGHBORHOODS
    )
    harris_subjects = select_ranked_subjects(
        cursor,
        county_id="harris",
        requested_tax_year=requested_tax_year,
        neighborhoods=harris_neighborhoods,
        max_subjects_per_neighborhood=max_subjects_per_neighborhood,
        limit_total=max_subjects_per_county,
        selection_source=(
            "harris_targeted_neighborhoods"
            if harris_neighborhood_override
            else "harris_seeded_neighborhoods"
        ),
    )
    fort_bend_neighborhoods = (
        fort_bend_neighborhood_override
        if fort_bend_neighborhood_override
        else discover_fort_bend_neighborhoods(
            cursor,
            requested_tax_year=requested_tax_year,
            fort_bend_neighborhood_limit=fort_bend_neighborhood_limit,
            fort_bend_min_neighborhood_count=fort_bend_min_neighborhood_count,
        )
    )
    fort_bend_subjects = select_ranked_subjects(
        cursor,
        county_id="fort_bend",
        requested_tax_year=requested_tax_year,
        neighborhoods=fort_bend_neighborhoods,
        max_subjects_per_neighborhood=max_subjects_per_neighborhood,
        limit_total=max_subjects_per_county,
        selection_source=(
            "fort_bend_targeted_neighborhoods"
            if fort_bend_neighborhood_override
            else "fort_bend_land_repaired_neighborhoods"
        ),
    )
    combined = merge_balanced_subjects(harris_subjects, fort_bend_subjects)
    if smoke_limit is not None:
        combined = combined[:smoke_limit]
    else:
        combined = combined[:max_total_subjects]
    summary = {
        "selection_mode": selection_mode,
        "requested_tax_year": requested_tax_year,
        "max_subjects_per_county": max_subjects_per_county,
        "max_subjects_per_neighborhood": max_subjects_per_neighborhood,
        "max_total_subjects": max_total_subjects,
        "smoke_limit": smoke_limit,
        "selection_rank_strategy": "deterministic_round_robin_with_value_spread",
        "selection_criteria": [
            "parcel_summary_view read-only sampling",
            "property_type_code = sfr",
            "appraised_value > 0",
            "living_area_sf > 0",
            "nonblank neighborhood_code",
            "Harris seeded neighborhoods are sampled fairly via per-neighborhood caps plus deterministic round-robin interleaving before truncation.",
            "Within each neighborhood, subject selection uses a deterministic appraised-value spread rather than appraised_value DESC only.",
            (
                "Targeted segment validation uses explicit neighborhood overrides and is not a representative countywide cohort."
                if selection_mode == "targeted"
                else "Fort Bend neighborhoods are intentionally selected for strong land_sf coverage and form a land-repaired validation cohort, not a countywide-representative sample."
            ),
        ],
        "harris_seeded_neighborhoods": harris_neighborhoods,
        "fort_bend_selected_neighborhoods": fort_bend_neighborhoods,
        "targeted_harris_neighborhoods": harris_neighborhood_override,
        "targeted_fort_bend_neighborhoods": fort_bend_neighborhood_override,
        "fort_bend_selection_bias": {
            "intentionally_land_repaired_biased": not bool(fort_bend_neighborhood_override),
            "countywide_representative": False,
            "disclosure": (
                "Explicit Fort Bend targeted neighborhoods were supplied; this is a targeted segment validation cohort and not a countywide-representative sample."
                if fort_bend_neighborhood_override
                else "Fort Bend neighborhoods are intentionally selected for strong land_sf coverage after the repaired 2026 land baseline; results should be interpreted as a land-repaired validation cohort rather than a countywide-representative sample."
            ),
        },
        "selected_subject_count": len(combined),
    }
    return combined, summary


def merge_balanced_subjects(
    harris_subjects: list[SelectedSubject],
    fort_bend_subjects: list[SelectedSubject],
) -> list[SelectedSubject]:
    merged: list[SelectedSubject] = []
    max_len = max(len(harris_subjects), len(fort_bend_subjects))
    for index in range(max_len):
        if index < len(harris_subjects):
            merged.append(harris_subjects[index])
        if index < len(fort_bend_subjects):
            merged.append(fort_bend_subjects[index])
    return merged


def fetch_neighborhood_median_psf(
    cursor: Any,
    *,
    requested_tax_year: int,
    neighborhoods_by_county: dict[str, list[str]],
) -> dict[tuple[str, str], float]:
    medians: dict[tuple[str, str], float] = {}
    for county_id, neighborhoods in neighborhoods_by_county.items():
        if not neighborhoods:
            continue
        rows = _read_only_query(
            cursor,
            """
            SELECT
              county_id,
              neighborhood_code,
              percentile_cont(0.5) WITHIN GROUP (
                ORDER BY appraised_value / NULLIF(living_area_sf, 0)
              ) AS median_appraised_psf
            FROM parcel_summary_view
            WHERE county_id = %s
              AND tax_year = %s
              AND property_type_code = 'sfr'
              AND neighborhood_code = ANY(%s)
              AND COALESCE(appraised_value, 0) > 0
              AND COALESCE(living_area_sf, 0) > 0
            GROUP BY county_id, neighborhood_code
            """,
            (county_id, requested_tax_year, neighborhoods),
        )
        for row in rows:
            value = _as_float(row.get("median_appraised_psf"))
            if value is not None:
                medians[(str(row["county_id"]), str(row["neighborhood_code"]))] = value
    return medians


def median_value(values: list[float | None]) -> float | None:
    usable = [value for value in values if value is not None]
    return round(float(median(usable)), 4) if usable else None


def avg_value(values: list[float | None]) -> float | None:
    usable = [value for value in values if value is not None]
    return round(sum(usable) / len(usable), 4) if usable else None


def count_high_psf_outliers(rows: list[dict[str, Any]], neighborhood_median_psf: float | None) -> int:
    if neighborhood_median_psf is None:
        return 0
    count = 0
    for row in rows:
        raw_psf = _as_float(row.get("raw_appraised_value_per_sf"))
        if raw_psf is not None and raw_psf - neighborhood_median_psf >= VALUE_PER_SF_OUTLIER_THRESHOLD:
            count += 1
    return count


def summarize_included_rows(
    rows: list[dict[str, Any]],
    *,
    neighborhood_median_psf: float | None,
) -> dict[str, Any]:
    raw_psfs = [_as_float(row.get("raw_appraised_value_per_sf")) for row in rows]
    adjusted_psfs = [_as_float(row.get("adjusted_appraised_value_per_sf")) for row in rows]
    similarities = [_as_float(row.get("similarity_score")) for row in rows]
    adjusted_values = [_as_float(row.get("adjusted_appraised_value")) for row in rows]
    median_raw_psf = median_value(raw_psfs)
    return {
        "count": len(rows),
        "avg_similarity_score": avg_value(similarities),
        "median_raw_appraised_value_per_sf": median_raw_psf,
        "median_adjusted_value_per_sf": median_value(adjusted_psfs),
        "median_adjusted_value": median_value(adjusted_values),
        "median_distance_from_neighborhood_median_psf": None
        if neighborhood_median_psf is None or median_raw_psf is None
        else round(median_raw_psf - neighborhood_median_psf, 4),
        "high_value_per_sf_outlier_count": count_high_psf_outliers(rows, neighborhood_median_psf),
    }


def summarize_lower_value_alternative(base_swap_result: dict[str, Any]) -> dict[str, Any]:
    automation = dict(base_swap_result.get("automation_assessment") or {})
    accepted_swaps = list(base_swap_result.get("accepted_swaps") or [])
    return {
        "classification": automation.get("automation_status") or "no_safe_opportunity",
        "estimated_reduction_impact": automation.get("reduction_gain_vs_smart"),
        "accepted_swap_count": len(accepted_swaps),
        "top_swapped_in_accounts": [
            str(row.get("swapped_in_candidate_parcel_id") or "") for row in accepted_swaps[:3]
        ],
    }


def derive_trigger_labels(
    *,
    current_included: dict[str, Any],
    smart_included: dict[str, Any],
    adjusted_median_delta: float | None,
    similarity_delta: float | None,
    lower_value_available: bool,
) -> list[str]:
    labels: list[str] = []
    raw_psf_delta = (
        (_as_float(smart_included.get("median_raw_appraised_value_per_sf")) or 0.0)
        - (_as_float(current_included.get("median_raw_appraised_value_per_sf")) or 0.0)
    )
    adjusted_psf_delta = (
        (_as_float(smart_included.get("median_adjusted_value_per_sf")) or 0.0)
        - (_as_float(current_included.get("median_adjusted_value_per_sf")) or 0.0)
    )
    smart_outlier_count = int(smart_included.get("high_value_per_sf_outlier_count") or 0)
    if (
        raw_psf_delta >= VALUE_PER_SF_OUTLIER_THRESHOLD
        or adjusted_psf_delta >= VALUE_PER_SF_OUTLIER_THRESHOLD
        or smart_outlier_count > 0
    ):
        labels.append("value_per_sf_outlier_risk")
    if (
        (_as_float(adjusted_median_delta) or 0.0) >= PRICE_TIER_MEDIAN_INCREASE_THRESHOLD
        and (
            raw_psf_delta >= VALUE_PER_SF_OUTLIER_THRESHOLD
            or (
                (_as_float(smart_included.get("median_distance_from_neighborhood_median_psf")) or 0.0)
                - (_as_float(current_included.get("median_distance_from_neighborhood_median_psf")) or 0.0)
            )
            >= VALUE_PER_SF_OUTLIER_THRESHOLD
        )
    ):
        labels.append("possible_price_tier_drift")
    if (
        (_as_float(similarity_delta) or 0.0) <= SIMILARITY_MARGIN_THRESHOLD
        and (_as_float(adjusted_median_delta) or 0.0) >= PRICE_TIER_MEDIAN_INCREASE_THRESHOLD
    ):
        labels.append("marginal_similarity_high_value_tradeoff")
    if lower_value_available:
        labels.append("lower_value_credible_available")
    return labels


def should_trigger_strategy(
    *,
    labels: list[str],
    lower_value_available: bool,
    strategy: ExperimentStrategy,
) -> tuple[bool, list[str]]:
    matched = [label for label in strategy.trigger_labels if label in labels]
    if strategy.requires_lower_value_candidate and not lower_value_available:
        return False, matched
    if not strategy.trigger_labels:
        return lower_value_available, matched
    return bool(matched), matched


def summarize_strategy_result(
    *,
    strategy: ExperimentStrategy,
    subject: SelectedSubject,
    current_result: dict[str, Any],
    smart_result: dict[str, Any],
    experiment_result: dict[str, Any] | None,
    triggered: bool,
    trigger_labels: list[str],
) -> dict[str, Any]:
    result = experiment_result or smart_result
    smart_reduction = _as_float(smart_result.get("requested_reduction_amount")) or 0.0
    current_reduction = _as_float(current_result.get("requested_reduction_amount")) or 0.0
    result_reduction = _as_float(result.get("requested_reduction_amount")) or 0.0
    automation = dict(result.get("automation_assessment") or {})
    return {
        "strategy_key": strategy.key,
        "strategy_report_label": strategy.report_label,
        "subject_account": subject.account_number,
        "county_id": subject.county_id,
        "neighborhood_code": subject.neighborhood_code,
        "triggered": triggered,
        "trigger_labels": trigger_labels,
        "comparison_baseline": "similarity_top_100",
        "experiment_method": "post_selection_swap_recompute",
        "trigger_signal_only": True,
        "requested_reduction_amount": result.get("requested_reduction_amount"),
        "taxpayer_delta_vs_smart": round(result_reduction - smart_reduction, 2),
        "taxpayer_delta_vs_current": round(result_reduction - current_reduction, 2),
        "adjusted_median_delta_vs_smart": round(
            (_as_float(result.get("requested_roll_value")) or 0.0)
            - (_as_float(smart_result.get("requested_roll_value")) or 0.0),
            2,
        ),
        "support_status_drift": str(result.get("support_status") or "")
        != str(smart_result.get("support_status") or ""),
        "final_status_drift": str(result.get("final_value_status") or "")
        != str(smart_result.get("final_value_status") or ""),
        "included_comp_count_delta_vs_smart": (result.get("included_comp_count") or 0)
        - (smart_result.get("included_comp_count") or 0),
        "review_heavy_delta_vs_smart": (result.get("excluded_review_heavy_count") or 0)
        - (smart_result.get("excluded_review_heavy_count") or 0),
        "likely_exclude_delta_vs_smart": (result.get("excluded_likely_exclude_count") or 0)
        - (smart_result.get("excluded_likely_exclude_count") or 0),
        "automation_assessment": automation,
        "recovery_source_explanation": strategy.recovery_source if triggered else "no_strategy_trigger",
    }


def build_subject_validation_row(
    *,
    subject: SelectedSubject,
    current_result: dict[str, Any],
    smart_result: dict[str, Any],
    neighborhood_median_psf: float | None,
    base_swap_result: dict[str, Any] | None,
    strategy_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    current_detail = dict(current_result.get("final_value_detail_json") or {})
    smart_detail = dict(smart_result.get("final_value_detail_json") or {})
    current_included_rows = list(current_detail.get("included_comp_rows") or [])
    smart_included_rows = list(smart_detail.get("included_comp_rows") or [])

    if str(current_result.get("replay_status") or "") != "completed" or str(
        smart_result.get("replay_status") or ""
    ) != "completed":
        return {
            "county_id": subject.county_id,
            "subject_account": subject.account_number,
            "neighborhood_code": subject.neighborhood_code,
            "selection_source": subject.selection_source,
            "current_replay_status": current_result.get("replay_status"),
            "smart_replay_status": smart_result.get("replay_status"),
            "comparison_ready": False,
            "requested_reduction_change_smart_vs_current": None,
            "similarity_delta_smart_vs_current": None,
            "base_lower_value_classification": None,
            "trigger_labels": [],
            "strategy_results": strategy_rows,
        }

    current_included = summarize_included_rows(
        current_included_rows,
        neighborhood_median_psf=neighborhood_median_psf,
    )
    smart_included = summarize_included_rows(
        smart_included_rows,
        neighborhood_median_psf=neighborhood_median_psf,
    )
    requested_reduction_change = round(
        (_as_float(smart_result.get("requested_reduction_amount")) or 0.0)
        - (_as_float(current_result.get("requested_reduction_amount")) or 0.0),
        2,
    )
    adjusted_median_delta = round(
        (_as_float(smart_result.get("requested_roll_value")) or 0.0)
        - (_as_float(current_result.get("requested_roll_value")) or 0.0),
        2,
    )
    similarity_delta = round(
        (_as_float(smart_included.get("avg_similarity_score")) or 0.0)
        - (_as_float(current_included.get("avg_similarity_score")) or 0.0),
        4,
    )
    lower_value_summary = summarize_lower_value_alternative(base_swap_result or {})
    lower_value_available = lower_value_summary["classification"] in {
        "safe_automated_candidate",
        "manual_review_only",
    }
    labels = derive_trigger_labels(
        current_included=current_included,
        smart_included=smart_included,
        adjusted_median_delta=adjusted_median_delta,
        similarity_delta=similarity_delta,
        lower_value_available=lower_value_available,
    )
    return {
        "county_id": subject.county_id,
        "subject_account": subject.account_number,
        "neighborhood_code": subject.neighborhood_code,
        "selection_source": subject.selection_source,
        "current_replay_status": current_result.get("replay_status"),
        "smart_replay_status": smart_result.get("replay_status"),
        "comparison_ready": True,
        "current_requested_reduction_amount": current_result.get("requested_reduction_amount"),
        "smart_requested_reduction_amount": smart_result.get("requested_reduction_amount"),
        "requested_reduction_change_smart_vs_current": requested_reduction_change,
        "current_requested_roll_value": current_result.get("requested_roll_value"),
        "smart_requested_roll_value": smart_result.get("requested_roll_value"),
        "adjusted_median_delta_smart_vs_current": adjusted_median_delta,
        "current_included_comp_count": current_result.get("included_comp_count"),
        "smart_included_comp_count": smart_result.get("included_comp_count"),
        "included_comp_count_delta": (smart_result.get("included_comp_count") or 0)
        - (current_result.get("included_comp_count") or 0),
        "similarity_delta_smart_vs_current": similarity_delta,
        "review_heavy_delta": (smart_result.get("excluded_review_heavy_count") or 0)
        - (current_result.get("excluded_review_heavy_count") or 0),
        "likely_exclude_delta": (smart_result.get("excluded_likely_exclude_count") or 0)
        - (current_result.get("excluded_likely_exclude_count") or 0),
        "current_support_status": current_result.get("support_status"),
        "smart_support_status": smart_result.get("support_status"),
        "support_status_drift": str(smart_result.get("support_status") or "")
        != str(current_result.get("support_status") or ""),
        "current_final_value_status": current_result.get("final_value_status"),
        "smart_final_value_status": smart_result.get("final_value_status"),
        "final_status_drift": str(smart_result.get("final_value_status") or "")
        != str(current_result.get("final_value_status") or ""),
        "no_reduction_change_flag": (
            (_as_float(current_result.get("requested_reduction_amount")) or 0.0) == 0.0
        )
        != (
            (_as_float(smart_result.get("requested_reduction_amount")) or 0.0) == 0.0
        ),
        "current_included_summary": current_included,
        "smart_included_summary": smart_included,
        "neighborhood_median_appraised_psf": neighborhood_median_psf,
        "base_lower_value_classification": lower_value_summary["classification"],
        "base_lower_value_estimated_reduction_impact": lower_value_summary[
            "estimated_reduction_impact"
        ],
        "trigger_labels": labels,
        "strategy_results": strategy_rows,
    }


def summarize_strategy_collection(strategy: ExperimentStrategy, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "strategy_key": strategy.key,
        "strategy_report_label": strategy.report_label,
        "comparison_baseline": "similarity_top_100",
        "experiment_method": "post_selection_swap_recompute",
        "trigger_signal_only": True,
        "cases_evaluated": len(rows),
        "cases_triggered": sum(1 for row in rows if row.get("triggered")),
        "taxpayer_reduction_recovered": round(
            sum(max(0.0, _as_float(row.get("taxpayer_delta_vs_smart")) or 0.0) for row in rows), 2
        ),
        "taxpayer_reduction_lost": round(
            sum(min(0.0, _as_float(row.get("taxpayer_delta_vs_smart")) or 0.0) for row in rows), 2
        ),
        "net_taxpayer_impact": round(
            sum(_as_float(row.get("taxpayer_delta_vs_smart")) or 0.0 for row in rows), 2
        ),
        "support_status_drift_count": sum(1 for row in rows if row.get("support_status_drift")),
        "final_status_drift_count": sum(1 for row in rows if row.get("final_status_drift")),
        "automation_assessment_counts": build_automation_counts(rows),
    }


def build_automation_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
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


def build_transition_counts(
    rows: list[dict[str, Any]],
    *,
    from_key: str,
    to_key: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        if not row.get("comparison_ready"):
            continue
        source = str(row.get(from_key) or "")
        target = str(row.get(to_key) or "")
        if not source or not target or source == target:
            continue
        transition = f"{source} -> {target}"
        counts[transition] = counts.get(transition, 0) + 1
    return dict(sorted(counts.items()))


def build_group_summary(
    rows: list[dict[str, Any]],
    *,
    label: str,
) -> dict[str, Any]:
    comparable = [row for row in rows if row.get("comparison_ready")]
    taxpayer_changes = [
        _as_float(row.get("requested_reduction_change_smart_vs_current")) for row in comparable
    ]
    usable_changes = [value for value in taxpayer_changes if value is not None]
    total_loss = round(sum(value for value in usable_changes if value < 0), 2)
    strategy_recovery = 0.0
    strategy_breakdown: dict[str, dict[str, Any]] = {}
    for strategy in EXPERIMENT_STRATEGIES:
        per_strategy = []
        for row in comparable:
            strategy_row = next(
                (item for item in row.get("strategy_results") or [] if item.get("strategy_key") == strategy.key),
                None,
            )
            if strategy_row is not None:
                per_strategy.append(strategy_row)
        summary = summarize_strategy_collection(strategy, per_strategy)
        strategy_breakdown[strategy.key] = summary
        strategy_recovery = max(strategy_recovery, summary["taxpayer_reduction_recovered"])

    return {
        "label": label,
        "cohort_count": len(rows),
        "comparison_ready_count": len(comparable),
        "net_taxpayer_impact_current_vs_smart": round(sum(usable_changes), 2) if usable_changes else 0.0,
        "total_taxpayer_gained": round(sum(max(0.0, value) for value in usable_changes), 2),
        "total_taxpayer_lost": total_loss,
        "material_gain_count": sum(
            1 for value in usable_changes if value >= MATERIAL_TAXPAYER_CHANGE_THRESHOLD
        ),
        "material_loss_count": sum(
            1 for value in usable_changes if value <= -MATERIAL_TAXPAYER_CHANGE_THRESHOLD
        ),
        "median_taxpayer_change": round(float(median(usable_changes)), 2) if usable_changes else None,
        "average_similarity_delta": avg_value(
            [row.get("similarity_delta_smart_vs_current") for row in comparable]
        ),
        "included_comp_count_delta": avg_value(
            [row.get("included_comp_count_delta") for row in comparable]
        ),
        "review_heavy_delta": avg_value([row.get("review_heavy_delta") for row in comparable]),
        "likely_exclude_delta": avg_value([row.get("likely_exclude_delta") for row in comparable]),
        "support_status_drift_count": sum(1 for row in comparable if row.get("support_status_drift")),
        "support_status_transition_counts": build_transition_counts(
            comparable,
            from_key="current_support_status",
            to_key="smart_support_status",
        ),
        "final_status_drift_count": sum(1 for row in comparable if row.get("final_status_drift")),
        "final_status_transition_counts": build_transition_counts(
            comparable,
            from_key="current_final_value_status",
            to_key="smart_final_value_status",
        ),
        "no_reduction_change_count": sum(1 for row in comparable if row.get("no_reduction_change_flag")),
        "post_selection_recovery_amount": round(strategy_recovery, 2),
        "recovery_as_pct_of_smart_harvest_taxpayer_loss": None
        if total_loss == 0.0
        else round(strategy_recovery / abs(total_loss), 4),
        "strategy_specific_summary": strategy_breakdown,
    }


def flatten_strategy_rows(subject_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened = []
    for row in subject_rows:
        for strategy_row in row.get("strategy_results") or []:
            flattened.append(strategy_row)
    return flattened


def top_subjects(rows: list[dict[str, Any]], *, reverse: bool, limit: int = 10) -> list[dict[str, Any]]:
    comparable = [row for row in rows if row.get("comparison_ready")]
    ranked = sorted(
        comparable,
        key=lambda row: (_as_float(row.get("requested_reduction_change_smart_vs_current")) or 0.0),
        reverse=reverse,
    )
    output = []
    for row in ranked[:limit]:
        output.append(
            {
                "county_id": row.get("county_id"),
                "subject_account": row.get("subject_account"),
                "neighborhood_code": row.get("neighborhood_code"),
                "requested_reduction_change_smart_vs_current": row.get(
                    "requested_reduction_change_smart_vs_current"
                ),
                "similarity_delta_smart_vs_current": row.get("similarity_delta_smart_vs_current"),
            }
        )
    return output


def build_payload(
    *,
    selection_summary: dict[str, Any],
    subject_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    county_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    neighborhood_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in subject_rows:
        county_groups[str(row.get("county_id") or "")].append(row)
        neighborhood_groups[(str(row.get("county_id") or ""), str(row.get("neighborhood_code") or ""))].append(row)

    county_summary = [
        build_group_summary(rows, label=county_id) for county_id, rows in sorted(county_groups.items())
    ]
    neighborhood_summary = [
        {
            "county_id": county_id,
            "neighborhood_code": neighborhood_code,
            **build_group_summary(rows, label=f"{county_id}:{neighborhood_code}"),
        }
        for (county_id, neighborhood_code), rows in sorted(neighborhood_groups.items())
    ]
    strategy_rows = flatten_strategy_rows(subject_rows)
    overall_strategy_summary = [
        summarize_strategy_collection(
            strategy,
            [row for row in strategy_rows if row.get("strategy_key") == strategy.key],
        )
        for strategy in EXPERIMENT_STRATEGIES
    ]
    comparable = [row for row in subject_rows if row.get("comparison_ready")]
    payload = {
        "generated_at": datetime.now().isoformat(),
        "input_contract": INPUT_CONTRACT,
        "guardrails": build_guardrail_summary(),
        "cohort_selection_summary": selection_summary,
        "cohort_size": len(subject_rows),
        "county_summary": county_summary,
        "neighborhood_summary": neighborhood_summary,
        "current_vs_similarity_top_100_summary": build_group_summary(comparable, label="overall"),
        "taxpayer_impact_summary": {
            "top_helped_cases": top_subjects(subject_rows, reverse=True),
            "top_harmed_cases": top_subjects(subject_rows, reverse=False),
        },
        "defensibility_support_drift_summary": {
            "support_status_drift_count": sum(
                1 for row in comparable if row.get("support_status_drift")
            ),
            "support_status_transition_counts": build_transition_counts(
                comparable,
                from_key="current_support_status",
                to_key="smart_support_status",
            ),
            "final_status_drift_count": sum(
                1 for row in comparable if row.get("final_status_drift")
            ),
            "final_status_transition_counts": build_transition_counts(
                comparable,
                from_key="current_final_value_status",
                to_key="smart_final_value_status",
            ),
            "loss_with_improved_similarity_cases": [
                {
                    "county_id": row.get("county_id"),
                    "subject_account": row.get("subject_account"),
                    "neighborhood_code": row.get("neighborhood_code"),
                    "requested_reduction_change_smart_vs_current": row.get(
                        "requested_reduction_change_smart_vs_current"
                    ),
                    "similarity_delta_smart_vs_current": row.get("similarity_delta_smart_vs_current"),
                }
                for row in comparable
                if (_as_float(row.get("requested_reduction_change_smart_vs_current")) or 0.0) < 0
                and (_as_float(row.get("similarity_delta_smart_vs_current")) or 0.0) > 0
            ],
        },
        "post_selection_swap_strategy_summary": overall_strategy_summary,
        "cases_with_lower_value_credible_alternatives": [
            {
                "county_id": row.get("county_id"),
                "subject_account": row.get("subject_account"),
                "neighborhood_code": row.get("neighborhood_code"),
                "base_lower_value_classification": row.get("base_lower_value_classification"),
                "base_lower_value_estimated_reduction_impact": row.get(
                    "base_lower_value_estimated_reduction_impact"
                ),
            }
            for row in comparable
            if row.get("base_lower_value_classification") in {"safe_automated_candidate", "manual_review_only"}
        ],
        "automation_assessment_counts": {
            strategy["strategy_key"]: strategy["automation_assessment_counts"]
            for strategy in overall_strategy_summary
        },
        "subject_rows": subject_rows,
        "evidence_backed_findings": build_evidence_findings(county_summary, overall_strategy_summary),
        "heuristic_findings": build_heuristic_findings(),
        "hypotheses_requiring_more_validation": build_hypotheses(),
        "recommendation_for_full_reranking_experiment": recommend_full_reranking(
            county_summary, overall_strategy_summary
        ),
    }
    return payload


def build_evidence_findings(
    county_summary: list[dict[str, Any]], strategy_summary: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows = []
    for county in county_summary:
        rows.append(
            {
                "county_id": county["label"],
                "net_taxpayer_impact_current_vs_smart": county[
                    "net_taxpayer_impact_current_vs_smart"
                ],
                "post_selection_recovery_amount": county["post_selection_recovery_amount"],
                "support_status_drift_count": county["support_status_drift_count"],
                "final_status_drift_count": county["final_status_drift_count"],
            }
        )
    for strategy in strategy_summary:
        rows.append(
            {
                "strategy_key": strategy["strategy_key"],
                "strategy_report_label": strategy["strategy_report_label"],
                "net_taxpayer_impact": strategy["net_taxpayer_impact"],
                "support_status_drift_count": strategy["support_status_drift_count"],
                "final_status_drift_count": strategy["final_status_drift_count"],
            }
        )
    return rows


def build_heuristic_findings() -> list[dict[str, Any]]:
    return [
        {
            "finding": "Value-per-SF outlier and price-tier drift remain trigger labels derived from replay outputs plus neighborhood medians; they are not production scoring penalties.",
        }
    ]


def build_hypotheses() -> list[dict[str, Any]]:
    return [
        {
            "finding": "If broader validation shows stable recovery with low drift across Harris and Fort Bend, the next experiment should test full reranking rather than more post-selection swaps.",
        }
    ]


def recommend_full_reranking(
    county_summary: list[dict[str, Any]], strategy_summary: list[dict[str, Any]]
) -> str:
    if any(
        county["support_status_drift_count"] > 0 or county["final_status_drift_count"] > 0
        for county in county_summary
    ):
        return "do_not_proceed_yet"
    best_strategy = max(
        (strategy["net_taxpayer_impact"] for strategy in strategy_summary),
        default=0.0,
    )
    if best_strategy < 10000:
        return "keep_broader_validation_only"
    return "proceed_to_bounded_full_reranking_experiment"


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
    selection_mode = payload["cohort_selection_summary"]["selection_mode"]
    comparable_count = payload["current_vs_similarity_top_100_summary"]["comparison_ready_count"]
    total_count = payload["cohort_size"]
    non_comparable_count = total_count - comparable_count
    lines = [
        "# Broader Smart-Harvest Validation",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Cohort size: {payload['cohort_size']}",
        f"- Recommendation for full reranking experiment: `{payload['recommendation_for_full_reranking_experiment']}`",
        "",
        "## Method Boundary",
        "- Validation/reporting only",
        "- No DB writes",
        "- No production scoring changes",
        "- Not full candidate reranking",
        "- Post-selection swap only for sensitivity strategies",
        "- Smart harvest remains non-default",
        "- Cohort is bounded and not necessarily representative",
        "",
        "## Cohort Selection Summary",
        f"- Selection mode: `{payload['cohort_selection_summary']['selection_mode']}`",
        f"- Selected subject count: `{payload['cohort_selection_summary']['selected_subject_count']}`",
        f"- Comparison-ready subject count: `{comparable_count}`",
        f"- Not-comparison-ready subject count: `{non_comparable_count}`",
        f"- Selection rank strategy: `{payload['cohort_selection_summary']['selection_rank_strategy']}`",
        (
            f"- Harris targeted neighborhoods: `{payload['cohort_selection_summary']['harris_seeded_neighborhoods']}`"
            if selection_mode == "targeted"
            else f"- Harris seeded neighborhoods: `{payload['cohort_selection_summary']['harris_seeded_neighborhoods']}`"
        ),
        (
            f"- Fort Bend targeted neighborhoods: `{payload['cohort_selection_summary']['fort_bend_selected_neighborhoods']}`"
            if selection_mode == "targeted"
            else f"- Fort Bend selected neighborhoods: `{payload['cohort_selection_summary']['fort_bend_selected_neighborhoods']}`"
        ),
        f"- Targeted Harris neighborhoods: `{payload['cohort_selection_summary']['targeted_harris_neighborhoods']}`",
        f"- Targeted Fort Bend neighborhoods: `{payload['cohort_selection_summary']['targeted_fort_bend_neighborhoods']}`",
        f"- Fort Bend cohort disclosure: `{payload['cohort_selection_summary']['fort_bend_selection_bias']['disclosure']}`",
        "",
        "## County Summary",
    ]
    for row in payload["county_summary"]:
        lines.extend(
            [
                f"### {row['label']}",
                f"- Cohort count: `{row['cohort_count']}`",
                f"- Net taxpayer impact current vs smart: `{row['net_taxpayer_impact_current_vs_smart']}`",
                f"- Post-selection recovery amount: `{row['post_selection_recovery_amount']}`",
                f"- Support drift: `{row['support_status_drift_count']}`",
                f"- Support transitions: `{row['support_status_transition_counts']}`",
                f"- Final drift: `{row['final_status_drift_count']}`",
                f"- Final transitions: `{row['final_status_transition_counts']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Overall Drift Transitions",
            f"- Support transitions: `{payload['defensibility_support_drift_summary']['support_status_transition_counts']}`",
            f"- Final transitions: `{payload['defensibility_support_drift_summary']['final_status_transition_counts']}`",
            "",
        ]
    )
    lines.append("## Strategy Summary")
    for row in payload["post_selection_swap_strategy_summary"]:
        lines.extend(
            [
                f"### {row['strategy_report_label']}",
                f"- Net taxpayer impact: `{row['net_taxpayer_impact']}`",
                f"- Cases triggered: `{row['cases_triggered']}`",
                f"- Automation counts: `{row['automation_assessment_counts']}`",
                "",
            ]
        )
    path.write_text("\n".join(lines))


def write_payload(payload: dict[str, Any], *, output_dir: Path) -> dict[str, str]:
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    stem = output_dir / f"unequal_roll_broader_smart_harvest_validation_{timestamp}"
    json_path = f"{stem}.json"
    csv_path = f"{stem}.csv"
    md_path = f"{stem}.md"
    Path(json_path).write_text(json.dumps(payload, indent=2))
    write_csv(Path(csv_path), payload["subject_rows"])
    write_md(Path(md_path), payload)
    return {"json": json_path, "csv": csv_path, "md": md_path}


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def replay_subject(
    service: UnequalRollNoPersistReplayService,
    conn: Any,
    *,
    subject: SelectedSubject,
    requested_tax_year: int,
    strategy: str,
) -> dict[str, Any]:
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            result = service.replay_subject(
                cursor,
                request=UnequalRollReplayRequest(
                    county_id=subject.county_id,
                    account_number=subject.account_number,
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


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    harris_neighborhood_override = parse_neighborhood_override(args.harris_neighborhoods)
    fort_bend_neighborhood_override = parse_neighborhood_override(args.fort_bend_neighborhoods)
    try:
        validate_selection_options(
            selection_mode=args.selection_mode,
            harris_neighborhood_override=harris_neighborhood_override,
            fort_bend_neighborhood_override=fort_bend_neighborhood_override,
        )
    except ValueError as exc:
        parser.error(str(exc))

    replay_service = UnequalRollNoPersistReplayService()
    tie_service = UnequalRollTaxpayerFavorableTieBreakService()
    with connect_read_only(args.database_url) as conn:
        with conn.cursor() as cursor:
            selected_subjects, selection_summary = select_validation_cohort(
                cursor,
                selection_mode=args.selection_mode,
                harris_neighborhood_override=harris_neighborhood_override,
                fort_bend_neighborhood_override=fort_bend_neighborhood_override,
                requested_tax_year=args.requested_tax_year,
                max_subjects_per_county=args.max_subjects_per_county,
                max_subjects_per_neighborhood=args.max_subjects_per_neighborhood,
                max_total_subjects=args.max_total_subjects,
                smoke_limit=args.smoke_limit,
                fort_bend_neighborhood_limit=args.fort_bend_neighborhood_limit,
                fort_bend_min_neighborhood_count=args.fort_bend_min_neighborhood_count,
            )
            neighborhoods_by_county: dict[str, list[str]] = defaultdict(list)
            for subject in selected_subjects:
                if subject.neighborhood_code not in neighborhoods_by_county[subject.county_id]:
                    neighborhoods_by_county[subject.county_id].append(subject.neighborhood_code)
            neighborhood_medians = fetch_neighborhood_median_psf(
                cursor,
                requested_tax_year=args.requested_tax_year,
                neighborhoods_by_county=neighborhoods_by_county,
            )

        subject_rows: list[dict[str, Any]] = []
        for subject in selected_subjects:
            current_result = replay_subject(
                replay_service,
                conn,
                subject=subject,
                requested_tax_year=args.requested_tax_year,
                strategy=CURRENT_ORDER_CAP_100,
            )
            smart_result = replay_subject(
                replay_service,
                conn,
                subject=subject,
                requested_tax_year=args.requested_tax_year,
                strategy=SIMILARITY_TOP_100,
            )
            base_swap_result: dict[str, Any] | None = None
            strategy_rows: list[dict[str, Any]] = []
            if (
                str(current_result.get("replay_status") or "") == "completed"
                and str(smart_result.get("replay_status") or "") == "completed"
            ):
                base_swap_result = tie_service.simulate(
                    current_result=current_result,
                    smart_result=smart_result,
                    config=TaxpayerFavorableTieBreakConfig(max_swaps=1),
                )
                lower_value_available = summarize_lower_value_alternative(base_swap_result)[
                    "classification"
                ] in {"safe_automated_candidate", "manual_review_only"}
                current_included = summarize_included_rows(
                    list((current_result.get("final_value_detail_json") or {}).get("included_comp_rows") or []),
                    neighborhood_median_psf=neighborhood_medians.get(
                        (subject.county_id, subject.neighborhood_code)
                    ),
                )
                smart_included = summarize_included_rows(
                    list((smart_result.get("final_value_detail_json") or {}).get("included_comp_rows") or []),
                    neighborhood_median_psf=neighborhood_medians.get(
                        (subject.county_id, subject.neighborhood_code)
                    ),
                )
                labels = derive_trigger_labels(
                    current_included=current_included,
                    smart_included=smart_included,
                    adjusted_median_delta=(
                        (_as_float(smart_result.get("requested_roll_value")) or 0.0)
                        - (_as_float(current_result.get("requested_roll_value")) or 0.0)
                    ),
                    similarity_delta=(
                        (_as_float(smart_included.get("avg_similarity_score")) or 0.0)
                        - (_as_float(current_included.get("avg_similarity_score")) or 0.0)
                    ),
                    lower_value_available=lower_value_available,
                )
                for strategy in EXPERIMENT_STRATEGIES:
                    triggered, trigger_labels = should_trigger_strategy(
                        labels=labels,
                        lower_value_available=lower_value_available,
                        strategy=strategy,
                    )
                    experiment_result = (
                        tie_service.simulate(
                            current_result=current_result,
                            smart_result=smart_result,
                            config=strategy.config,
                        )
                        if triggered
                        else None
                    )
                    strategy_rows.append(
                        summarize_strategy_result(
                            strategy=strategy,
                            subject=subject,
                            current_result=current_result,
                            smart_result=smart_result,
                            experiment_result=experiment_result,
                            triggered=triggered,
                            trigger_labels=trigger_labels,
                        )
                    )
            subject_rows.append(
                build_subject_validation_row(
                    subject=subject,
                    current_result=current_result,
                    smart_result=smart_result,
                    neighborhood_median_psf=neighborhood_medians.get(
                        (subject.county_id, subject.neighborhood_code)
                    ),
                    base_swap_result=base_swap_result,
                    strategy_rows=strategy_rows,
                )
            )

    payload = build_payload(selection_summary=selection_summary, subject_rows=subject_rows)
    artifacts = write_payload(payload, output_dir=args.output_dir)
    print(json.dumps({"artifacts": artifacts, "county_summary": payload["county_summary"]}, indent=2))


if __name__ == "__main__":
    main()
