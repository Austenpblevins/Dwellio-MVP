from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.services.unequal_roll_candidate_normalization import (  # noqa: E402
    condition_rank,
    quality_rank,
)
from app.services.unequal_roll_no_persist_replay import (  # noqa: E402
    UnequalRollNoPersistReplayService,
    UnequalRollReplayRequest,
)
from app.services.unequal_roll_smart_harvest import (  # noqa: E402
    CURRENT_ORDER_CAP_100,
    SIMILARITY_TOP_100,
    SameNeighborhoodHarvestSelection,
    cheap_same_neighborhood_similarity_score,
)
from app.services.unequal_roll_taxpayer_favorable_tiebreak import (  # noqa: E402
    TaxpayerFavorableTieBreakConfig,
    UnequalRollTaxpayerFavorableTieBreakService,
)
from infra.scripts.run_unequal_roll_broader_smart_harvest_validation import (  # noqa: E402
    connect_read_only,
    parse_neighborhood_override,
    select_validation_cohort,
    validate_selection_options,
)

EXPERIMENTAL_FULL_RERANKING = "experimental_full_reranking_v1"
EXPERIMENTAL_HARVEST_CAP = 100
MATERIAL_TAXPAYER_CHANGE_THRESHOLD = 1000.0
EXPERIMENT_TIEBREAK_CONFIG = TaxpayerFavorableTieBreakConfig(
    max_swaps=1,
    similarity_tolerance=0.02,
    median_movement_cap_ratio=0.02,
    max_avg_similarity_drop=0.01,
)

INPUT_CONTRACT = {
    "script_mode": "full_same_neighborhood_reranking_experiment",
    "full_candidate_reranking": True,
    "post_selection_swap_only": False,
    "production_scoring_penalty": False,
    "baseline_strategies": ["current_order_cap_100", "similarity_top_100"],
    "notes": [
        "This script is validation-only and no-persist.",
        "It reranks the full same-neighborhood candidate universe before final comp selection.",
        "It does not change production smart-harvest defaults, production scoring formulas, or final-value logic.",
        "All reranking penalties and preferences are experimental and applied only inside this runner.",
    ],
}


@dataclass(frozen=True)
class ExperimentalRerankingConfig:
    value_per_sf_outlier_delta: float = 15.0
    value_per_sf_outlier_penalty: float = 8.0
    price_tier_ratio_threshold: float = 1.12
    price_tier_absolute_threshold: float = 30000.0
    price_tier_penalty: float = 7.0
    subdivision_mismatch_penalty: float = 5.0
    micro_location_proxy_extra_penalty: float = 2.0
    land_mismatch_ratio_threshold: float = 0.30
    land_mismatch_penalty: float = 4.0
    severe_land_mismatch_ratio_threshold: float = 0.60
    severe_land_mismatch_penalty: float = 7.0
    bedroom_mismatch_penalty_per_room: float = 2.5
    bedroom_mismatch_penalty_cap: float = 5.0
    adjustment_burden_soft_ratio: float = 0.10
    adjustment_burden_soft_penalty: float = 3.0
    adjustment_burden_hard_ratio: float = 0.15
    adjustment_burden_hard_penalty: float = 6.0
    lower_value_credible_bonus: float = 1.5
    lower_value_similarity_band: float = 0.03
    experiment_harvest_cap: int = EXPERIMENTAL_HARVEST_CAP

    def as_metadata(self) -> dict[str, Any]:
        return {
            "value_per_sf_outlier_delta": self.value_per_sf_outlier_delta,
            "value_per_sf_outlier_penalty": self.value_per_sf_outlier_penalty,
            "price_tier_ratio_threshold": self.price_tier_ratio_threshold,
            "price_tier_absolute_threshold": self.price_tier_absolute_threshold,
            "price_tier_penalty": self.price_tier_penalty,
            "subdivision_mismatch_penalty": self.subdivision_mismatch_penalty,
            "micro_location_proxy_extra_penalty": self.micro_location_proxy_extra_penalty,
            "land_mismatch_ratio_threshold": self.land_mismatch_ratio_threshold,
            "land_mismatch_penalty": self.land_mismatch_penalty,
            "severe_land_mismatch_ratio_threshold": self.severe_land_mismatch_ratio_threshold,
            "severe_land_mismatch_penalty": self.severe_land_mismatch_penalty,
            "bedroom_mismatch_penalty_per_room": self.bedroom_mismatch_penalty_per_room,
            "bedroom_mismatch_penalty_cap": self.bedroom_mismatch_penalty_cap,
            "adjustment_burden_soft_ratio": self.adjustment_burden_soft_ratio,
            "adjustment_burden_soft_penalty": self.adjustment_burden_soft_penalty,
            "adjustment_burden_hard_ratio": self.adjustment_burden_hard_ratio,
            "adjustment_burden_hard_penalty": self.adjustment_burden_hard_penalty,
            "lower_value_credible_bonus": self.lower_value_credible_bonus,
            "lower_value_similarity_band": self.lower_value_similarity_band,
            "experiment_harvest_cap": self.experiment_harvest_cap,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a bounded no-persist full same-neighborhood reranking experiment for unequal-roll "
            "smart harvest across Harris and Fort Bend cohorts."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    parser.add_argument("--output-dir", type=Path, default=Path("/private/tmp"))
    parser.add_argument("--selection-mode", choices=["default", "targeted"], default="targeted")
    parser.add_argument("--harris-neighborhoods", default=None)
    parser.add_argument("--fort-bend-neighborhoods", default=None)
    parser.add_argument("--max-subjects-per-county", type=int, default=24)
    parser.add_argument("--max-subjects-per-neighborhood", type=int, default=2)
    parser.add_argument("--max-total-subjects", type=int, default=24)
    parser.add_argument("--smoke-limit", type=int, default=None)
    parser.add_argument("--fort-bend-neighborhood-limit", type=int, default=5)
    parser.add_argument("--fort-bend-min-neighborhood-count", type=int, default=20)
    return parser


def build_guardrail_summary() -> dict[str, Any]:
    return {
        "db_writes_occurred": False,
        "migrations_added": False,
        "runtime_defaults_changed": False,
        "smart_harvest_became_default": False,
        "reranking_became_default": False,
        "tie_break_automation_enabled": False,
        "production_scoring_adjustment_final_value_logic_changed": False,
        "workflow": "no_persist_experiment_only",
    }


def build_experiment_limitations() -> list[str]:
    return [
        "This is a true pre-selection reranking experiment on the full same-neighborhood candidate universe.",
        "It still relies on the existing no-persist downstream pipeline after the experimental top-N harvest is chosen.",
        "All penalty weights and thresholds are heuristic experiment controls, not production scoring changes.",
        "Lower-value credible alternative preference is a small experimental ranking bonus, not an automation decision.",
    ]


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_mean(values: list[float]) -> float | None:
    return round(mean(values), 4) if values else None


def _median(values: list[float]) -> float | None:
    return round(median(values), 4) if values else None


def _value_per_sf(value: float | None, living_area_sf: float | None) -> float | None:
    if value in {None, 0.0} or living_area_sf in {None, 0.0}:
        return None
    return round(value / living_area_sf, 4)


def _preferred_land_size(row: dict[str, Any]) -> float | None:
    land_sf = _as_float(row.get("land_sf"))
    if land_sf is not None and land_sf > 0:
        return land_sf
    land_acres = _as_float(row.get("land_acres"))
    if land_acres is not None and land_acres > 0:
        return land_acres * 43560.0
    return None


def _pct_gap(subject_value: float | None, candidate_value: float | None) -> float | None:
    if subject_value in {None, 0.0} or candidate_value is None:
        return None
    return abs(candidate_value - subject_value) / subject_value


def _effective_baths(row: dict[str, Any]) -> float | None:
    full_baths = _as_float(row.get("full_baths"))
    half_baths = _as_float(row.get("half_baths"))
    if full_baths is None and half_baths is None:
        return None
    return round((full_baths or 0.0) + ((half_baths or 0.0) * 0.5), 2)


def _quality_gap_steps(county_id: str, subject_value: Any, candidate_value: Any) -> int | None:
    subject_rank = quality_rank(county_id=county_id, value=subject_value)
    candidate_rank = quality_rank(county_id=county_id, value=candidate_value)
    if subject_rank is None or candidate_rank is None:
        return None
    return abs(subject_rank - candidate_rank)


def _condition_gap_steps(county_id: str, subject_value: Any, candidate_value: Any) -> int | None:
    subject_rank = condition_rank(county_id=county_id, value=subject_value)
    candidate_rank = condition_rank(county_id=county_id, value=candidate_value)
    if subject_rank is None or candidate_rank is None:
        return None
    return abs(subject_rank - candidate_rank)


def build_neighborhood_stats(universe_rows: list[dict[str, Any]]) -> dict[str, Any]:
    appraised_values = [
        value for row in universe_rows if (value := _as_float(row.get("appraised_value"))) is not None
    ]
    appraised_value_per_sf = [
        value
        for row in universe_rows
        if (
            value := _value_per_sf(
                _as_float(row.get("appraised_value")),
                _as_float(row.get("living_area_sf")),
            )
        )
        is not None
    ]
    land_sizes = [
        value for row in universe_rows if (value := _preferred_land_size(row)) is not None
    ]
    return {
        "candidate_count": len(universe_rows),
        "median_appraised_value": _median(appraised_values),
        "median_appraised_value_per_sf": _median(appraised_value_per_sf),
        "median_land_size": _median(land_sizes),
    }


def validate_selection_override(
    *,
    subject_snapshot: dict[str, Any],
    request: UnequalRollReplayRequest,
    selection: SameNeighborhoodHarvestSelection,
    config: ExperimentalRerankingConfig,
) -> str | None:
    if selection.selected_count > config.experiment_harvest_cap:
        return "selection_override_exceeds_experiment_cap"
    if len(selection.selected_rows) > config.experiment_harvest_cap:
        return "selection_override_exceeds_experiment_cap"
    required_fields = ("parcel_id", "county_id", "tax_year", "account_number", "neighborhood_code")
    subject_county = str(subject_snapshot.get("county_id") or "")
    subject_neighborhood = str(subject_snapshot.get("neighborhood_code") or "")
    subject_account = str(request.account_number or "")
    for row in selection.selected_rows:
        missing = [field for field in required_fields if row.get(field) in {None, ""}]
        if missing:
            return "selection_override_missing_required_fields"
        if str(row.get("county_id") or "") != subject_county:
            return "selection_override_county_mismatch"
        if _as_int(row.get("tax_year")) != int(request.requested_tax_year):
            return "selection_override_tax_year_mismatch"
        if str(row.get("neighborhood_code") or "") != subject_neighborhood:
            return "selection_override_neighborhood_mismatch"
        if str(row.get("account_number") or "") == subject_account:
            return "selection_override_includes_subject_account"
    return None


def estimate_adjustment_burden_ratio(
    *,
    subject_snapshot: dict[str, Any],
    row: dict[str, Any],
) -> float | None:
    subject_raw_value = _as_float(subject_snapshot.get("appraised_value"))
    subject_living_area = _as_float(subject_snapshot.get("living_area_sf"))
    if subject_raw_value in {None, 0.0} or subject_living_area in {None, 0.0}:
        return None

    subject_raw_value_per_sf = _value_per_sf(subject_raw_value, subject_living_area) or 0.0
    total_absolute_adjustment = 0.0
    total_absolute_adjustment += abs(
        ((_as_float(subject_snapshot.get("living_area_sf")) or 0.0) - (_as_float(row.get("living_area_sf")) or 0.0))
        * subject_raw_value_per_sf
        * 0.55
    )
    total_absolute_adjustment += abs(
        ((_as_float(row.get("effective_age")) or 0.0) - (_as_float(subject_snapshot.get("effective_age")) or 0.0))
        * subject_raw_value
        * 0.0015
    )
    total_absolute_adjustment += abs(
        ((_as_float(subject_snapshot.get("full_baths")) or 0.0) - (_as_float(row.get("full_baths")) or 0.0))
        * subject_raw_value
        * 0.03
    )
    total_absolute_adjustment += abs(
        ((_as_float(subject_snapshot.get("half_baths")) or 0.0) - (_as_float(row.get("half_baths")) or 0.0))
        * subject_raw_value
        * 0.01625
    )
    total_absolute_adjustment += abs(
        ((_as_float(subject_snapshot.get("stories")) or 0.0) - (_as_float(row.get("stories")) or 0.0))
        * subject_raw_value
        * 0.01
    )
    if bool(subject_snapshot.get("pool_flag")) != bool(row.get("pool_flag")):
        total_absolute_adjustment += subject_raw_value * 0.02
    quality_gap = _quality_gap_steps(
        county_id=str(subject_snapshot.get("county_id") or ""),
        subject_value=subject_snapshot.get("quality_code"),
        candidate_value=row.get("quality_code"),
    )
    if quality_gap is not None:
        total_absolute_adjustment += abs(quality_gap * subject_raw_value * 0.02)
    condition_gap = _condition_gap_steps(
        county_id=str(subject_snapshot.get("county_id") or ""),
        subject_value=subject_snapshot.get("condition_code"),
        candidate_value=row.get("condition_code"),
    )
    if condition_gap is not None:
        total_absolute_adjustment += abs(condition_gap * subject_raw_value * 0.015)
    return round(total_absolute_adjustment / subject_raw_value, 4)


def compute_experimental_rerank_score(
    *,
    subject_snapshot: dict[str, Any],
    row: dict[str, Any],
    neighborhood_stats: dict[str, Any],
    config: ExperimentalRerankingConfig,
) -> dict[str, Any]:
    base_score = cheap_same_neighborhood_similarity_score(
        subject_snapshot=subject_snapshot,
        row=row,
    )
    candidate_psf = _value_per_sf(
        _as_float(row.get("appraised_value")),
        _as_float(row.get("living_area_sf")),
    )
    subject_psf = _value_per_sf(
        _as_float(subject_snapshot.get("appraised_value")),
        _as_float(subject_snapshot.get("living_area_sf")),
    )
    neighborhood_psf_median = _as_float(neighborhood_stats.get("median_appraised_value_per_sf"))
    candidate_appraised = _as_float(row.get("appraised_value"))
    subject_appraised = _as_float(subject_snapshot.get("appraised_value"))
    neighborhood_value_median = _as_float(neighborhood_stats.get("median_appraised_value"))

    penalty_points = 0.0
    bonus_points = 0.0
    trigger_labels: list[str] = []

    if (
        candidate_psf is not None
        and neighborhood_psf_median is not None
        and candidate_psf - neighborhood_psf_median >= config.value_per_sf_outlier_delta
    ):
        penalty_points += config.value_per_sf_outlier_penalty
        trigger_labels.append("value_per_sf_outlier_penalty")

    price_tier_trigger = False
    if candidate_appraised is not None and subject_appraised is not None:
        if (
            candidate_appraised >= subject_appraised * config.price_tier_ratio_threshold
            and (candidate_appraised - subject_appraised) >= config.price_tier_absolute_threshold
        ):
            penalty_points += config.price_tier_penalty
            price_tier_trigger = True
            trigger_labels.append("price_tier_drift_penalty")
    if (
        not price_tier_trigger
        and candidate_appraised is not None
        and neighborhood_value_median is not None
        and candidate_appraised >= neighborhood_value_median * config.price_tier_ratio_threshold
        and candidate_psf is not None
        and neighborhood_psf_median is not None
        and candidate_psf > neighborhood_psf_median
    ):
        penalty_points += config.price_tier_penalty
        trigger_labels.append("price_tier_drift_penalty")

    subdivision_name = str(subject_snapshot.get("subdivision_name") or "").strip()
    candidate_subdivision = str(row.get("subdivision_name") or "").strip()
    subdivision_mismatch = subdivision_name != "" and subdivision_name != candidate_subdivision
    if subdivision_mismatch:
        penalty_points += config.subdivision_mismatch_penalty
        trigger_labels.append("subdivision_mismatch_penalty")
        if candidate_psf is not None and neighborhood_psf_median is not None and candidate_psf > neighborhood_psf_median:
            penalty_points += config.micro_location_proxy_extra_penalty
            trigger_labels.append("micro_location_proxy_penalty")

    land_gap_ratio = _pct_gap(
        _preferred_land_size(subject_snapshot),
        _preferred_land_size(row),
    )
    if land_gap_ratio is not None:
        if land_gap_ratio >= config.severe_land_mismatch_ratio_threshold:
            penalty_points += config.severe_land_mismatch_penalty
            trigger_labels.append("severe_land_mismatch_penalty")
        elif land_gap_ratio >= config.land_mismatch_ratio_threshold:
            penalty_points += config.land_mismatch_penalty
            trigger_labels.append("land_mismatch_penalty")

    bedroom_diff = abs((_as_int(subject_snapshot.get("bedrooms")) or 0) - (_as_int(row.get("bedrooms")) or 0))
    if bedroom_diff > 0:
        bedroom_penalty = min(
            config.bedroom_mismatch_penalty_cap,
            bedroom_diff * config.bedroom_mismatch_penalty_per_room,
        )
        penalty_points += bedroom_penalty
        trigger_labels.append("bedroom_mismatch_penalty")

    adjustment_burden_ratio = estimate_adjustment_burden_ratio(
        subject_snapshot=subject_snapshot,
        row=row,
    )
    if adjustment_burden_ratio is not None:
        if adjustment_burden_ratio >= config.adjustment_burden_hard_ratio:
            penalty_points += config.adjustment_burden_hard_penalty
            trigger_labels.append("adjustment_burden_hard_penalty")
        elif adjustment_burden_ratio >= config.adjustment_burden_soft_ratio:
            penalty_points += config.adjustment_burden_soft_penalty
            trigger_labels.append("adjustment_burden_soft_penalty")

    if (
        candidate_appraised is not None
        and subject_appraised is not None
        and candidate_appraised < subject_appraised
        and bedroom_diff <= 1
        and (land_gap_ratio is None or land_gap_ratio < config.land_mismatch_ratio_threshold)
        and not subdivision_mismatch
    ):
        bonus_points += config.lower_value_credible_bonus
        trigger_labels.append("lower_value_credible_bonus")

    experimental_score = round(base_score - penalty_points + bonus_points, 4)
    return {
        "account_number": str(row.get("account_number") or ""),
        "candidate_parcel_id": str(row.get("parcel_id") or ""),
        "base_similarity_score": base_score,
        "experimental_score": experimental_score,
        "penalty_points": round(penalty_points, 4),
        "bonus_points": round(bonus_points, 4),
        "candidate_appraised_value": candidate_appraised,
        "candidate_appraised_value_per_sf": candidate_psf,
        "adjustment_burden_ratio": adjustment_burden_ratio,
        "land_gap_ratio": round(land_gap_ratio, 4) if land_gap_ratio is not None else None,
        "bedroom_diff": bedroom_diff,
        "subdivision_mismatch": subdivision_mismatch,
        "trigger_labels": trigger_labels,
    }


def select_full_reranking_harvest(
    *,
    subject_snapshot: dict[str, Any],
    universe_rows: list[dict[str, Any]],
    config: ExperimentalRerankingConfig,
) -> tuple[SameNeighborhoodHarvestSelection, dict[str, Any]]:
    neighborhood_stats = build_neighborhood_stats(universe_rows)
    scored_rows = [
        compute_experimental_rerank_score(
            subject_snapshot=subject_snapshot,
            row=row,
            neighborhood_stats=neighborhood_stats,
            config=config,
        )
        for row in universe_rows
    ]
    scored_rows.sort(
        key=lambda item: (-float(item["experimental_score"]), str(item["account_number"]))
    )
    selected_accounts = {
        item["account_number"] for item in scored_rows[: config.experiment_harvest_cap]
    }
    selected_rows = [
        row for row in universe_rows if str(row.get("account_number") or "") in selected_accounts
    ]
    selected_rows.sort(
        key=lambda row: (
            -next(
                (
                    float(item["experimental_score"])
                    for item in scored_rows
                    if item["account_number"] == str(row.get("account_number") or "")
                ),
                -1.0,
            ),
            str(row.get("account_number") or ""),
        )
    )
    selection = SameNeighborhoodHarvestSelection(
        strategy=EXPERIMENTAL_FULL_RERANKING,
        universe_count=len(universe_rows),
        selected_count=len(selected_rows),
        cap_used=config.experiment_harvest_cap,
        excluded_by_cap=max(0, len(universe_rows) - len(selected_rows)),
        scored_universe=scored_rows,
        selected_rows=selected_rows,
    )
    selection_meta = {
        "neighborhood_stats": neighborhood_stats,
        "scored_universe_top20": scored_rows[:20],
        "selected_account_numbers": [str(row.get("account_number") or "") for row in selected_rows],
        "selected_signal_counts": Counter(
            label
            for item in scored_rows[: config.experiment_harvest_cap]
            for label in item["trigger_labels"]
        ),
    }
    return selection, selection_meta


def replay(
    service: UnequalRollNoPersistReplayService,
    conn: Any,
    *,
    county: str,
    account: str,
    requested_tax_year: int,
    strategy: str,
    selection_override: SameNeighborhoodHarvestSelection | None = None,
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
                same_neighborhood_selection_override=selection_override,
                include_taxpayer_favorable_tiebreak_reporting=False,
            )
        conn.rollback()
        return result
    except Exception:
        conn.rollback()
        raise


def prepare_subject_snapshot_and_universe(
    service: UnequalRollNoPersistReplayService,
    conn: Any,
    *,
    request: UnequalRollReplayRequest,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]] | None, dict[str, Any] | None]:
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '120s'")
            cur.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            subject_row = service._subject_snapshot_service._fetch_subject_row(
                cur,
                county_id=request.county_id,
                requested_tax_year=request.requested_tax_year,
                account_number=request.account_number,
            )
            if subject_row is None:
                conn.rollback()
                return None, None, {"replay_status": "blocked", "blocker_code": "subject_not_found"}
            subject_snapshot = service._build_subject_snapshot(
                cur,
                request=request,
                subject_row=subject_row,
            )
            if subject_snapshot.get("support_status") == "unsupported":
                conn.rollback()
                return None, None, {
                    "replay_status": "blocked",
                    "blocker_code": str(subject_snapshot.get("support_blocker_code") or "subject_not_ready"),
                }
            universe_rows = service._discovery_service._fetch_same_neighborhood_candidates(
                cur,
                subject_snapshot=subject_snapshot,
                limit=None,
            )
        conn.rollback()
        return subject_snapshot, universe_rows, None
    except Exception:
        conn.rollback()
        raise


def included_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    return list((result.get("final_value_detail_json") or {}).get("included_comp_rows") or [])


def exposed_requested_roll_value(result: dict[str, Any]) -> float | None:
    safe_value = _as_float(result.get("safe_requested_roll_value"))
    if safe_value is not None:
        return safe_value
    return _as_float(result.get("requested_roll_value"))


def exposed_requested_reduction_amount(result: dict[str, Any]) -> float | None:
    safe_value = _as_float(result.get("safe_requested_reduction_amount"))
    if safe_value is not None:
        return safe_value
    return _as_float(result.get("requested_reduction_amount"))


def exposed_requested_reduction_pct(result: dict[str, Any]) -> float | None:
    safe_value = _as_float(result.get("safe_requested_reduction_pct"))
    if safe_value is not None:
        return safe_value
    return _as_float(result.get("requested_reduction_pct"))


def avg_similarity(rows: list[dict[str, Any]]) -> float | None:
    values = [
        score for row in rows if (score := _as_float(row.get("similarity_score"))) is not None
    ]
    return round(mean(values), 4) if values else None


def total_adjustment_burden(rows: list[dict[str, Any]]) -> float:
    total = 0.0
    for row in rows:
        for line_item in list(row.get("line_items") or []):
            amount = _as_float(line_item.get("signed_adjustment_amount"))
            if amount is not None:
                total += abs(amount)
    return round(total, 2)


def final_comp_ids(result: dict[str, Any]) -> list[str]:
    return [
        str(row.get("candidate_parcel_id") or "")
        for row in included_rows(result)
        if str(row.get("candidate_parcel_id") or "")
    ]


def summarize_replay_result(
    result: dict[str, Any],
    *,
    neighborhood_stats: dict[str, Any] | None = None,
    selection_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = included_rows(result)
    neighborhood_psf_median = (
        _as_float(neighborhood_stats.get("median_appraised_value_per_sf"))
        if neighborhood_stats
        else None
    )
    outlier_count = 0
    for row in rows:
        raw_psf = _as_float(row.get("raw_appraised_value_per_sf"))
        if (
            raw_psf is not None
            and neighborhood_psf_median is not None
            and raw_psf - neighborhood_psf_median >= 15.0
        ):
            outlier_count += 1
    return {
        "replay_status": result.get("replay_status"),
        "support_status": result.get("support_status"),
        "final_value_status": result.get("final_value_status"),
        "requested_roll_value": exposed_requested_roll_value(result),
        "requested_reduction_amount": exposed_requested_reduction_amount(result),
        "requested_reduction_pct": exposed_requested_reduction_pct(result),
        "value_interpretation": result.get("value_interpretation"),
        "included_comp_count": _as_int(result.get("included_comp_count")) or 0,
        "excluded_review_heavy_count": _as_int(result.get("excluded_review_heavy_count")) or 0,
        "excluded_likely_exclude_count": _as_int(result.get("excluded_likely_exclude_count")) or 0,
        "average_similarity": avg_similarity(rows),
        "included_comp_ids": final_comp_ids(result),
        "total_adjustment_burden": total_adjustment_burden(rows),
        "value_per_sf_outlier_count": outlier_count,
        "selection_signal_counts": dict(selection_meta.get("selected_signal_counts") or {})
        if selection_meta
        else {},
    }


def build_comp_delta(before_ids: list[str], after_ids: list[str]) -> dict[str, Any]:
    before_set = set(before_ids)
    after_set = set(after_ids)
    return {
        "overlap_count": len(before_set & after_set),
        "removed_comp_ids": sorted(before_set - after_set),
        "added_comp_ids": sorted(after_set - before_set),
    }


def _final_status_rank(status: str | None) -> int:
    normalized = str(status or "")
    ranking = {
        "supported": 3,
        "supported_with_review": 2,
        "manual_review_required": 1,
        "unsupported": 0,
    }
    return ranking.get(normalized, -1)


def evaluate_lower_value_signal(
    tiebreak_service: UnequalRollTaxpayerFavorableTieBreakService,
    *,
    current_result: dict[str, Any],
    reranked_result: dict[str, Any],
) -> dict[str, Any]:
    simulation = tiebreak_service.simulate(
        current_result=current_result,
        smart_result=reranked_result,
        config=EXPERIMENT_TIEBREAK_CONFIG,
    )
    accepted_swaps = list(simulation.get("accepted_swaps") or [])
    automation = dict(simulation.get("automation_assessment") or {})
    return {
        "available": bool(accepted_swaps),
        "accepted_swap_count": len(accepted_swaps),
        "automation_status": automation.get("automation_status", "no_safe_opportunity"),
        "estimated_reduction_impact": round(
            (_as_float(simulation.get("requested_reduction_amount")) or 0.0)
            - (exposed_requested_reduction_amount(reranked_result) or 0.0),
            2,
        ),
    }


def explain_subject_outcome(
    *,
    smart_summary: dict[str, Any],
    rerank_summary: dict[str, Any],
    rerank_meta: dict[str, Any] | None,
) -> str:
    taxpayer_delta = round(
        (rerank_summary.get("requested_reduction_amount") or 0.0)
        - (smart_summary.get("requested_reduction_amount") or 0.0),
        2,
    )
    signal_counts = dict(rerank_meta.get("selected_signal_counts") or {}) if rerank_meta else {}
    if taxpayer_delta > 0 and signal_counts.get("value_per_sf_outlier_penalty", 0) < 3:
        return "reduced_value_per_sf_outlier_exposure"
    if taxpayer_delta > 0 and signal_counts.get("land_mismatch_penalty", 0) < 3:
        return "reduced_land_mismatch_exposure"
    if taxpayer_delta > 0 and signal_counts.get("bedroom_mismatch_penalty", 0) < 3:
        return "reduced_bedroom_mismatch_exposure"
    if taxpayer_delta < 0 and signal_counts.get("subdivision_mismatch_penalty", 0) > 0:
        return "micro_location_penalty_may_be_noise"
    if taxpayer_delta < 0 and signal_counts.get("price_tier_drift_penalty", 0) > 0:
        return "price_tier_penalty_may_be_too_strong"
    return "mixed_signal_needs_manual_review"


def build_subject_comparison_row(
    *,
    subject: Any,
    current_result: dict[str, Any],
    smart_result: dict[str, Any],
    reranked_result: dict[str, Any] | None,
    rerank_meta: dict[str, Any] | None,
    lower_value_signal: dict[str, Any] | None,
) -> dict[str, Any]:
    comparison_ready = (
        current_result.get("replay_status") == "completed"
        and smart_result.get("replay_status") == "completed"
        and (reranked_result or {}).get("replay_status") == "completed"
    )
    if not comparison_ready:
        return {
            "county_id": subject.county_id,
            "subject_account": subject.account_number,
            "neighborhood_code": subject.neighborhood_code,
            "comparison_ready": False,
            "blocker_code": (
                (reranked_result or {}).get("blocker_code")
                or smart_result.get("blocker_code")
                or current_result.get("blocker_code")
            ),
        }

    neighborhood_stats = dict(rerank_meta.get("neighborhood_stats") or {}) if rerank_meta else {}
    current_summary = summarize_replay_result(current_result, neighborhood_stats=neighborhood_stats)
    smart_summary = summarize_replay_result(smart_result, neighborhood_stats=neighborhood_stats)
    rerank_summary = summarize_replay_result(
        reranked_result or {},
        neighborhood_stats=neighborhood_stats,
        selection_meta=rerank_meta,
    )
    current_vs_rerank = build_comp_delta(
        current_summary["included_comp_ids"],
        rerank_summary["included_comp_ids"],
    )
    smart_vs_rerank = build_comp_delta(
        smart_summary["included_comp_ids"],
        rerank_summary["included_comp_ids"],
    )

    return {
        "county_id": subject.county_id,
        "subject_account": subject.account_number,
        "neighborhood_code": subject.neighborhood_code,
        "selection_source": subject.selection_source,
        "comparison_ready": True,
        "current_requested_reduction_amount": current_summary["requested_reduction_amount"],
        "smart_requested_reduction_amount": smart_summary["requested_reduction_amount"],
        "rerank_requested_reduction_amount": rerank_summary["requested_reduction_amount"],
        "current_requested_roll_value": current_summary["requested_roll_value"],
        "smart_requested_roll_value": smart_summary["requested_roll_value"],
        "rerank_requested_roll_value": rerank_summary["requested_roll_value"],
        "current_final_value_status": current_summary["final_value_status"],
        "smart_final_value_status": smart_summary["final_value_status"],
        "rerank_final_value_status": rerank_summary["final_value_status"],
        "current_value_interpretation": current_summary["value_interpretation"],
        "smart_value_interpretation": smart_summary["value_interpretation"],
        "rerank_value_interpretation": rerank_summary["value_interpretation"],
        "current_included_comp_count": current_summary["included_comp_count"],
        "smart_included_comp_count": smart_summary["included_comp_count"],
        "rerank_included_comp_count": rerank_summary["included_comp_count"],
        "current_average_similarity": current_summary["average_similarity"],
        "smart_average_similarity": smart_summary["average_similarity"],
        "rerank_average_similarity": rerank_summary["average_similarity"],
        "smart_vs_current_taxpayer_delta": round(
            (smart_summary["requested_reduction_amount"] or 0.0)
            - (current_summary["requested_reduction_amount"] or 0.0),
            2,
        ),
        "rerank_vs_current_taxpayer_delta": round(
            (rerank_summary["requested_reduction_amount"] or 0.0)
            - (current_summary["requested_reduction_amount"] or 0.0),
            2,
        ),
        "rerank_vs_smart_taxpayer_delta": round(
            (rerank_summary["requested_reduction_amount"] or 0.0)
            - (smart_summary["requested_reduction_amount"] or 0.0),
            2,
        ),
        "smart_vs_current_similarity_delta": round(
            (smart_summary["average_similarity"] or 0.0)
            - (current_summary["average_similarity"] or 0.0),
            4,
        ),
        "rerank_vs_smart_similarity_delta": round(
            (rerank_summary["average_similarity"] or 0.0)
            - (smart_summary["average_similarity"] or 0.0),
            4,
        ),
        "current_vs_rerank_overlap_count": current_vs_rerank["overlap_count"],
        "smart_vs_rerank_overlap_count": smart_vs_rerank["overlap_count"],
        "current_vs_rerank_removed_comp_ids": current_vs_rerank["removed_comp_ids"],
        "current_vs_rerank_added_comp_ids": current_vs_rerank["added_comp_ids"],
        "smart_vs_rerank_removed_comp_ids": smart_vs_rerank["removed_comp_ids"],
        "smart_vs_rerank_added_comp_ids": smart_vs_rerank["added_comp_ids"],
        "rerank_review_heavy_delta_vs_smart": (
            rerank_summary["excluded_review_heavy_count"]
            - smart_summary["excluded_review_heavy_count"]
        ),
        "rerank_likely_exclude_delta_vs_smart": (
            rerank_summary["excluded_likely_exclude_count"]
            - smart_summary["excluded_likely_exclude_count"]
        ),
        "rerank_support_status_drift_vs_smart": (
            rerank_summary["support_status"] != smart_summary["support_status"]
        ),
        "rerank_final_status_drift_vs_smart": (
            rerank_summary["final_value_status"] != smart_summary["final_value_status"]
        ),
        "support_status_transition_smart_to_rerank": (
            f"{smart_summary['support_status']} -> {rerank_summary['support_status']}"
        ),
        "final_status_transition_smart_to_rerank": (
            f"{smart_summary['final_value_status']} -> {rerank_summary['final_value_status']}"
        ),
        "rerank_total_adjustment_burden": rerank_summary["total_adjustment_burden"],
        "rerank_value_per_sf_outlier_count": rerank_summary["value_per_sf_outlier_count"],
        "rerank_selection_signal_counts": rerank_summary["selection_signal_counts"],
        "rerank_selected_top_100_count": len(
            list((rerank_meta or {}).get("selected_account_numbers") or [])
        ),
        "rerank_final_included_comp_count": rerank_summary["included_comp_count"],
        "lower_value_credible_alternative_signal": lower_value_signal or {},
        "primary_explanation": explain_subject_outcome(
            smart_summary=smart_summary,
            rerank_summary=rerank_summary,
            rerank_meta=rerank_meta,
        ),
    }


def build_group_summary(subject_rows: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    ready = [row for row in subject_rows if row.get("comparison_ready")]
    taxpayer_deltas = [float(row["rerank_vs_smart_taxpayer_delta"]) for row in ready]
    smart_current_deltas = [float(row["smart_vs_current_taxpayer_delta"]) for row in ready]
    rerank_current_deltas = [float(row["rerank_vs_current_taxpayer_delta"]) for row in ready]
    positive_recovery = [delta for delta in taxpayer_deltas if delta > 0]
    material_losses = [
        row for row in ready if (row.get("rerank_vs_smart_taxpayer_delta") or 0.0) <= -MATERIAL_TAXPAYER_CHANGE_THRESHOLD
    ]
    material_gains = [
        row for row in ready if (row.get("rerank_vs_smart_taxpayer_delta") or 0.0) >= MATERIAL_TAXPAYER_CHANGE_THRESHOLD
    ]
    final_transitions = Counter(
        str(row.get("final_status_transition_smart_to_rerank"))
        for row in ready
        if row.get("rerank_final_status_drift_vs_smart")
    )
    true_downgrades = sum(
        1
        for row in ready
        if _final_status_rank(str(row.get("rerank_final_value_status") or ""))
        < _final_status_rank(str(row.get("smart_final_value_status") or ""))
    )
    true_upgrades = sum(
        1
        for row in ready
        if _final_status_rank(str(row.get("rerank_final_value_status") or ""))
        > _final_status_rank(str(row.get("smart_final_value_status") or ""))
    )
    manual_or_unsupported_results = sum(
        1
        for row in ready
        if str(row.get("rerank_final_value_status") or "")
        in {"manual_review_required", "unsupported"}
    )
    return {
        "label": label,
        "subject_count": len(subject_rows),
        "comparison_ready_count": len(ready),
        "blocked_count": len(subject_rows) - len(ready),
        "net_taxpayer_delta_rerank_vs_smart": round(sum(taxpayer_deltas), 2),
        "net_taxpayer_delta_smart_vs_current": round(sum(smart_current_deltas), 2),
        "net_taxpayer_delta_rerank_vs_current": round(sum(rerank_current_deltas), 2),
        "total_recovery_amount": round(sum(positive_recovery), 2),
        "material_gain_count": len(material_gains),
        "material_loss_count": len(material_losses),
        "median_taxpayer_delta_rerank_vs_smart": _median(taxpayer_deltas),
        "average_similarity_delta_rerank_vs_smart": _safe_mean(
            [float(row.get("rerank_vs_smart_similarity_delta") or 0.0) for row in ready]
        ),
        "average_review_heavy_delta_rerank_vs_smart": _safe_mean(
            [float(row.get("rerank_review_heavy_delta_vs_smart") or 0.0) for row in ready]
        ),
        "average_likely_exclude_delta_rerank_vs_smart": _safe_mean(
            [float(row.get("rerank_likely_exclude_delta_vs_smart") or 0.0) for row in ready]
        ),
        "support_status_drift_count": sum(
            1 for row in ready if row.get("rerank_support_status_drift_vs_smart")
        ),
        "final_status_drift_count": sum(
            1 for row in ready if row.get("rerank_final_status_drift_vs_smart")
        ),
        "final_status_transition_counts": dict(final_transitions),
        "final_status_true_downgrade_count": true_downgrades,
        "final_status_true_upgrade_count": true_upgrades,
        "final_status_manual_or_unsupported_result_count": manual_or_unsupported_results,
        "safe_lower_value_signal_count": sum(
            1
            for row in ready
            if ((row.get("lower_value_credible_alternative_signal") or {}).get("automation_status"))
            == "safe_automated_candidate"
        ),
        "manual_lower_value_signal_count": sum(
            1
            for row in ready
            if ((row.get("lower_value_credible_alternative_signal") or {}).get("automation_status"))
            == "manual_review_only"
        ),
    }


def build_value_interpretation_transition_counts(
    subject_rows: list[dict[str, Any]],
    *,
    source_key: str,
    target_key: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in subject_rows:
        if not row.get("comparison_ready"):
            continue
        source = str(row.get(source_key) or "unknown")
        target = str(row.get(target_key) or "unknown")
        counts[f"{source} -> {target}"] += 1
    return dict(counts)


def build_model_backed_only_summary(subject_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = build_model_backed_only_rows(subject_rows)
    return build_group_summary(rows, label="model_backed_only")


def build_non_model_backed_summary(subject_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [
        row
        for row in subject_rows
        if row.get("comparison_ready")
        and (
            row.get("current_value_interpretation") != "final_model_value"
            or row.get("smart_value_interpretation") != "final_model_value"
            or row.get("rerank_value_interpretation") != "final_model_value"
        )
    ]
    return build_group_summary(rows, label="diagnostic_provisional_or_unsupported")


def build_model_backed_only_rows(subject_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in subject_rows
        if row.get("comparison_ready")
        and row.get("current_value_interpretation") == "final_model_value"
        and row.get("smart_value_interpretation") == "final_model_value"
        and row.get("rerank_value_interpretation") == "final_model_value"
    ]


def build_top_case_rows(
    subject_rows: list[dict[str, Any]],
    *,
    top_n: int,
    reverse: bool,
) -> list[dict[str, Any]]:
    ready = [row for row in subject_rows if row.get("comparison_ready")]
    ordered = sorted(
        ready,
        key=lambda row: float(row.get("rerank_vs_smart_taxpayer_delta") or 0.0),
        reverse=reverse,
    )[:top_n]
    return [
        {
            "account": row.get("subject_account"),
            "county": row.get("county_id"),
            "neighborhood": row.get("neighborhood_code"),
            "taxpayer_delta": row.get("rerank_vs_smart_taxpayer_delta"),
            "value_interpretation_transition": (
                f"{row.get('smart_value_interpretation')} -> {row.get('rerank_value_interpretation')}"
            ),
            "final_status_transition": row.get("final_status_transition_smart_to_rerank"),
            "similarity_delta": row.get("rerank_vs_smart_similarity_delta"),
            "primary_explanation": row.get("primary_explanation"),
        }
        for row in ordered
    ]


def build_outlier_sensitivity_summary(subject_rows: list[dict[str, Any]]) -> dict[str, Any]:
    ready = [row for row in subject_rows if row.get("comparison_ready")]
    deltas = sorted(
        [float(row.get("rerank_vs_smart_taxpayer_delta") or 0.0) for row in ready],
        reverse=True,
    )
    total = round(sum(deltas), 2)
    return {
        "net_taxpayer_delta_rerank_vs_smart": total,
        "net_excluding_top_1_gain": round(total - sum(deltas[:1]), 2),
        "net_excluding_top_3_gains": round(total - sum(deltas[:3]), 2),
        "net_excluding_top_5_gains": round(total - sum(deltas[:5]), 2),
    }


def recommend_outcome(payload: dict[str, Any]) -> str:
    overall = dict(payload.get("overall_summary") or {})
    if overall.get("comparison_ready_count", 0) == 0:
        return "abandon_experiment_runner_until_blockers_resolved"
    if any(
        key == "manual_review_required -> unsupported"
        for key in dict(overall.get("final_status_transition_counts") or {}).keys()
    ):
        return "refine_before_more_validation"
    if overall.get("comparison_ready_count", 0) == 0:
        return "abandon_experiment_runner_until_blockers_resolved"
    if overall.get("net_taxpayer_delta_rerank_vs_smart", 0.0) <= 0:
        return "keep_analysis_only_or_abandon"
    if overall.get("final_status_drift_count", 0) > max(3, overall.get("comparison_ready_count", 0) * 0.15):
        return "refine_before_more_validation"
    if overall.get("material_loss_count", 0) >= overall.get("material_gain_count", 0):
        return "refine_before_more_validation"
    return "continue_bounded_validation_only"


def build_payload(
    *,
    selection_summary: dict[str, Any],
    subject_rows: list[dict[str, Any]],
    experiment_config: ExperimentalRerankingConfig,
) -> dict[str, Any]:
    county_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    neighborhood_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in subject_rows:
        county_groups[str(row.get("county_id"))].append(row)
        neighborhood_groups[str(row.get("neighborhood_code"))].append(row)
    payload = {
        "input_contract": INPUT_CONTRACT,
        "guardrails": build_guardrail_summary(),
        "experiment_limitations": build_experiment_limitations(),
        "experiment_weights_and_thresholds": experiment_config.as_metadata(),
        "selection_summary": selection_summary,
        "comparison_ready_count": sum(1 for row in subject_rows if row.get("comparison_ready")),
        "blocked_or_unsupported_count": sum(1 for row in subject_rows if not row.get("comparison_ready")),
        "overall_summary": build_group_summary(subject_rows, label="overall"),
        "model_backed_only_summary": build_model_backed_only_summary(subject_rows),
        "diagnostic_provisional_or_unsupported_summary": build_non_model_backed_summary(
            subject_rows
        ),
        "value_interpretation_transitions": {
            "current_to_smart": build_value_interpretation_transition_counts(
                subject_rows,
                source_key="current_value_interpretation",
                target_key="smart_value_interpretation",
            ),
            "smart_to_rerank": build_value_interpretation_transition_counts(
                subject_rows,
                source_key="smart_value_interpretation",
                target_key="rerank_value_interpretation",
            ),
        },
        "top_gains": build_top_case_rows(subject_rows, top_n=10, reverse=True),
        "top_losses": build_top_case_rows(subject_rows, top_n=10, reverse=False),
        "model_backed_top_gains": build_top_case_rows(
            build_model_backed_only_rows(subject_rows),
            top_n=10,
            reverse=True,
        ),
        "model_backed_top_losses": build_top_case_rows(
            build_model_backed_only_rows(subject_rows),
            top_n=10,
            reverse=False,
        ),
        "outlier_sensitivity_summary": build_outlier_sensitivity_summary(subject_rows),
        "county_summaries": {
            county: build_group_summary(rows, label=county)
            for county, rows in county_groups.items()
        },
        "neighborhood_summaries": {
            neighborhood: build_group_summary(rows, label=neighborhood)
            for neighborhood, rows in neighborhood_groups.items()
        },
        "subject_rows": subject_rows,
    }
    payload["segment_table"] = [
        {
            "segment": neighborhood,
            "smart_vs_current": summary["net_taxpayer_delta_smart_vs_current"],
            "rerank_vs_smart": summary["net_taxpayer_delta_rerank_vs_smart"],
            "rerank_vs_current": summary["net_taxpayer_delta_rerank_vs_current"],
            "final_status_drift": summary["final_status_drift_count"],
            "material_gains": summary["material_gain_count"],
            "material_losses": summary["material_loss_count"],
        }
        for neighborhood, summary in payload["neighborhood_summaries"].items()
    ]
    payload["attrition_summary"] = {
        "rerank_selected_top_100_count": sum(
            int(row.get("rerank_selected_top_100_count") or 0)
            for row in subject_rows
            if row.get("comparison_ready")
        ),
        "rerank_final_included_comp_count": sum(
            int(row.get("rerank_final_included_comp_count") or 0)
            for row in subject_rows
            if row.get("comparison_ready")
        ),
    }
    if (
        payload["diagnostic_provisional_or_unsupported_summary"]["net_taxpayer_delta_rerank_vs_smart"]
        > 0
    ):
        payload["recommendation_note"] = (
            "Large gains include diagnostic/provisional/unsupported outcome paths; treat headline net benefit conservatively."
        )
    payload["recommendation"] = recommend_outcome(payload)
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def write_csv(path: Path, subject_rows: list[dict[str, Any]]) -> None:
    flat_rows: list[dict[str, Any]] = []
    for row in subject_rows:
        flat = dict(row)
        for key in (
            "current_vs_rerank_removed_comp_ids",
            "current_vs_rerank_added_comp_ids",
            "smart_vs_rerank_removed_comp_ids",
            "smart_vs_rerank_added_comp_ids",
            "rerank_selection_signal_counts",
            "lower_value_credible_alternative_signal",
        ):
            if key in flat:
                flat[key] = json.dumps(flat[key], sort_keys=True)
        flat_rows.append(flat)
    fieldnames = sorted({key for row in flat_rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in flat_rows:
            writer.writerow(row)


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Unequal Roll Full Reranking Experiment",
        "",
        "## Method Boundary",
        "",
        "- Validation/reporting only",
        "- No DB writes",
        "- No migrations",
        "- No runtime default changes",
        "- Smart harvest remains non-default",
        "- Experimental reranking remains non-default",
        "- No production scoring changes",
        "- No production adjustment/final-value changes",
        "- No-persist only",
        "- True full same-neighborhood reranking before final comp selection",
        "",
        "## Cohort",
        "",
        f"- Selection mode: {payload['selection_summary'].get('selection_mode')}",
        f"- Selected subjects: {payload['selection_summary'].get('selected_subject_count')}",
        f"- Comparison-ready: {payload.get('comparison_ready_count')}",
        f"- Blocked/unsupported: {payload.get('blocked_or_unsupported_count')}",
        "",
        "## Recommendation",
        "",
        f"- {payload.get('recommendation')}",
        "",
        "## Overall Summary",
        "",
        json.dumps(payload.get("overall_summary"), indent=2, sort_keys=True),
        "",
        "## Model-Backed-Only Summary",
        "",
        json.dumps(payload.get("model_backed_only_summary"), indent=2, sort_keys=True),
        "",
        "## Diagnostic / Provisional / Unsupported Summary",
        "",
        json.dumps(
            payload.get("diagnostic_provisional_or_unsupported_summary"),
            indent=2,
            sort_keys=True,
        ),
        "",
        "## Value Interpretation Transitions",
        "",
        json.dumps(payload.get("value_interpretation_transitions"), indent=2, sort_keys=True),
        "",
        "## Outlier Sensitivity",
        "",
        json.dumps(payload.get("outlier_sensitivity_summary"), indent=2, sort_keys=True),
        "",
        "## Top Gains",
        "",
        json.dumps(payload.get("top_gains"), indent=2, sort_keys=True),
        "",
        "## Top Losses",
        "",
        json.dumps(payload.get("top_losses"), indent=2, sort_keys=True),
        "",
        "## Model-Backed Top Gains",
        "",
        json.dumps(payload.get("model_backed_top_gains"), indent=2, sort_keys=True),
        "",
        "## Model-Backed Top Losses",
        "",
        json.dumps(payload.get("model_backed_top_losses"), indent=2, sort_keys=True),
        "",
        "## Attrition Summary",
        "",
        json.dumps(payload.get("attrition_summary"), indent=2, sort_keys=True),
        "",
        "## County Summaries",
        "",
        json.dumps(payload.get("county_summaries"), indent=2, sort_keys=True),
        "",
        "## Neighborhood Summaries",
        "",
        json.dumps(payload.get("neighborhood_summaries"), indent=2, sort_keys=True),
        "",
    ]
    path.write_text("\n".join(lines))


def run_subject_experiment(
    *,
    service: UnequalRollNoPersistReplayService,
    tiebreak_service: UnequalRollTaxpayerFavorableTieBreakService,
    conn: Any,
    subject: Any,
    requested_tax_year: int,
    config: ExperimentalRerankingConfig,
) -> dict[str, Any]:
    current_result = replay(
        service,
        conn,
        county=subject.county_id,
        account=subject.account_number,
        requested_tax_year=requested_tax_year,
        strategy=CURRENT_ORDER_CAP_100,
    )
    smart_result = replay(
        service,
        conn,
        county=subject.county_id,
        account=subject.account_number,
        requested_tax_year=requested_tax_year,
        strategy=SIMILARITY_TOP_100,
    )
    request = UnequalRollReplayRequest(
        county_id=subject.county_id,
        account_number=subject.account_number,
        requested_tax_year=requested_tax_year,
    )
    subject_snapshot, universe_rows, blocked = prepare_subject_snapshot_and_universe(
        service,
        conn,
        request=request,
    )
    reranked_result: dict[str, Any] | None = None
    rerank_meta: dict[str, Any] | None = None
    lower_value_signal: dict[str, Any] | None = None
    if blocked is None and subject_snapshot is not None and universe_rows:
        selection, rerank_meta = select_full_reranking_harvest(
            subject_snapshot=subject_snapshot,
            universe_rows=universe_rows,
            config=config,
        )
        override_blocker = validate_selection_override(
            subject_snapshot=subject_snapshot,
            request=request,
            selection=selection,
            config=config,
        )
        if override_blocker is not None:
            reranked_result = {"replay_status": "blocked", "blocker_code": override_blocker}
            return build_subject_comparison_row(
                subject=subject,
                current_result=current_result,
                smart_result=smart_result,
                reranked_result=reranked_result,
                rerank_meta=rerank_meta,
                lower_value_signal=None,
            )
        reranked_result = replay(
            service,
            conn,
            county=subject.county_id,
            account=subject.account_number,
            requested_tax_year=requested_tax_year,
            strategy=EXPERIMENTAL_FULL_RERANKING,
            selection_override=selection,
        )
        if reranked_result.get("replay_status") == "completed" and current_result.get("replay_status") == "completed":
            lower_value_signal = evaluate_lower_value_signal(
                tiebreak_service,
                current_result=current_result,
                reranked_result=reranked_result,
            )
    elif blocked is not None:
        reranked_result = blocked

    return build_subject_comparison_row(
        subject=subject,
        current_result=current_result,
        smart_result=smart_result,
        reranked_result=reranked_result,
        rerank_meta=rerank_meta,
        lower_value_signal=lower_value_signal,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    harris_override = parse_neighborhood_override(args.harris_neighborhoods)
    fort_bend_override = parse_neighborhood_override(args.fort_bend_neighborhoods)
    try:
        validate_selection_options(
            selection_mode=args.selection_mode,
            harris_neighborhood_override=harris_override,
            fort_bend_neighborhood_override=fort_bend_override,
        )
    except ValueError as exc:
        parser.error(str(exc))

    service = UnequalRollNoPersistReplayService()
    tiebreak_service = UnequalRollTaxpayerFavorableTieBreakService()
    config = ExperimentalRerankingConfig()

    with connect_read_only(args.database_url) as conn:
        with conn.cursor() as cur:
            subjects, selection_summary = select_validation_cohort(
                cur,
                selection_mode=args.selection_mode,
                harris_neighborhood_override=harris_override,
                fort_bend_neighborhood_override=fort_bend_override,
                requested_tax_year=args.requested_tax_year,
                max_subjects_per_county=args.max_subjects_per_county,
                max_subjects_per_neighborhood=args.max_subjects_per_neighborhood,
                max_total_subjects=args.max_total_subjects,
                smoke_limit=args.smoke_limit,
                fort_bend_neighborhood_limit=args.fort_bend_neighborhood_limit,
                fort_bend_min_neighborhood_count=args.fort_bend_min_neighborhood_count,
            )
        subject_rows = [
            run_subject_experiment(
                service=service,
                tiebreak_service=tiebreak_service,
                conn=conn,
                subject=subject,
                requested_tax_year=args.requested_tax_year,
                config=config,
            )
            for subject in subjects
        ]

    payload = build_payload(
        selection_summary=selection_summary,
        subject_rows=subject_rows,
        experiment_config=config,
    )
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    base = args.output_dir / f"unequal_roll_full_reranking_experiment_{timestamp}"
    write_json(base.with_suffix(".json"), payload)
    write_csv(base.with_suffix(".csv"), subject_rows)
    write_md(base.with_suffix(".md"), payload)
    print(base.with_suffix(".json"))
    print(base.with_suffix(".csv"))
    print(base.with_suffix(".md"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
