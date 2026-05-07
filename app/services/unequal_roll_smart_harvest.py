from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.services.unequal_roll_candidate_normalization import property_class_relation

CURRENT_ORDER_CAP_100 = "current_order_cap_100"
DYNAMIC_CAP_150 = "dynamic_cap_150"
SIMILARITY_TOP_100 = "similarity_top_100"
SIMILARITY_TOP_125 = "similarity_top_125"
SIMILARITY_TOP_150 = "similarity_top_150"

DEFAULT_HARVEST_STRATEGY = CURRENT_ORDER_CAP_100
SUPPORTED_HARVEST_STRATEGIES = {
    CURRENT_ORDER_CAP_100,
    DYNAMIC_CAP_150,
    SIMILARITY_TOP_100,
    SIMILARITY_TOP_125,
    SIMILARITY_TOP_150,
}


@dataclass(frozen=True)
class SameNeighborhoodHarvestSelection:
    strategy: str
    universe_count: int
    selected_count: int
    cap_used: int
    excluded_by_cap: int
    scored_universe: list[dict[str, Any]]
    selected_rows: list[dict[str, Any]]


def select_same_neighborhood_harvest(
    *,
    subject_snapshot: dict[str, Any],
    same_neighborhood_rows: list[dict[str, Any]],
    strategy: str,
) -> SameNeighborhoodHarvestSelection:
    if strategy not in SUPPORTED_HARVEST_STRATEGIES:
        raise ValueError(f"Unsupported same-neighborhood harvest strategy: {strategy}")

    universe_count = len(same_neighborhood_rows)
    if strategy == CURRENT_ORDER_CAP_100:
        cap_used = 100
        selected_rows = list(same_neighborhood_rows[:cap_used])
        return SameNeighborhoodHarvestSelection(
            strategy=strategy,
            universe_count=universe_count,
            selected_count=len(selected_rows),
            cap_used=cap_used,
            excluded_by_cap=max(0, universe_count - len(selected_rows)),
            scored_universe=[],
            selected_rows=selected_rows,
        )

    if strategy == DYNAMIC_CAP_150:
        cap_used = _dynamic_cap(universe_count)
        selected_rows = list(same_neighborhood_rows[:cap_used])
        return SameNeighborhoodHarvestSelection(
            strategy=strategy,
            universe_count=universe_count,
            selected_count=len(selected_rows),
            cap_used=cap_used,
            excluded_by_cap=max(0, universe_count - len(selected_rows)),
            scored_universe=[],
            selected_rows=selected_rows,
        )

    cap_used = {
        SIMILARITY_TOP_100: 100,
        SIMILARITY_TOP_125: 125,
        SIMILARITY_TOP_150: 150,
    }[strategy]
    scored_universe = _score_universe(
        subject_snapshot=subject_snapshot,
        same_neighborhood_rows=same_neighborhood_rows,
    )
    selected_accounts = {row["account_number"] for row in scored_universe[:cap_used]}
    selected_rows = [
        row for row in same_neighborhood_rows if row.get("account_number") in selected_accounts
    ]
    selected_rows.sort(
        key=lambda row: (
            -_score_lookup(scored_universe, str(row.get("account_number"))),
            str(row.get("account_number") or ""),
        )
    )
    return SameNeighborhoodHarvestSelection(
        strategy=strategy,
        universe_count=universe_count,
        selected_count=len(selected_rows),
        cap_used=cap_used,
        excluded_by_cap=max(0, universe_count - len(selected_rows)),
        scored_universe=scored_universe,
        selected_rows=selected_rows,
    )


def cheap_same_neighborhood_similarity_score(
    *,
    subject_snapshot: dict[str, Any],
    row: dict[str, Any],
) -> float:
    score = 0.0
    if _same_subdivision(subject_snapshot, row):
        score += 24.0
    else:
        score += 18.0

    score += 22.0 * _pct_similarity(
        _as_float(subject_snapshot.get("living_area_sf")),
        _as_float(row.get("living_area_sf")),
        missing_ratio=0.2,
    )
    score += 10.0 * _year_similarity(
        _as_int(subject_snapshot.get("year_built")),
        _as_int(row.get("year_built")),
    )
    score += 8.0 * _year_similarity(
        _effective_year(subject_snapshot),
        _effective_year(row),
    )
    score += 10.0 * _property_class_similarity(
        county_id=str(subject_snapshot.get("county_id") or ""),
        subject_value=subject_snapshot.get("property_class_code"),
        candidate_value=row.get("property_class_code"),
    )
    score += 8.0 * _integer_similarity(
        _as_int(subject_snapshot.get("bedrooms")),
        _as_int(row.get("bedrooms")),
        tolerance_one=0.82,
        tolerance_two=0.55,
        missing_ratio=0.35,
    )
    score += 8.0 * _float_similarity(
        _effective_baths(subject_snapshot),
        _effective_baths(row),
        one_step=0.8,
        two_step=0.45,
        missing_ratio=0.35,
    )
    score += 5.0 * _float_similarity(
        _as_float(subject_snapshot.get("stories")),
        _as_float(row.get("stories")),
        one_step=0.8,
        two_step=0.4,
        missing_ratio=0.35,
    )
    score += 5.0 * _pct_similarity(
        _preferred_land_size(subject_snapshot),
        _preferred_land_size(row),
        missing_ratio=0.5,
    )
    return round(score, 4)


def _score_universe(
    *,
    subject_snapshot: dict[str, Any],
    same_neighborhood_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    scored = []
    for row in same_neighborhood_rows:
        scored.append(
            {
                "account_number": str(row.get("account_number") or ""),
                "cheap_similarity_score": cheap_same_neighborhood_similarity_score(
                    subject_snapshot=subject_snapshot,
                    row=row,
                ),
            }
        )
    scored.sort(
        key=lambda item: (-item["cheap_similarity_score"], item["account_number"])
    )
    return scored


def _score_lookup(scored_universe: list[dict[str, Any]], account_number: str) -> float:
    for row in scored_universe:
        if row["account_number"] == account_number:
            return float(row["cheap_similarity_score"])
    return -1.0


def _dynamic_cap(universe_count: int) -> int:
    if universe_count > 300:
        return 150
    return 100


def _same_subdivision(subject_snapshot: dict[str, Any], row: dict[str, Any]) -> bool:
    subject = str(subject_snapshot.get("subdivision_name") or "").strip()
    candidate = str(row.get("subdivision_name") or "").strip()
    return subject != "" and subject == candidate


def _preferred_land_size(row: dict[str, Any]) -> float | None:
    land_sf = _as_float(row.get("land_sf"))
    if land_sf is not None and land_sf > 0:
        return land_sf
    land_acres = _as_float(row.get("land_acres"))
    if land_acres is not None and land_acres > 0:
        return land_acres * 43560.0
    return None


def _effective_year(row: dict[str, Any]) -> int | None:
    year_built = _as_int(row.get("year_built"))
    effective_age = _as_float(row.get("effective_age"))
    if year_built is None:
        return None
    if effective_age is None:
        return year_built
    return year_built - int(round(effective_age))


def _effective_baths(row: dict[str, Any]) -> float | None:
    full_baths = _as_float(row.get("full_baths"))
    half_baths = _as_float(row.get("half_baths"))
    if full_baths is None and half_baths is None:
        return None
    return round((full_baths or 0.0) + ((half_baths or 0.0) * 0.5), 2)


def _property_class_similarity(
    *,
    county_id: str,
    subject_value: Any,
    candidate_value: Any,
) -> float:
    if str(subject_value or "").strip() == "" or str(candidate_value or "").strip() == "":
        return 0.35
    relation = property_class_relation(
        county_id=county_id,
        subject_value=subject_value,
        candidate_value=candidate_value,
    )
    if relation == "exact":
        return 1.0
    if relation == "adjacent_family":
        return 0.72
    if relation == "non_adjacent":
        return 0.15
    return 0.35


def _pct_similarity(
    subject_value: float | None,
    candidate_value: float | None,
    *,
    missing_ratio: float,
) -> float:
    diff_pct = _pct_diff(subject_value, candidate_value)
    if diff_pct is None:
        return missing_ratio
    if diff_pct <= 0.05:
        return 1.0
    if diff_pct <= 0.10:
        return 0.9
    if diff_pct <= 0.15:
        return 0.75
    if diff_pct <= 0.20:
        return 0.55
    if diff_pct <= 0.30:
        return 0.35
    return 0.1


def _year_similarity(subject_value: int | None, candidate_value: int | None) -> float:
    diff = _abs_diff(subject_value, candidate_value)
    if diff is None:
        return 0.35
    if diff <= 2:
        return 1.0
    if diff <= 5:
        return 0.85
    if diff <= 10:
        return 0.65
    if diff <= 20:
        return 0.35
    return 0.12


def _integer_similarity(
    subject_value: int | None,
    candidate_value: int | None,
    *,
    tolerance_one: float,
    tolerance_two: float,
    missing_ratio: float,
) -> float:
    diff = _abs_diff(subject_value, candidate_value)
    if diff is None:
        return missing_ratio
    if diff == 0:
        return 1.0
    if diff == 1:
        return tolerance_one
    if diff == 2:
        return tolerance_two
    return 0.12


def _float_similarity(
    subject_value: float | None,
    candidate_value: float | None,
    *,
    one_step: float,
    two_step: float,
    missing_ratio: float,
) -> float:
    diff = _abs_diff(subject_value, candidate_value)
    if diff is None:
        return missing_ratio
    if diff == 0:
        return 1.0
    if diff <= 1.0:
        return one_step
    if diff <= 2.0:
        return two_step
    return 0.12


def _pct_diff(subject_value: float | None, candidate_value: float | None) -> float | None:
    if subject_value is None or candidate_value is None or subject_value == 0:
        return None
    return abs(candidate_value - subject_value) / abs(subject_value)


def _abs_diff(subject_value: int | float | None, candidate_value: int | float | None) -> float | None:
    if subject_value is None or candidate_value is None:
        return None
    return abs(float(candidate_value) - float(subject_value))


def _as_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)
