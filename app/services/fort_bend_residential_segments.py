from __future__ import annotations

from collections.abc import Iterable
from typing import Any

FORT_BEND_CHARACTERISTIC_SEGMENT_TYPES = {
    "MA",
    "MA1.5",
    "MA2",
    "MA3",
    "MA4",
    "MAA",
}
FORT_BEND_EXPLICIT_STORY_SEGMENTS = {
    "MA",
    "MA2",
    "MA3",
    "MA4",
}
FORT_BEND_POOL_SEGMENT_TYPES = {"RP"}
FORT_BEND_REQUIRED_RESIDENTIAL_SEGMENT_COLUMNS = {
    "QuickRefID",
    "fActYear",
    "fBedrooms",
    "fCDU",
    "fCondition",
    "fEffYear",
    "fNumHalfBath",
    "fSegClass",
    "fSegType",
    "vTSGRSeg_AdjArea",
    "vTSGRSeg_ImpNum",
    "vTSGRSeg_PoolValue",
}
FORT_BEND_PRIMARY_IMPROVEMENT_RULE_VERSION = "fort_bend_primary_residential_improvement_v1"


def fort_bend_has_pool_signal(row: dict[str, str], *, segment_type: str) -> bool:
    if segment_type in FORT_BEND_POOL_SEGMENT_TYPES:
        return True
    pool_value = _as_float(row.get("vTSGRSeg_PoolValue"))
    return pool_value is not None and pool_value > 0


def select_fort_bend_primary_residential_candidate(
    candidates: Iterable[dict[str, Any]],
) -> dict[str, Any] | None:
    ranked_candidates: list[tuple[int, int, int, dict[str, Any]]] = []
    for candidate in candidates:
        story_count = len(candidate["story_segments"])
        main_area_sqft = int(candidate["main_area_sqft"] or 0)
        improvement_num = _as_int(candidate["improvement_num"])
        tie_breaker = improvement_num if improvement_num is not None else 999999
        ranked_candidates.append((main_area_sqft, story_count, -tie_breaker, candidate))
    if not ranked_candidates:
        return None
    return max(ranked_candidates)[-1]


def _as_int(value: Any) -> int | None:
    numeric = _as_float(value)
    if numeric is None or not float(numeric).is_integer():
        return None
    return int(numeric)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
