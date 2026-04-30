from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.services.unequal_roll_candidate_normalization import (
    ordinal_gap,
    property_class_relation,
)

SIMILARITY_SCORING_VERSION = "unequal_roll_similarity_v1"
SIMILARITY_SCORING_CONFIG_VERSION = "unequal_roll_similarity_v1"

COMPONENT_WEIGHTS = {
    "locality": 30.0,
    "living_area_similarity": 22.0,
    "bedroom_similarity": 10.0,
    "story_similarity": 6.0,
    "property_class_relation": 10.0,
    "quality_similarity": 12.0,
    "condition_similarity": 10.0,
}

ELIGIBILITY_STATUS_MULTIPLIERS = {
    "eligible": 1.0,
    "review": 0.85,
    "excluded": 0.35,
}


@dataclass(frozen=True)
class SimilarityScoreResult:
    raw_similarity_score: float
    normalized_similarity_score: float
    scoring_version: str
    scoring_config_version: str
    score_detail_json: dict[str, Any]


def compute_similarity_score(
    *,
    subject_snapshot: dict[str, Any],
    row: dict[str, Any],
    discovery_tier: str,
    eligibility_status: str,
    eligibility_detail_json: dict[str, Any],
    valuation_bathroom_features_json: dict[str, Any] | None,
) -> SimilarityScoreResult:
    county_id = str(subject_snapshot.get("county_id") or "")
    locality_ratio = _locality_ratio(
        subject_snapshot=subject_snapshot,
        row=row,
        discovery_tier=discovery_tier,
    )
    living_area_ratio = _living_area_ratio(
        subject_value=subject_snapshot.get("living_area_sf"),
        candidate_value=row.get("living_area_sf"),
    )
    bedroom_ratio = _bedroom_ratio(
        subject_value=subject_snapshot.get("bedrooms"),
        candidate_value=row.get("bedrooms"),
    )
    story_ratio = _story_ratio(
        subject_value=subject_snapshot.get("stories"),
        candidate_value=row.get("stories"),
    )
    property_class_ratio, property_class_relation_value = _property_class_ratio(
        county_id=county_id,
        subject_value=subject_snapshot.get("property_class_code"),
        candidate_value=row.get("property_class_code"),
    )
    quality_ratio, quality_gap_steps = _quality_ratio(
        county_id=county_id,
        subject_value=subject_snapshot.get("quality_code"),
        candidate_value=row.get("quality_code"),
    )
    condition_ratio, condition_gap_steps = _condition_ratio(
        county_id=county_id,
        subject_value=subject_snapshot.get("condition_code"),
        candidate_value=row.get("condition_code"),
    )

    components = {
        "locality": _component_detail(
            weight=COMPONENT_WEIGHTS["locality"],
            ratio=locality_ratio,
            detail={
                "same_neighborhood_flag": (
                    row.get("neighborhood_code") == subject_snapshot.get("neighborhood_code")
                ),
                "same_subdivision_flag": (
                    str(row.get("subdivision_name") or "").strip() != ""
                    and row.get("subdivision_name") == subject_snapshot.get("subdivision_name")
                ),
                "discovery_tier": discovery_tier,
            },
        ),
        "living_area_similarity": _component_detail(
            weight=COMPONENT_WEIGHTS["living_area_similarity"],
            ratio=living_area_ratio,
            detail={
                "living_area_diff_pct": _pct_diff(
                    _as_float(subject_snapshot.get("living_area_sf")),
                    _as_float(row.get("living_area_sf")),
                ),
            },
        ),
        "bedroom_similarity": _component_detail(
            weight=COMPONENT_WEIGHTS["bedroom_similarity"],
            ratio=bedroom_ratio,
            detail={
                "bedroom_diff_abs": _abs_diff(
                    _as_int(subject_snapshot.get("bedrooms")),
                    _as_int(row.get("bedrooms")),
                ),
            },
        ),
        "story_similarity": _component_detail(
            weight=COMPONENT_WEIGHTS["story_similarity"],
            ratio=story_ratio,
            detail={
                "story_diff_abs": _abs_diff(
                    _as_float(subject_snapshot.get("stories")),
                    _as_float(row.get("stories")),
                ),
            },
        ),
        "property_class_relation": _component_detail(
            weight=COMPONENT_WEIGHTS["property_class_relation"],
            ratio=property_class_ratio,
            detail={
                "property_class_relation": property_class_relation_value,
            },
        ),
        "quality_similarity": _component_detail(
            weight=COMPONENT_WEIGHTS["quality_similarity"],
            ratio=quality_ratio,
            detail={
                "quality_gap_steps": quality_gap_steps,
            },
        ),
        "condition_similarity": _component_detail(
            weight=COMPONENT_WEIGHTS["condition_similarity"],
            ratio=condition_ratio,
            detail={
                "condition_gap_steps": condition_gap_steps,
            },
        ),
    }

    base_similarity_score = round(
        sum(component["weighted_points"] for component in components.values()),
        2,
    )
    eligibility_multiplier = ELIGIBILITY_STATUS_MULTIPLIERS.get(eligibility_status, 0.5)
    fort_bend_modifier_detail = _fort_bend_bathroom_modifier_detail(
        valuation_bathroom_features_json
    )
    raw_similarity_score = round(
        base_similarity_score
        * eligibility_multiplier
        * fort_bend_modifier_detail["value"],
        2,
    )
    normalized_similarity_score = round(raw_similarity_score / 100.0, 4)

    return SimilarityScoreResult(
        raw_similarity_score=raw_similarity_score,
        normalized_similarity_score=normalized_similarity_score,
        scoring_version=SIMILARITY_SCORING_VERSION,
        scoring_config_version=SIMILARITY_SCORING_CONFIG_VERSION,
        score_detail_json={
            "scoring_version": SIMILARITY_SCORING_VERSION,
            "scoring_config_version": SIMILARITY_SCORING_CONFIG_VERSION,
            "base_similarity_score": base_similarity_score,
            "raw_similarity_score": raw_similarity_score,
            "normalized_similarity_score": normalized_similarity_score,
            "eligibility_status_multiplier": {
                "eligibility_status": eligibility_status,
                "value": eligibility_multiplier,
            },
            "fort_bend_bathroom_modifier": fort_bend_modifier_detail,
            "components": components,
            "eligibility_context": {
                "primary_reason_code": eligibility_detail_json.get("primary_reason_code"),
                "secondary_reason_codes": list(
                    eligibility_detail_json.get("secondary_reason_codes") or []
                ),
            },
        },
    )


def _component_detail(*, weight: float, ratio: float, detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "weight": weight,
        "ratio": round(ratio, 4),
        "weighted_points": round(weight * ratio, 2),
        **detail,
    }


def _locality_ratio(
    *,
    subject_snapshot: dict[str, Any],
    row: dict[str, Any],
    discovery_tier: str,
) -> float:
    same_neighborhood = row.get("neighborhood_code") == subject_snapshot.get("neighborhood_code")
    same_subdivision = (
        str(row.get("subdivision_name") or "").strip() != ""
        and row.get("subdivision_name") == subject_snapshot.get("subdivision_name")
    )
    if same_neighborhood and same_subdivision:
        return 1.0
    if same_neighborhood:
        return 0.94
    if same_subdivision:
        return 0.82
    if discovery_tier == "county_sfr_fallback":
        return 0.7
    return 0.6


def _living_area_ratio(subject_value: Any, candidate_value: Any) -> float:
    diff_pct = _pct_diff(_as_float(subject_value), _as_float(candidate_value))
    if diff_pct is None:
        return 0.3
    if diff_pct <= 0.05:
        return 1.0
    if diff_pct <= 0.10:
        return 0.92
    if diff_pct <= 0.15:
        return 0.78
    if diff_pct <= 0.20:
        return 0.6
    return 0.2


def _bedroom_ratio(subject_value: Any, candidate_value: Any) -> float:
    diff = _abs_diff(_as_int(subject_value), _as_int(candidate_value))
    if diff is None:
        return 0.35
    if diff == 0:
        return 1.0
    if diff <= 1.0:
        return 0.82
    if diff <= 2.0:
        return 0.58
    return 0.15


def _story_ratio(subject_value: Any, candidate_value: Any) -> float:
    diff = _abs_diff(_as_float(subject_value), _as_float(candidate_value))
    if diff is None:
        return 0.35
    if diff == 0:
        return 1.0
    if diff <= 0.5:
        return 0.85
    if diff <= 1.0:
        return 0.65
    return 0.15


def _property_class_ratio(
    *,
    county_id: str,
    subject_value: Any,
    candidate_value: Any,
) -> tuple[float, str | None]:
    relation = property_class_relation(county_id, subject_value, candidate_value)
    if relation == "same":
        return 1.0, relation
    if relation == "adjacent_family":
        return 0.75, relation
    if relation == "non_adjacent":
        return 0.1, relation
    if relation == "different_unmapped":
        return 0.45, relation
    return 0.35, relation


def _quality_ratio(
    *,
    county_id: str,
    subject_value: Any,
    candidate_value: Any,
) -> tuple[float, int | None]:
    gap = ordinal_gap(
        county_id=county_id,
        field_name="quality",
        subject_value=subject_value,
        candidate_value=candidate_value,
    )
    return _gap_ratio(gap=gap, subject_value=subject_value, candidate_value=candidate_value)


def _condition_ratio(
    *,
    county_id: str,
    subject_value: Any,
    candidate_value: Any,
) -> tuple[float, int | None]:
    gap = ordinal_gap(
        county_id=county_id,
        field_name="condition",
        subject_value=subject_value,
        candidate_value=candidate_value,
    )
    return _gap_ratio(gap=gap, subject_value=subject_value, candidate_value=candidate_value)


def _gap_ratio(
    *,
    gap: int | None,
    subject_value: Any,
    candidate_value: Any,
) -> tuple[float, int | None]:
    if gap is not None:
        if gap == 0:
            return 1.0, gap
        if gap == 1:
            return 0.72, gap
        return 0.2, gap

    subject_text = str(subject_value or "").strip()
    candidate_text = str(candidate_value or "").strip()
    if not subject_text or not candidate_text:
        return 0.4, gap
    if subject_text == candidate_text:
        return 1.0, gap
    return 0.45, gap


def _fort_bend_bathroom_modifier_detail(
    valuation_bathroom_features_json: dict[str, Any] | None,
) -> dict[str, Any]:
    if valuation_bathroom_features_json is None:
        return {
            "value": 1.0,
            "attachment_status": "not_applicable",
            "review_required": False,
        }

    attachment_status = valuation_bathroom_features_json.get("attachment_status")
    bathroom_count_status = valuation_bathroom_features_json.get("bathroom_count_status")
    review_required = (
        attachment_status == "attached"
        and bathroom_count_status
        not in {"exact_supported", "reconciled_fractional_plumbing", "quarter_bath_present"}
    )

    value = 1.0
    if attachment_status == "missing":
        value = 0.98
    elif review_required:
        value = 0.95

    return {
        "value": value,
        "attachment_status": attachment_status,
        "bathroom_count_status": bathroom_count_status,
        "bathroom_count_confidence": valuation_bathroom_features_json.get(
            "bathroom_count_confidence"
        ),
        "review_required": review_required,
    }


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _pct_diff(subject_value: float | None, candidate_value: float | None) -> float | None:
    if subject_value in {None, 0.0} or candidate_value is None:
        return None
    return abs(candidate_value - subject_value) / subject_value


def _abs_diff(subject_value: float | int | None, candidate_value: float | int | None) -> float | None:
    if subject_value is None or candidate_value is None:
        return None
    return abs(float(candidate_value) - float(subject_value))
