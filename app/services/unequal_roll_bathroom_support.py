from __future__ import annotations

from typing import Any


FORT_BEND_BATHROOM_SOURCE_TABLE = "fort_bend_valuation_bathroom_features"


def build_bathroom_support_context(
    *,
    county_id: str,
    canonical_full_baths: Any,
    canonical_half_baths: Any,
    valuation_bathroom_features_json: dict[str, Any] | None,
) -> dict[str, Any]:
    valuation_features = dict(valuation_bathroom_features_json or {})
    full_support = _resolve_bathroom_field_support(
        county_id=county_id,
        field_name="full_bath",
        canonical_value=canonical_full_baths,
        fallback_value=valuation_features.get("full_baths_derived"),
        valuation_features=valuation_features,
    )
    half_support = _resolve_bathroom_field_support(
        county_id=county_id,
        field_name="half_bath",
        canonical_value=canonical_half_baths,
        fallback_value=valuation_features.get("half_baths_derived"),
        valuation_features=valuation_features,
    )

    return {
        "county_id": county_id,
        "support_contract": (
            "fort_bend_bathroom_normalized"
            if county_id == "fort_bend"
            else "canonical_bathroom_only"
        ),
        "full_bath": full_support,
        "half_bath": half_support,
        "resolved_full_baths": full_support["resolved_value"],
        "resolved_half_baths": half_support["resolved_value"],
        "full_bath_clean_flag": full_support["clean_flag"],
        "half_bath_clean_flag": half_support["clean_flag"],
        "resolved_bathroom_support_flag": (
            full_support["clean_flag"] and half_support["clean_flag"]
        ),
        "unresolved_bathroom_support_flag": not (
            full_support["clean_flag"] and half_support["clean_flag"]
        ),
        "normalized_source_codes": {
            "full_bath": full_support["source_used"],
            "half_bath": half_support["source_used"],
        },
    }


def attach_bathroom_support_context(
    *,
    county_id: str,
    canonical_full_baths: Any,
    canonical_half_baths: Any,
    valuation_bathroom_features_json: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if valuation_bathroom_features_json is None:
        return None
    payload = dict(valuation_bathroom_features_json)
    payload["bathroom_support"] = build_bathroom_support_context(
        county_id=county_id,
        canonical_full_baths=canonical_full_baths,
        canonical_half_baths=canonical_half_baths,
        valuation_bathroom_features_json=valuation_bathroom_features_json,
    )
    return payload


def _resolve_bathroom_field_support(
    *,
    county_id: str,
    field_name: str,
    canonical_value: Any,
    fallback_value: Any,
    valuation_features: dict[str, Any],
) -> dict[str, Any]:
    canonical_clean_flag, canonical_normalized, canonical_reason = _classify_bathroom_value(
        field_name=field_name,
        value=canonical_value,
    )
    fallback_clean_flag, fallback_normalized, fallback_reason = _classify_bathroom_value(
        field_name=field_name,
        value=fallback_value,
    )

    source_used: str | None = None
    resolved_value: float | None = None
    clean_flag = False
    dirty_reason_code = canonical_reason
    monetized_adjustment_flag = False

    if canonical_clean_flag:
        source_used = "canonical_roll"
        resolved_value = canonical_normalized
        clean_flag = True
        dirty_reason_code = None
        monetized_adjustment_flag = True
    elif county_id == "fort_bend" and fallback_clean_flag:
        source_used = "fort_bend_valuation_bathroom_features"
        resolved_value = fallback_normalized
        clean_flag = True
        dirty_reason_code = None
        monetized_adjustment_flag = True
    elif county_id == "fort_bend":
        dirty_reason_code = fallback_reason or canonical_reason

    return {
        "field_name": field_name,
        "clean_flag": clean_flag,
        "resolved_value": resolved_value,
        "source_used": source_used,
        "monetized_adjustment_flag": monetized_adjustment_flag,
        "dirty_reason_code": dirty_reason_code,
        "canonical_raw_value": _json_safe_value(canonical_value),
        "canonical_clean_flag": canonical_clean_flag,
        "canonical_normalized_value": canonical_normalized,
        "canonical_dirty_reason_code": canonical_reason,
        "fallback_raw_value": _json_safe_value(fallback_value),
        "fallback_clean_flag": fallback_clean_flag,
        "fallback_normalized_value": fallback_normalized,
        "fallback_dirty_reason_code": fallback_reason,
        "fallback_source_table": (
            valuation_features.get("source_table") or FORT_BEND_BATHROOM_SOURCE_TABLE
        )
        if valuation_features
        else None,
        "fallback_attachment_status": valuation_features.get("attachment_status")
        if valuation_features
        else None,
        "fallback_bathroom_count_status": valuation_features.get("bathroom_count_status")
        if valuation_features
        else None,
        "fallback_bathroom_count_confidence": valuation_features.get(
            "bathroom_count_confidence"
        )
        if valuation_features
        else None,
    }


def _classify_bathroom_value(
    *,
    field_name: str,
    value: Any,
) -> tuple[bool, float | None, str | None]:
    if value is None:
        return False, None, "missing_bathroom_count"
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return False, None, "missing_bathroom_count"
        if stripped.lower() == "none":
            return False, None, "none_bathroom_count"
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return False, None, "non_numeric_bathroom_count"

    if field_name == "full_bath":
        if numeric_value == 0:
            return False, None, "zero_full_bath_count"
        if numeric_value <= 0:
            return False, None, "non_numeric_bathroom_count"
        if not float(numeric_value).is_integer():
            return False, None, "fractional_full_bath_count"
        return True, float(numeric_value), None

    if numeric_value < 0:
        return False, None, "non_numeric_bathroom_count"
    return True, float(numeric_value), None


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)
