from __future__ import annotations

from typing import Any

GENERIC_CONDITION_RANKS = {
    "poor": 1,
    "low": 2,
    "fair": 2,
    "average": 3,
    "average_plus": 4,
    "good": 4,
    "very_good": 5,
    "excellent": 6,
    "superior": 7,
    "luxury": 8,
    "very_low": 1,
}

HARRIS_QUALITY_RANKS = {
    "E": 1,
    "D": 2,
    "C": 3,
    "B": 4,
    "A": 5,
    "X": 6,
}

HARRIS_ADJACENT_PROPERTY_CLASS_PAIRS = {
    frozenset({"A1", "A4"}),
}


def quality_rank(county_id: str | None, value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None

    normalized_county_id = str(county_id or "").strip().lower()
    if normalized_county_id == "harris":
        harris_rank = HARRIS_QUALITY_RANKS.get(text.upper())
        if harris_rank is not None:
            return harris_rank

    if normalized_county_id == "fort_bend":
        fort_bend_rank = _parse_signed_numeric_rank(text)
        if fort_bend_rank is not None:
            return fort_bend_rank

    return GENERIC_CONDITION_RANKS.get(_normalize_label(text))


def condition_rank(county_id: str | None, value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None

    normalized_county_id = str(county_id or "").strip().lower()
    if normalized_county_id == "fort_bend":
        fort_bend_rank = _parse_signed_numeric_rank(text)
        if fort_bend_rank is not None:
            return fort_bend_rank

    return GENERIC_CONDITION_RANKS.get(_normalize_label(text))


def property_class_relation(
    county_id: str | None,
    subject_value: Any,
    candidate_value: Any,
) -> str | None:
    subject_code = str(subject_value or "").strip().upper()
    candidate_code = str(candidate_value or "").strip().upper()
    if not subject_code or not candidate_code:
        return None
    if subject_code == candidate_code:
        return "same"

    normalized_county_id = str(county_id or "").strip().lower()
    if normalized_county_id == "harris":
        if frozenset({subject_code, candidate_code}) in HARRIS_ADJACENT_PROPERTY_CLASS_PAIRS:
            return "adjacent_family"
        return "non_adjacent"

    subject_group = _leading_alpha_group(subject_code)
    candidate_group = _leading_alpha_group(candidate_code)
    if subject_group and candidate_group and subject_group == candidate_group:
        return "adjacent_family"
    if subject_group and candidate_group:
        return "non_adjacent"
    return "different_unmapped"


def ordinal_gap(
    *,
    county_id: str | None,
    field_name: str,
    subject_value: Any,
    candidate_value: Any,
) -> int | None:
    if field_name == "quality":
        subject_rank = quality_rank(county_id, subject_value)
        candidate_rank = quality_rank(county_id, candidate_value)
    else:
        subject_rank = condition_rank(county_id, subject_value)
        candidate_rank = condition_rank(county_id, candidate_value)

    if subject_rank is None or candidate_rank is None:
        return None
    return abs(candidate_rank - subject_rank)


def _parse_signed_numeric_rank(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def _leading_alpha_group(value: str) -> str:
    letters = []
    for char in value:
        if char.isalpha():
            letters.append(char)
        else:
            break
    return "".join(letters)


def _normalize_label(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")
