from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SINGLE_ASSIGNMENT_TYPES = {"county", "city", "school", "mud"}
MATCH_CONFIDENCE = {
    "account_numbers": 0.995,
    "county_ids": 0.99,
    "school_district_names": 0.96,
    "cities": 0.94,
    "subdivisions": 0.91,
    "neighborhood_codes": 0.88,
    "zip_codes": 0.82,
}
MATCH_PRIORITY = {
    "account_numbers": 120,
    "county_ids": 110,
    "school_district_names": 100,
    "cities": 90,
    "subdivisions": 80,
    "neighborhood_codes": 70,
    "zip_codes": 60,
}
MATCH_FIELD_NAMES = {
    "account_numbers": "account_number",
    "county_ids": "county_id",
    "school_district_names": "school_district_name",
    "cities": "situs_city",
    "subdivisions": "subdivision_name",
    "neighborhood_codes": "neighborhood_code",
    "zip_codes": "situs_zip",
}
MATCH_REASON_CODES = {
    "account_numbers": "match_account_number",
    "county_ids": "match_county_id",
    "school_district_names": "match_school_district_name",
    "cities": "match_city",
    "subdivisions": "match_subdivision",
    "neighborhood_codes": "match_neighborhood_code",
    "zip_codes": "match_zip_code",
}
EXCLUDED_RATE_BEARING_STATUSES = {"non_rate", "linked_to_other_taxing_unit"}


@dataclass(frozen=True)
class ParcelTaxContext:
    parcel_id: str
    county_id: str
    tax_year: int
    account_number: str
    situs_city: str | None = None
    situs_zip: str | None = None
    school_district_name: str | None = None
    subdivision_name: str | None = None
    neighborhood_code: str | None = None


@dataclass(frozen=True)
class TaxingUnitContext:
    taxing_unit_id: str
    county_id: str
    tax_year: int
    unit_type_code: str
    unit_code: str
    unit_name: str
    metadata_json: dict[str, Any]


@dataclass(frozen=True)
class ParcelTaxAssignment:
    parcel_id: str
    tax_year: int
    taxing_unit_id: str
    assignment_method: str
    assignment_confidence: float
    is_primary: bool
    assignment_reason_code: str
    match_basis_json: dict[str, Any]


@dataclass(frozen=True)
class _MatchCandidate:
    taxing_unit_id: str
    unit_type_code: str
    assignment_method: str
    assignment_confidence: float
    assignment_reason_code: str
    match_basis_json: dict[str, Any]
    priority: int
    unit_code: str


def build_tax_assignments(
    *,
    parcels: list[ParcelTaxContext],
    taxing_units: list[TaxingUnitContext],
) -> list[ParcelTaxAssignment]:
    assignments: list[ParcelTaxAssignment] = []
    units_by_county_year: dict[tuple[str, int], list[TaxingUnitContext]] = {}
    for unit in taxing_units:
        units_by_county_year.setdefault((unit.county_id, unit.tax_year), []).append(unit)

    for parcel in parcels:
        candidates_by_type: dict[str, list[_MatchCandidate]] = {}
        for unit in units_by_county_year.get((parcel.county_id, parcel.tax_year), []):
            match = _match_taxing_unit(parcel=parcel, unit=unit)
            if match is None:
                continue
            candidates_by_type.setdefault(unit.unit_type_code, []).append(match)

        for unit_type_code, candidates in candidates_by_type.items():
            ordered = sorted(
                candidates,
                key=lambda candidate: (
                    candidate.priority,
                    candidate.assignment_confidence,
                    candidate.unit_code,
                ),
                reverse=True,
            )
            if unit_type_code in SINGLE_ASSIGNMENT_TYPES:
                ordered = ordered[:1]

            for index, candidate in enumerate(ordered):
                assignments.append(
                    ParcelTaxAssignment(
                        parcel_id=parcel.parcel_id,
                        tax_year=parcel.tax_year,
                        taxing_unit_id=candidate.taxing_unit_id,
                        assignment_method=candidate.assignment_method,
                        assignment_confidence=candidate.assignment_confidence,
                        is_primary=index == 0,
                        assignment_reason_code=candidate.assignment_reason_code,
                        match_basis_json=candidate.match_basis_json,
                    )
                )
    return assignments


def _match_taxing_unit(*, parcel: ParcelTaxContext, unit: TaxingUnitContext) -> _MatchCandidate | None:
    metadata_json = unit.metadata_json or {}
    if str(metadata_json.get("rate_bearing_status") or "").strip() in EXCLUDED_RATE_BEARING_STATUSES:
        return None
    assignment_hints = dict(metadata_json.get("assignment_hints") or {})

    if unit.unit_type_code == "county" and _normalize_value(parcel.county_id) == _normalize_value(unit.county_id):
        return _build_candidate(
            parcel=parcel,
            unit=unit,
            match_key="county_ids",
            matched_value=parcel.county_id,
            candidate_values=[unit.county_id],
        )

    for match_key in (
        "account_numbers",
        "school_district_names",
        "cities",
        "subdivisions",
        "neighborhood_codes",
        "zip_codes",
        "county_ids",
    ):
        candidate_values = _normalize_list(assignment_hints.get(match_key))
        if not candidate_values:
            candidate_values = _fallback_candidate_values(unit=unit, match_key=match_key, metadata_json=metadata_json)
        if not candidate_values:
            continue

        parcel_value = _parcel_match_value(parcel=parcel, match_key=match_key)
        if parcel_value is None:
            continue
        normalized_parcel_value = _normalize_value(parcel_value)
        if normalized_parcel_value not in candidate_values:
            continue
        return _build_candidate(
            parcel=parcel,
            unit=unit,
            match_key=match_key,
            matched_value=parcel_value,
            candidate_values=candidate_values,
            hint_priority=assignment_hints.get("priority"),
        )

    return None


def _fallback_candidate_values(
    *,
    unit: TaxingUnitContext,
    match_key: str,
    metadata_json: dict[str, Any],
) -> list[str]:
    aliases = _normalize_list(metadata_json.get("aliases"))
    if match_key == "cities" and unit.unit_type_code == "city":
        return _normalize_list([unit.unit_name, *aliases])
    if match_key == "school_district_names" and unit.unit_type_code == "school":
        return _normalize_list([unit.unit_name, *aliases])
    if match_key == "subdivisions" and unit.unit_type_code == "mud":
        return aliases
    return []


def _parcel_match_value(*, parcel: ParcelTaxContext, match_key: str) -> str | None:
    field_name = MATCH_FIELD_NAMES[match_key]
    return getattr(parcel, field_name)


def _build_candidate(
    *,
    parcel: ParcelTaxContext,
    unit: TaxingUnitContext,
    match_key: str,
    matched_value: str,
    candidate_values: list[str],
    hint_priority: Any = None,
) -> _MatchCandidate:
    assignment_method = "source_direct" if match_key in {"account_numbers", "county_ids"} else "source_inferred"
    priority = int(hint_priority) if hint_priority is not None else MATCH_PRIORITY[match_key]
    return _MatchCandidate(
        taxing_unit_id=unit.taxing_unit_id,
        unit_type_code=unit.unit_type_code,
        assignment_method=assignment_method,
        assignment_confidence=MATCH_CONFIDENCE[match_key],
        assignment_reason_code=MATCH_REASON_CODES[match_key],
        match_basis_json={
            "matched_field": MATCH_FIELD_NAMES[match_key],
            "matched_value": matched_value,
            "candidate_values": candidate_values,
            "unit_code": unit.unit_code,
            "unit_name": unit.unit_name,
            "parcel_id": parcel.parcel_id,
            "hint_source": assignment_hints_source(unit.metadata_json),
            "rate_bearing_status": (unit.metadata_json or {}).get("rate_bearing_status", "rate_bearing"),
        },
        priority=priority,
        unit_code=unit.unit_code,
    )


def assignment_hints_source(metadata_json: dict[str, Any] | None) -> str | None:
    hints = dict((metadata_json or {}).get("assignment_hints") or {})
    value = hints.get("source")
    if value in (None, ""):
        return None
    return str(value)


def _normalize_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    return [
        normalized
        for normalized in (_normalize_value(value) for value in values)
        if normalized is not None
    ]


def _normalize_value(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned.upper()
