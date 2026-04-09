from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.county_adapters.common.field_mapping import build_normalized_record
from app.services.ownership_reconciliation import build_normalized_deed_record
from app.utils.hashing import sha256_text


@dataclass(frozen=True)
class NormalizeResult:
    normalized_records: list[dict[str, Any]]
    row_count: int
    failed_row_count: int


HARRIS_SOURCE_TYPE_FAMILY_MAP = {
    "T": "county",
    "C": "city",
    "I": "school",
    "J": "junior_college",
    "D": "drainage",
    "B": "emergency_fire_service",
    "E": "emergency_fire_service",
    "F": "emergency_fire_service",
    "W": "water_utility",
    "M": "management_district",
    "A": "defined_area_levy",
    "Z": "overlay_participation",
}
RATE_BEARING = "rate_bearing"
NON_RATE = "non_rate"
LINKED = "linked_to_other_taxing_unit"
CAVEATED_DEFERRED = "caveated_rate_row_deferred"
EXCLUDED_RATE_BEARING_STATUSES = {NON_RATE, LINKED}


def normalize_property_roll(*, config: CountyAdapterConfig, staging_rows: list[dict[str, Any]]) -> NormalizeResult:
    normalized_records = [
        build_normalized_record(config=config, dataset_type="property_roll", source_row=row)
        for row in staging_rows
    ]
    return NormalizeResult(
        normalized_records=normalized_records,
        row_count=len(normalized_records),
        failed_row_count=max(len(staging_rows) - len(normalized_records), 0),
    )


def normalize_tax_rates(*, staging_rows: list[dict[str, Any]]) -> NormalizeResult:
    normalized_records = []
    for row in staging_rows:
        source_record_hash = sha256_text(json.dumps(row, sort_keys=True))
        metadata_json = _build_taxing_unit_metadata(row=row)
        unit_type_code = _normalize_unit_type_code(row=row, metadata_json=metadata_json)
        tax_rate = _build_tax_rate_payload(row=row, metadata_json=metadata_json)
        normalized_records.append(
            {
                "taxing_unit": {
                    "unit_type_code": unit_type_code,
                    "unit_code": row["unit_code"],
                    "unit_name": row["unit_name"],
                    "parent_unit_code": row.get("parent_unit_code"),
                    "state_geoid": row.get("state_geoid"),
                    "active_flag": row.get("active_flag", True),
                    "metadata_json": metadata_json,
                },
                "tax_rate": tax_rate,
                "source_record_hash": source_record_hash,
            }
        )
    return NormalizeResult(
        normalized_records=normalized_records,
        row_count=len(normalized_records),
        failed_row_count=max(len(staging_rows) - len(normalized_records), 0),
    )


def normalize_deeds(*, county_id: str, staging_rows: list[dict[str, Any]]) -> NormalizeResult:
    normalized_records = [
        build_normalized_deed_record(county_id=county_id, row=row)
        for row in staging_rows
    ]
    return NormalizeResult(
        normalized_records=normalized_records,
        row_count=len(normalized_records),
        failed_row_count=max(len(staging_rows) - len(normalized_records), 0),
    )


def normalize(parsed_records: list[dict[str, Any]]) -> dict[str, object]:
    from app.county_adapters.common.config_loader import load_county_adapter_config

    config = load_county_adapter_config("harris")
    result = normalize_property_roll(config=config, staging_rows=parsed_records)
    return {"property_roll": result.normalized_records}


def _build_taxing_unit_metadata(*, row: dict[str, Any]) -> dict[str, Any]:
    metadata_json = dict(row.get("metadata_json") or {})
    metadata_json["aliases"] = _dedupe_strings(
        [*metadata_json.get("aliases", []), *(row.get("aliases") or [])]
    )
    metadata_json["assignment_hints"] = _merge_assignment_hints(
        metadata_json.get("assignment_hints"),
        row.get("assignment_hints"),
        bridge_account_numbers=row.get("bridge_account_numbers") or row.get("account_numbers"),
    )
    metadata_json["harris_family_label"] = _infer_harris_family_label(row=row, metadata_json=metadata_json)
    metadata_json["rate_bearing_status"] = _infer_rate_bearing_status(
        row=row,
        metadata_json=metadata_json,
    )
    metadata_json["normalization_caveat_codes"] = _dedupe_strings(
        [
            *metadata_json.get("normalization_caveat_codes", []),
            *(row.get("normalization_caveat_codes") or []),
        ]
    )
    if row.get("linked_to_unit_code") or metadata_json.get("linked_to_unit_code"):
        metadata_json["linked_to_unit_code"] = (
            row.get("linked_to_unit_code") or metadata_json.get("linked_to_unit_code")
        )
    metadata_json["source_lineage"] = _merge_source_lineage(
        metadata_json.get("source_lineage"),
        row.get("source_lineage"),
    )
    if metadata_json["rate_bearing_status"] == CAVEATED_DEFERRED:
        metadata_json["assignment_eligible_without_rate"] = True
    return metadata_json


def _normalize_unit_type_code(*, row: dict[str, Any], metadata_json: dict[str, Any]) -> str:
    family_label = str(metadata_json.get("harris_family_label") or "").strip()
    if family_label == "county":
        return "county"
    if family_label == "city":
        return "city"
    if family_label == "school":
        return "school"
    if family_label in {"mud", "defined_area_levy"}:
        return "mud"
    unit_type_code = str(row.get("unit_type_code") or "").strip()
    if unit_type_code:
        return unit_type_code
    return "special"


def _build_tax_rate_payload(
    *,
    row: dict[str, Any],
    metadata_json: dict[str, Any],
) -> dict[str, Any] | None:
    rate_bearing_status = metadata_json["rate_bearing_status"]
    if rate_bearing_status in EXCLUDED_RATE_BEARING_STATUSES:
        return None
    if rate_bearing_status == CAVEATED_DEFERRED:
        return None
    return {
        "rate_component": row.get("rate_component", "ad_valorem"),
        "rate_value": row["rate_value"],
        "rate_per_100": row.get("rate_per_100"),
        "effective_from": row.get("effective_from"),
        "effective_to": row.get("effective_to"),
        "is_current": row.get("is_current", True),
    }


def _infer_harris_family_label(*, row: dict[str, Any], metadata_json: dict[str, Any]) -> str:
    explicit = row.get("harris_family_label") or metadata_json.get("harris_family_label")
    if explicit:
        return str(explicit)

    source_type_code = str(
        row.get("source_jurisdiction_type_code")
        or metadata_json.get("source_jurisdiction_type_code")
        or ""
    ).strip().upper()
    family_label = HARRIS_SOURCE_TYPE_FAMILY_MAP.get(source_type_code)
    unit_name = str(row.get("unit_name") or "").strip().upper()
    unit_type_code = str(row.get("unit_type_code") or "").strip()

    if "MUD" in unit_name and any(
        token in unit_name for token in (" DA ", "(DA", "DEFINED AREA")
    ):
        return "defined_area_levy"
    if "TIRZ" in unit_name or "TIFS" in unit_name or "TIF " in unit_name:
        return "overlay_participation"
    if any(token in unit_name for token in (" ANNEX", " ANNX", " ENL", " ZN", "ZONE")):
        return "overlay_participation"
    if "MUD" in unit_name or "PUD" in unit_name:
        return "mud"
    if any(token in unit_name for token in ("MGMT", "MANAGEMENT", "MMD")):
        return "management_district"
    if family_label:
        return family_label
    if unit_type_code == "county":
        return "county"
    if unit_type_code == "city":
        return "city"
    if unit_type_code == "school":
        return "school"
    if unit_type_code == "mud":
        return "mud"
    return "special"


def _infer_rate_bearing_status(*, row: dict[str, Any], metadata_json: dict[str, Any]) -> str:
    explicit = row.get("rate_bearing_status") or metadata_json.get("rate_bearing_status")
    if explicit:
        return str(explicit)
    family_label = str(metadata_json.get("harris_family_label") or "").strip()
    if family_label == "overlay_participation":
        return LINKED
    if family_label == "legacy_informational":
        return NON_RATE
    return RATE_BEARING


def _merge_assignment_hints(
    current: Any,
    incoming: Any,
    *,
    bridge_account_numbers: Any = None,
) -> dict[str, Any]:
    merged = dict(current or {})
    merged.update(dict(incoming or {}))
    for list_key in (
        "account_numbers",
        "school_district_names",
        "cities",
        "subdivisions",
        "neighborhood_codes",
        "zip_codes",
        "county_ids",
    ):
        merged[list_key] = _dedupe_strings(merged.get(list_key, []))
    bridge_values = _dedupe_strings(bridge_account_numbers or [])
    if bridge_values:
        merged["account_numbers"] = _dedupe_strings(
            [*merged.get("account_numbers", []), *bridge_values]
        )
        merged.setdefault("source", "real_acct_jurs_special_family_bridge")
    return merged


def _merge_source_lineage(current: Any, incoming: Any) -> dict[str, Any]:
    merged = dict(current or {})
    merged.update(dict(incoming or {}))
    merged.setdefault("parcel_truth_primary", "jur_value.txt")
    merged.setdefault("parcel_truth_special_family_bridge", "real_acct.jurs")
    merged.setdefault("rate_truth", "jur_tax_dist_exempt_value_rate.txt")
    return merged


def _dedupe_strings(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        deduped.append(cleaned)
        seen.add(cleaned)
    return deduped
