from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.county_adapters.common.field_mapping import build_normalized_record
from app.utils.hashing import sha256_text


@dataclass(frozen=True)
class NormalizeResult:
    normalized_records: list[dict[str, Any]]
    row_count: int
    failed_row_count: int


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
        metadata_json = dict(row.get("metadata_json") or {})
        metadata_json["aliases"] = list(row.get("aliases") or [])
        metadata_json["assignment_hints"] = dict(row.get("assignment_hints") or {})
        normalized_records.append(
            {
                "taxing_unit": {
                    "unit_type_code": row["unit_type_code"],
                    "unit_code": row["unit_code"],
                    "unit_name": row["unit_name"],
                    "parent_unit_code": row.get("parent_unit_code"),
                    "state_geoid": row.get("state_geoid"),
                    "active_flag": row.get("active_flag", True),
                    "metadata_json": metadata_json,
                },
                "tax_rate": {
                    "rate_component": row.get("rate_component", "ad_valorem"),
                    "rate_value": row["rate_value"],
                    "rate_per_100": row.get("rate_per_100"),
                    "effective_from": row.get("effective_from"),
                    "effective_to": row.get("effective_to"),
                    "is_current": row.get("is_current", True),
                },
                "source_record_hash": source_record_hash,
            }
        )
    return NormalizeResult(
        normalized_records=normalized_records,
        row_count=len(normalized_records),
        failed_row_count=max(len(staging_rows) - len(normalized_records), 0),
    )


def normalize(parsed_records: list[dict[str, Any]]) -> dict[str, object]:
    from app.county_adapters.common.config_loader import load_county_adapter_config

    config = load_county_adapter_config("fort_bend")
    result = normalize_property_roll(config=config, staging_rows=parsed_records)
    return {"property_roll": result.normalized_records}
