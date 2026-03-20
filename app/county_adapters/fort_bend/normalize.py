from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.county_adapters.common.field_mapping import build_normalized_record


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


def normalize(parsed_records: list[dict[str, Any]]) -> dict[str, object]:
    from app.county_adapters.common.config_loader import load_county_adapter_config

    config = load_county_adapter_config("fort_bend")
    result = normalize_property_roll(config=config, staging_rows=parsed_records)
    return {"property_roll": result.normalized_records}
