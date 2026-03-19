from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from app.county_adapters.common.base import AcquiredDataset, StagingRow
from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.utils.hashing import sha256_text


@dataclass(frozen=True)
class ParseIssue:
    code: str
    message: str
    row_number: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParseResult:
    staging_rows: list[StagingRow]
    issues: list[ParseIssue]


def parse_raw_to_staging(*, config: CountyAdapterConfig, acquired: AcquiredDataset) -> ParseResult:
    dataset_config = config.dataset_configs.get(acquired.dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {config.county_id}/{acquired.dataset_type}.")

    parsed_rows = json.loads(acquired.content.decode("utf-8"))
    if not isinstance(parsed_rows, list):
        raise ValueError(f"Expected top-level list for {acquired.original_filename}.")

    staging_rows: list[StagingRow] = []
    issues: list[ParseIssue] = []
    for index, row in enumerate(parsed_rows, start=1):
        if not isinstance(row, dict):
            issues.append(
                ParseIssue(
                    code="INVALID_ROW_SHAPE",
                    message="Expected each parsed row to be an object.",
                    row_number=index,
                    payload={"value_type": type(row).__name__},
                )
            )
            continue
        row_hash = sha256_text(json.dumps(row, sort_keys=True))
        staging_rows.append(
            StagingRow(
                table_name=dataset_config.staging_table,
                raw_payload=row,
                row_hash=row_hash,
            )
        )
    return ParseResult(staging_rows=staging_rows, issues=issues)


def parse(raw_records: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if raw_records is not None:
        return raw_records

    from app.county_adapters.common.config_loader import load_county_adapter_config
    from app.county_adapters.harris.fetch import acquire_dataset

    config = load_county_adapter_config("harris")
    acquired = acquire_dataset(config=config, dataset_type="property_roll", tax_year=2026)
    result = parse_raw_to_staging(config=config, acquired=acquired)
    return [row.raw_payload for row in result.staging_rows]
