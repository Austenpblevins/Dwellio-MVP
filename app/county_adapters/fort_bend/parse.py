from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Any

from app.county_adapters.common.base import AcquiredDataset, StagingRow
from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.utils.hashing import sha256_text

INTEGER_FIELDS = {
    "bldg_sqft",
    "yr_built",
    "eff_yr_built",
    "eff_age",
    "bed_cnt",
    "bath_full",
    "bath_half",
    "story_count",
    "garage_capacity",
    "land_sqft",
    "land_market_value",
    "improvement_market_value",
    "market_value",
    "assessed_value",
    "notice_value",
    "appraised_value",
    "certified_value",
    "prior_market_value",
    "prior_assessed_value",
    "hs_amt",
    "ov65_amt",
}
FLOAT_FIELDS = {"land_acres"}
BOOLEAN_FIELDS = {"pool_ind"}
EXEMPTION_COLUMNS = {
    "hs_amt": "homestead",
    "ov65_amt": "over65",
}


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

    decoded = acquired.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(decoded))
    if reader.fieldnames is None:
        raise ValueError(f"Expected CSV header row for {acquired.original_filename}.")

    staging_rows: list[StagingRow] = []
    issues: list[ParseIssue] = []
    for index, row in enumerate(reader, start=1):
        if not any(value not in (None, "") for value in row.values()):
            continue
        try:
            normalized_row = _normalize_row(row)
        except ValueError as exc:
            issues.append(
                ParseIssue(
                    code="INVALID_FIELD_VALUE",
                    message=str(exc),
                    row_number=index,
                    payload={"raw_row": dict(row)},
                )
            )
            continue
        row_hash = sha256_text(json.dumps(normalized_row, sort_keys=True))
        staging_rows.append(
            StagingRow(
                table_name=dataset_config.staging_table,
                raw_payload=normalized_row,
                row_hash=row_hash,
            )
        )
    return ParseResult(staging_rows=staging_rows, issues=issues)


def parse(raw_records: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if raw_records is not None:
        return raw_records

    from app.county_adapters.common.config_loader import load_county_adapter_config
    from app.county_adapters.fort_bend.fetch import acquire_dataset

    config = load_county_adapter_config("fort_bend")
    acquired = acquire_dataset(config=config, dataset_type="property_roll", tax_year=2026)
    result = parse_raw_to_staging(config=config, acquired=acquired)
    return [row.raw_payload for row in result.staging_rows]


def _normalize_row(raw_row: dict[str, str | None]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in raw_row.items():
        cleaned = value.strip() if isinstance(value, str) else value
        if cleaned == "":
            cleaned = None
        if key in INTEGER_FIELDS:
            normalized[key] = _coerce_int(cleaned, field_name=key)
        elif key in FLOAT_FIELDS:
            normalized[key] = _coerce_float(cleaned, field_name=key)
        elif key in BOOLEAN_FIELDS:
            normalized[key] = _coerce_bool(cleaned)
        else:
            normalized[key] = cleaned

    normalized["exemptions"] = _build_exemptions(normalized)
    return normalized


def _build_exemptions(row: dict[str, Any]) -> list[dict[str, Any]]:
    exemptions: list[dict[str, Any]] = []
    for source_field, exemption_type_code in EXEMPTION_COLUMNS.items():
        exemption_amount = row.get(source_field) or 0
        if exemption_amount > 0:
            exemptions.append(
                {
                    "exemption_type_code": exemption_type_code,
                    "exemption_amount": exemption_amount,
                }
            )
    return exemptions


def _coerce_int(value: str | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError as exc:
        raise ValueError(f"Invalid integer value for {field_name}: {value}") from exc


def _coerce_float(value: str | None, *, field_name: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float value for {field_name}: {value}") from exc


def _coerce_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized_value = value.strip().lower()
    if normalized_value in {"y", "yes", "true", "1"}:
        return True
    if normalized_value in {"n", "no", "false", "0"}:
        return False
    raise ValueError(f"Invalid boolean flag: {value}")
