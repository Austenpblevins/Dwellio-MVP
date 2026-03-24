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
TAX_RATE_LIST_FIELDS = {
    "cities",
    "school_district_names",
    "subdivisions",
    "zip_codes",
    "account_numbers",
    "aliases",
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
            normalized_row = _normalize_row(raw_row=row, dataset_type=acquired.dataset_type)
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


def parse(raw_records: list[dict[str, Any]] | None = None, *, tax_year: int = 2026) -> list[dict[str, Any]]:
    if raw_records is not None:
        return raw_records

    from app.county_adapters.common.config_loader import load_county_adapter_config
    from app.county_adapters.fort_bend.fetch import acquire_dataset

    config = load_county_adapter_config("fort_bend")
    acquired = acquire_dataset(config=config, dataset_type="property_roll", tax_year=tax_year)
    result = parse_raw_to_staging(config=config, acquired=acquired)
    return [row.raw_payload for row in result.staging_rows]


def _normalize_row(*, raw_row: dict[str, str | None], dataset_type: str) -> dict[str, Any]:
    if dataset_type == "property_roll":
        return _normalize_property_roll_row(raw_row)
    if dataset_type == "tax_rates":
        return _normalize_tax_rate_row(raw_row)
    if dataset_type == "deeds":
        return _normalize_deed_row(raw_row)
    raise ValueError(f"Unsupported Fort Bend dataset_type={dataset_type}.")


def _normalize_property_roll_row(raw_row: dict[str, str | None]) -> dict[str, Any]:
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


def _normalize_tax_rate_row(raw_row: dict[str, str | None]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in raw_row.items():
        cleaned = value.strip() if isinstance(value, str) else value
        if cleaned == "":
            cleaned = None
        if key in {"rate_value", "rate_per_100"}:
            normalized[key] = _coerce_float(cleaned, field_name=key)
        elif key in {"priority"}:
            normalized[key] = _coerce_int(cleaned, field_name=key)
        elif key in {"allow_multiple"}:
            normalized[key] = _coerce_bool(cleaned)
        elif key in TAX_RATE_LIST_FIELDS:
            normalized[key] = _split_pipe_list(cleaned)
        else:
            normalized[key] = cleaned

    assignment_hints = {
        "county_ids": (
            [normalized["cities"][0].lower().replace(" ", "_")]
            if normalized.get("unit_type_code") == "county" and normalized.get("cities")
            else []
        ),
        "cities": normalized.get("cities", []),
        "school_district_names": normalized.get("school_district_names", []),
        "subdivisions": normalized.get("subdivisions", []),
        "zip_codes": normalized.get("zip_codes", []),
        "account_numbers": normalized.get("account_numbers", []),
        "priority": normalized.get("priority"),
        "allow_multiple": normalized.get("allow_multiple", False),
    }
    if normalized.get("unit_type_code") == "county":
        assignment_hints["county_ids"] = ["fort_bend"]

    return {
        "unit_type_code": normalized["unit_type_code"],
        "unit_code": normalized["unit_code"],
        "unit_name": normalized["unit_name"],
        "rate_component": normalized.get("rate_component") or "ad_valorem",
        "rate_value": normalized["rate_value"],
        "rate_per_100": normalized.get("rate_per_100"),
        "effective_from": normalized.get("effective_from"),
        "effective_to": normalized.get("effective_to"),
        "aliases": normalized.get("aliases", []),
        "assignment_hints": assignment_hints,
        "metadata_json": {"source_family": "tax_rates_fixture"},
    }


def _build_exemptions(row: dict[str, Any]) -> list[dict[str, Any]]:
    exemptions: list[dict[str, Any]] = []
    for source_field, exemption_type_code in EXEMPTION_COLUMNS.items():
        exemption_amount = row.get(source_field) or 0
        if exemption_amount > 0:
            exemptions.append(
                {
                    "exemption_type_code": exemption_type_code,
                    "raw_exemption_code": source_field,
                    "exemption_amount": exemption_amount,
                }
            )
    return exemptions


def _normalize_deed_row(raw_row: dict[str, str | None]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in raw_row.items():
        cleaned = value.strip() if isinstance(value, str) else value
        if cleaned == "":
            cleaned = None
        normalized[key] = cleaned

    grantors = _split_pipe_list(normalized.get("grantors"))
    grantees = [
        {
            "name": grantee_name,
            "mailing_address": normalized.get("mailing_address"),
        }
        for grantee_name in _split_pipe_list(normalized.get("grantees"))
    ]
    consideration_amount = _coerce_int(
        normalized.get("consideration_amount"), field_name="consideration_amount"
    )
    alias_values = [
        value for value in [normalized.get("account_id"), normalized.get("property_id")] if value
    ]

    return {
        "instrument_number": normalized.get("instrument_number"),
        "recording_date": normalized.get("recording_date"),
        "execution_date": normalized.get("execution_date"),
        "consideration_amount": consideration_amount,
        "document_type": normalized.get("document_type"),
        "transfer_type": normalized.get("transfer_type"),
        "account_number": normalized.get("account_id"),
        "cad_property_id": normalized.get("property_id"),
        "alias_values": alias_values,
        "grantors": grantors,
        "grantees": grantees,
        "metadata_json": {"source_family": "deeds_fixture"},
    }


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


def _split_pipe_list(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split("|") if item.strip()]
