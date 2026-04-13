from __future__ import annotations

from collections import Counter
from typing import Any

from app.county_adapters.common.base import ValidationFinding
from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.county_adapters.common.field_mapping import required_source_fields


RATE_BEARING = "rate_bearing"
NON_RATE = "non_rate"
LINKED = "linked_to_other_taxing_unit"
CAVEATED_DEFERRED = "caveated_rate_row_deferred"
OPTIONAL_RATE_STATUSES = {NON_RATE, LINKED, CAVEATED_DEFERRED}


def validate_property_roll(
    *,
    config: CountyAdapterConfig,
    job_id: str,
    tax_year: int,
    dataset_type: str,
    staging_rows: list[dict[str, Any]],
) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    required_fields = required_source_fields(config=config, dataset_type=dataset_type)
    account_numbers = [str(row.get("account_number") or "") for row in staging_rows if row.get("account_number")]
    duplicate_accounts = {account_number for account_number, count in Counter(account_numbers).items() if count > 1}

    findings.append(
        ValidationFinding(
            validation_code="ROW_COUNT_OK",
            message=f"Validated {len(staging_rows)} staging rows for {dataset_type}.",
            validation_scope="staging_row",
            entity_table=config.dataset_configs[dataset_type].staging_table,
            details_json={
                "job_id": job_id,
                "tax_year": tax_year,
                "row_count": len(staging_rows),
                "failed_record_count": 0,
            },
        )
    )

    if tax_year not in config.dataset_configs[dataset_type].supported_years:
        findings.append(
            ValidationFinding(
                validation_code="UNSUPPORTED_TAX_YEAR",
                message=f"Unsupported tax year {tax_year} for {config.county_id}/{dataset_type}.",
                severity="error",
                validation_scope="file_schema",
                entity_table=config.dataset_configs[dataset_type].staging_table,
                details_json={"tax_year": tax_year},
            )
        )

    for index, row in enumerate(staging_rows, start=1):
        row_account_number = row.get("account_number")
        missing_fields = [field_name for field_name in required_fields if row.get(field_name) in (None, "", [])]
        for field_name in missing_fields:
            findings.append(
                ValidationFinding(
                    validation_code=f"MISSING_{field_name.upper()}",
                    message=f"{field_name} is required.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=config.dataset_configs[dataset_type].staging_table,
                    details_json={
                        "row_number": index,
                        "account_number": row_account_number,
                        "field_name": field_name,
                        "failed_record": row,
                    },
                )
            )

        if row_account_number in duplicate_accounts:
            findings.append(
                ValidationFinding(
                    validation_code="DUPLICATE_ACCOUNT_NUMBER",
                    message=f"Duplicate account_number {row_account_number} in staged property_roll batch.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=config.dataset_configs[dataset_type].staging_table,
                    details_json={
                        "row_number": index,
                        "account_number": row_account_number,
                        "failed_record": row,
                    },
                )
            )

        exemptions = row.get("exemptions", []) or []
        if not isinstance(exemptions, list):
            findings.append(
                ValidationFinding(
                    validation_code="INVALID_EXEMPTIONS_SHAPE",
                    message="exemptions must be a list when provided.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=config.dataset_configs[dataset_type].staging_table,
                    details_json={
                        "row_number": index,
                        "account_number": row_account_number,
                        "failed_record": row,
                    },
                )
            )
            continue

        for exemption in exemptions:
            exemption_amount = exemption.get("exemption_amount") or 0
            if exemption_amount < 0:
                findings.append(
                    ValidationFinding(
                        validation_code="NEGATIVE_EXEMPTION_AMOUNT",
                        message="exemption_amount cannot be negative.",
                        severity="error",
                        validation_scope="staging_row",
                        entity_table=config.dataset_configs[dataset_type].staging_table,
                        details_json={
                            "row_number": index,
                            "account_number": row_account_number,
                            "failed_record": row,
                        },
                    )
                )

        if row.get("market_value") in (None, ""):
            findings.append(
                ValidationFinding(
                    validation_code="MISSING_MARKET_VALUE",
                    message="market_value is required for canonical assessment publish.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=config.dataset_configs[dataset_type].staging_table,
                    details_json={
                        "row_number": index,
                        "account_number": row_account_number,
                        "failed_record": row,
                    },
                )
            )

    error_count = sum(1 for finding in findings if finding.severity == "error")
    findings.append(
        ValidationFinding(
            validation_code="VALIDATION_SUMMARY",
            message="Validation completed for Harris property_roll staging rows.",
            validation_scope="file_schema",
            entity_table=config.dataset_configs[dataset_type].staging_table,
            details_json={
                "job_id": job_id,
                "tax_year": tax_year,
                "row_count": len(staging_rows),
                "failed_record_count": error_count,
                "error_count": error_count,
            },
        )
    )
    return findings


def validate_tax_rates(
    *,
    config: CountyAdapterConfig,
    job_id: str,
    tax_year: int,
    dataset_type: str,
    staging_rows: list[dict[str, Any]],
) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    dataset_config = config.dataset_configs[dataset_type]
    required_fields = required_source_fields(config=config, dataset_type=dataset_type)

    findings.append(
        ValidationFinding(
            validation_code="ROW_COUNT_OK",
            message=f"Validated {len(staging_rows)} staging rows for {dataset_type}.",
            validation_scope="staging_row",
            entity_table=dataset_config.staging_table,
            details_json={"job_id": job_id, "tax_year": tax_year, "row_count": len(staging_rows)},
        )
    )

    seen_keys: set[tuple[str, str]] = set()
    for index, row in enumerate(staging_rows, start=1):
        rate_bearing_status = str(
            row.get("rate_bearing_status")
            or (row.get("metadata_json") or {}).get("rate_bearing_status")
            or RATE_BEARING
        )
        missing_fields = [
            field_name
            for field_name in required_fields
            if field_name != "rate_value" or rate_bearing_status not in OPTIONAL_RATE_STATUSES
        ]
        missing_fields = [
            field_name for field_name in missing_fields if row.get(field_name) in (None, "", [])
        ]
        for field_name in missing_fields:
            findings.append(
                ValidationFinding(
                    validation_code=f"MISSING_{field_name.upper()}",
                    message=f"{field_name} is required.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "field_name": field_name, "failed_record": row},
                )
            )

        row_key = (
            str(row.get("unit_code") or ""),
            (
                "__unit_only__"
                if rate_bearing_status in OPTIONAL_RATE_STATUSES
                else str(row.get("rate_component") or "ad_valorem")
            ),
        )
        if row_key in seen_keys:
            findings.append(
                ValidationFinding(
                    validation_code="DUPLICATE_UNIT_RATE_COMPONENT",
                    message=f"Duplicate tax-rate row detected for unit_code={row_key[0]} rate_component={row_key[1]}.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )
        seen_keys.add(row_key)

        if row.get("unit_type_code") not in {"county", "city", "school", "mud", "special"}:
            findings.append(
                ValidationFinding(
                    validation_code="INVALID_UNIT_TYPE_CODE",
                    message="unit_type_code must be county, city, school, mud, or special.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )

        if rate_bearing_status not in {
            RATE_BEARING,
            NON_RATE,
            LINKED,
            CAVEATED_DEFERRED,
        }:
            findings.append(
                ValidationFinding(
                    validation_code="INVALID_RATE_BEARING_STATUS",
                    message=(
                        "rate_bearing_status must be rate_bearing, non_rate, "
                        "linked_to_other_taxing_unit, or caveated_rate_row_deferred."
                    ),
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )

        if rate_bearing_status == RATE_BEARING and (row.get("rate_value") or 0) <= 0:
            findings.append(
                ValidationFinding(
                    validation_code="INVALID_RATE_VALUE",
                    message="rate_value must be greater than zero.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )

    error_count = sum(1 for finding in findings if finding.severity == "error")
    findings.append(
        ValidationFinding(
            validation_code="VALIDATION_SUMMARY",
            message="Validation completed for Harris tax_rates staging rows.",
            validation_scope="file_schema",
            entity_table=dataset_config.staging_table,
            details_json={
                "job_id": job_id,
                "tax_year": tax_year,
                "row_count": len(staging_rows),
                "error_count": error_count,
            },
        )
    )
    return findings


def validate_deeds(
    *,
    config: CountyAdapterConfig,
    job_id: str,
    tax_year: int,
    dataset_type: str,
    staging_rows: list[dict[str, Any]],
) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    dataset_config = config.dataset_configs[dataset_type]
    findings.append(
        ValidationFinding(
            validation_code="ROW_COUNT_OK",
            message=f"Validated {len(staging_rows)} staging rows for {dataset_type}.",
            validation_scope="staging_row",
            entity_table=dataset_config.staging_table,
            details_json={"job_id": job_id, "tax_year": tax_year, "row_count": len(staging_rows)},
        )
    )

    for index, row in enumerate(staging_rows, start=1):
        if row.get("instrument_number") in (None, ""):
            findings.append(
                ValidationFinding(
                    validation_code="MISSING_INSTRUMENT_NUMBER",
                    message="instrument_number is required for deed reconciliation.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )
        if row.get("recording_date") in (None, ""):
            findings.append(
                ValidationFinding(
                    validation_code="MISSING_RECORDING_DATE",
                    message="recording_date is required for deed reconciliation.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )
        if not row.get("grantees"):
            findings.append(
                ValidationFinding(
                    validation_code="MISSING_GRANTEE_PARTY",
                    message="At least one grantee party is required.",
                    severity="error",
                    validation_scope="staging_row",
                    entity_table=dataset_config.staging_table,
                    details_json={"row_number": index, "failed_record": row},
                )
            )

    error_count = sum(1 for finding in findings if finding.severity == "error")
    findings.append(
        ValidationFinding(
            validation_code="VALIDATION_SUMMARY",
            message="Validation completed for Harris deeds staging rows.",
            validation_scope="file_schema",
            entity_table=dataset_config.staging_table,
            details_json={
                "job_id": job_id,
                "tax_year": tax_year,
                "row_count": len(staging_rows),
                "failed_record_count": error_count,
                "error_count": error_count,
            },
        )
    )
    return findings
