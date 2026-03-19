from __future__ import annotations

from collections import Counter
from typing import Any

from app.county_adapters.common.base import ValidationFinding
from app.county_adapters.common.config_loader import CountyAdapterConfig
from app.county_adapters.common.field_mapping import required_source_fields


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
