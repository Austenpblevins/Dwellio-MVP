from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.county_adapters.common.base import (
    AdapterMetadata,
    CountyAdapter,
    PublishResult,
    ValidationFinding,
)
from app.county_adapters.common.config_loader import load_county_adapter_config
from app.county_adapters.harris.fetch import acquire_dataset, list_available_datasets
from app.county_adapters.harris.normalize import normalize_deeds, normalize_property_roll, normalize_tax_rates
from app.county_adapters.harris.parse import parse_raw_to_staging
from app.county_adapters.harris.validation import validate_deeds, validate_property_roll, validate_tax_rates
from app.ingestion.source_registry import get_source_registry_entry
from app.utils.logging import get_logger

logger = get_logger(__name__)


class HarrisCountyAdapter(CountyAdapter):
    county_id = "harris"

    def __init__(self) -> None:
        self.config = load_county_adapter_config(self.county_id)

    def list_available_datasets(self, county_id: str, tax_year: int):
        return list_available_datasets(config=self.config, county_id=county_id, tax_year=tax_year)

    def acquire_dataset(self, dataset_type: str, tax_year: int):
        return acquire_dataset(config=self.config, dataset_type=dataset_type, tax_year=tax_year)

    def detect_file_format(self, file) -> str:
        if file.original_filename.endswith(".json"):
            return "json"
        raise ValueError(f"Unsupported file format for {file.original_filename}.")

    def parse_raw_to_staging(self, file):
        result = parse_raw_to_staging(config=self.config, acquired=file)
        if result.issues:
            logger.warning(
                "harris parse issues detected",
                extra={
                    "county_id": self.county_id,
                    "dataset_type": file.dataset_type,
                    "issue_count": len(result.issues),
                },
            )
        return result.staging_rows

    def normalize_staging_to_canonical(
        self,
        dataset_type: str,
        staging_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if dataset_type == "property_roll":
            result = normalize_property_roll(config=self.config, staging_rows=staging_rows)
            return {"property_roll": result.normalized_records}
        if dataset_type == "tax_rates":
            result = normalize_tax_rates(staging_rows=staging_rows)
            return {"tax_rates": result.normalized_records}
        if dataset_type == "deeds":
            result = normalize_deeds(county_id=self.county_id, staging_rows=staging_rows)
            return {"deeds": result.normalized_records}
        raise ValueError(f"Harris adapter does not support dataset_type={dataset_type}.")

    def validate_dataset(
        self,
        job_id: str,
        tax_year: int,
        dataset_type: str,
        staging_rows: list[dict[str, Any]],
    ) -> list[ValidationFinding]:
        if dataset_type == "property_roll":
            return validate_property_roll(
                config=self.config,
                job_id=job_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                staging_rows=staging_rows,
            )
        if dataset_type == "tax_rates":
            return validate_tax_rates(
                config=self.config,
                job_id=job_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                staging_rows=staging_rows,
            )
        if dataset_type == "deeds":
            return validate_deeds(
                config=self.config,
                job_id=job_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                staging_rows=staging_rows,
            )
        raise ValueError(f"Harris adapter does not support dataset_type={dataset_type}.")

    def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
        registry_entry = get_source_registry_entry(county_id=self.county_id, dataset_type=dataset_type)
        return PublishResult(
            publish_version=f"harris-{tax_year}-{dataset_type}-{job_id[:8]}",
            details_json={
                "county_id": self.county_id,
                "dataset_type": dataset_type,
                "source_system_code": registry_entry.source_system_code,
                "access_method": registry_entry.access_method,
            },
        )

    def rollback_publish(self, job_id: str) -> None:
        logger.info(
            "harris rollback hook executed",
            extra={"county_id": self.county_id, "job_id": job_id, "external_side_effects": False},
        )
        return None

    def get_adapter_metadata(self) -> AdapterMetadata:
        return AdapterMetadata(
            county_id=self.config.county_id,
            county_name="Harris County",
            appraisal_district_name=self.config.appraisal_district,
            supported_years=sorted(
                {year for dataset_config in self.config.dataset_configs.values() for year in dataset_config.supported_years}
            ),
            supported_dataset_types=self.config.datasets,
            known_limitations=[
                "Stage 6 Harris ingestion is fixture-backed for property_roll and tax_rates.",
                "Live Harris download automation remains deferred until a later county acquisition stage.",
            ],
            primary_keys_used=["account_number", "cad_property_id"],
            special_parsing_notes=(
                "Harris property_roll and tax_rates use config-driven JSON acquisition on the shared ingestion "
                "framework, with tax-rate normalization feeding the Stage 6 assignment engine."
            ),
            manual_fallback_instructions=(
                "Use the Harris fixture-backed workflow locally or register a manual upload under HCAD_BULK when "
                "live acquisition is not available."
            ),
        )


def build_harris_fixture_rows() -> list[dict[str, Any]]:
    adapter = HarrisCountyAdapter()
    acquired = adapter.acquire_dataset("property_roll", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    return [row.raw_payload for row in staging_rows]


def harris_metadata_dict() -> dict[str, Any]:
    return asdict(HarrisCountyAdapter().get_adapter_metadata())
