from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.county_adapters.common.base import (
    AcquiredDataset,
    AdapterMetadata,
    CountyAdapter,
    PublishResult,
    ValidationFinding,
)
from app.county_adapters.common.config_loader import load_county_adapter_config
from app.county_adapters.fort_bend.fetch import acquire_dataset, list_available_datasets
from app.county_adapters.fort_bend.normalize import normalize_property_roll, normalize_tax_rates
from app.county_adapters.fort_bend.parse import parse_raw_to_staging
from app.county_adapters.fort_bend.validation import validate_property_roll, validate_tax_rates
from app.ingestion.source_registry import get_source_registry_entry
from app.utils.logging import get_logger

logger = get_logger(__name__)


class FortBendCountyAdapter(CountyAdapter):
    county_id = "fort_bend"

    def __init__(self) -> None:
        self.config = load_county_adapter_config(self.county_id)

    def list_available_datasets(self, county_id: str, tax_year: int):
        return list_available_datasets(config=self.config, county_id=county_id, tax_year=tax_year)

    def acquire_dataset(self, dataset_type: str, tax_year: int):
        return acquire_dataset(config=self.config, dataset_type=dataset_type, tax_year=tax_year)

    def detect_file_format(self, file: AcquiredDataset) -> str:
        if file.original_filename.endswith(".csv"):
            return "csv"
        raise ValueError(f"Unsupported file format for {file.original_filename}.")

    def parse_raw_to_staging(self, file):
        result = parse_raw_to_staging(config=self.config, acquired=file)
        if result.issues:
            logger.warning(
                "fort_bend parse issues detected",
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
        raise ValueError(f"Fort Bend adapter does not support dataset_type={dataset_type}.")

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
        raise ValueError(f"Fort Bend adapter does not support dataset_type={dataset_type}.")

    def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
        registry_entry = get_source_registry_entry(county_id=self.county_id, dataset_type=dataset_type)
        return PublishResult(
            publish_version=f"fort_bend-{tax_year}-{dataset_type}-{job_id[:8]}",
            details_json={
                "county_id": self.county_id,
                "dataset_type": dataset_type,
                "source_system_code": registry_entry.source_system_code,
                "access_method": registry_entry.access_method,
            },
        )

    def rollback_publish(self, job_id: str) -> None:
        logger.info(
            "fort_bend rollback hook executed",
            extra={"county_id": self.county_id, "job_id": job_id, "external_side_effects": False},
        )
        return None

    def get_adapter_metadata(self) -> AdapterMetadata:
        return AdapterMetadata(
            county_id=self.config.county_id,
            county_name="Fort Bend County",
            appraisal_district_name=self.config.appraisal_district,
            supported_years=sorted(
                {year for dataset_config in self.config.dataset_configs.values() for year in dataset_config.supported_years}
            ),
            supported_dataset_types=self.config.datasets,
            known_limitations=[
                "Stage 6 Fort Bend ingestion is fixture-backed for property_roll and tax_rates.",
                "Live FBCAD download automation remains deferred until a later county acquisition stage.",
            ],
            primary_keys_used=["account_id", "property_id"],
            special_parsing_notes=(
                "Fort Bend property_roll and tax_rates use config-driven CSV parsing, with county-specific column "
                "handling feeding shared canonical normalization and Stage 6 tax assignment workflows."
            ),
            manual_fallback_instructions=(
                "Use the Fort Bend fixture-backed workflow locally or register a manual upload under FBCAD_EXPORT "
                "when live acquisition is not available."
            ),
        )


def build_fort_bend_fixture_rows() -> list[dict[str, Any]]:
    adapter = FortBendCountyAdapter()
    acquired = adapter.acquire_dataset("property_roll", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    return [row.raw_payload for row in staging_rows]


def fort_bend_metadata_dict() -> dict[str, Any]:
    return asdict(FortBendCountyAdapter().get_adapter_metadata())
