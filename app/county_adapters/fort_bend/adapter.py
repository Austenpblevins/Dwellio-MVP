from __future__ import annotations

from app.county_adapters.common.base import (
    AcquiredDataset,
    AdapterDataset,
    AdapterMetadata,
    CountyAdapter,
    PublishResult,
    StagingRow,
    ValidationFinding,
)
from app.county_adapters.common.config_loader import load_county_adapter_config
from app.utils.logging import get_logger

logger = get_logger(__name__)


class FortBendCountyAdapter(CountyAdapter):
    county_id = "fort_bend"

    def __init__(self) -> None:
        self.config = load_county_adapter_config(self.county_id)

    def list_available_datasets(self, county_id: str, tax_year: int) -> list[AdapterDataset]:
        if county_id != self.county_id:
            raise ValueError(f"Adapter for {self.county_id} cannot serve county {county_id}.")
        return [
            AdapterDataset(
                dataset_type=dataset_config.dataset_type,
                source_system_code=dataset_config.source_system_code,
                tax_year=tax_year,
                description=dataset_config.description,
                source_url=dataset_config.source_url,
            )
            for dataset_config in self.config.dataset_configs.values()
        ]

    def acquire_dataset(self, dataset_type: str, tax_year: int) -> AcquiredDataset:
        raise NotImplementedError("Fort Bend acquisition is deferred until the next county-specific stage.")

    def detect_file_format(self, file: AcquiredDataset) -> str:
        raise NotImplementedError("Fort Bend parsing is deferred until the next county-specific stage.")

    def parse_raw_to_staging(self, file: AcquiredDataset) -> list[StagingRow]:
        raise NotImplementedError("Fort Bend staging is deferred until the next county-specific stage.")

    def normalize_staging_to_canonical(self, dataset_type: str, staging_rows: list[dict[str, object]]) -> dict[str, object]:
        raise NotImplementedError("Fort Bend normalization is deferred until the next county-specific stage.")

    def validate_dataset(
        self,
        job_id: str,
        tax_year: int,
        dataset_type: str,
        staging_rows: list[dict[str, object]],
    ) -> list[ValidationFinding]:
        return [
            ValidationFinding(
                validation_code="FORT_BEND_DEFERRED",
                message="Fort Bend ingestion is intentionally deferred in Stage 2.",
                severity="warning",
                validation_scope="publish",
                details_json={"job_id": job_id, "tax_year": tax_year, "dataset_type": dataset_type},
            )
        ]

    def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
        return PublishResult(
            publish_version=f"fort_bend-{tax_year}-{dataset_type}-{job_id[:8]}",
            details_json={"county_id": self.county_id, "dataset_type": dataset_type},
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
            supported_years=[2026],
            supported_dataset_types=self.config.datasets,
            known_limitations=["Stage 2 keeps Fort Bend as a contract-only adapter scaffold."],
            primary_keys_used=["account_number"],
            special_parsing_notes="No real parsing implemented yet.",
            manual_fallback_instructions="Defer to a future Fort Bend stage or use manual upload after the county-specific adapter is implemented.",
        )
