from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AdapterDataset:
    dataset_type: str
    source_system_code: str
    tax_year: int
    description: str
    source_url: str | None = None


@dataclass(frozen=True)
class AcquiredDataset:
    dataset_type: str
    source_system_code: str
    tax_year: int
    original_filename: str
    content: bytes
    media_type: str
    source_url: str | None = None


@dataclass(frozen=True)
class StagingRow:
    table_name: str
    raw_payload: dict[str, Any]
    row_hash: str


@dataclass(frozen=True)
class ValidationFinding:
    validation_code: str
    message: str
    severity: str = "info"
    validation_scope: str = "staging_row"
    entity_table: str | None = None
    details_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PublishResult:
    publish_version: str
    publish_state: str = "published"
    details_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdapterMetadata:
    county_id: str
    county_name: str
    appraisal_district_name: str
    supported_years: list[int]
    supported_dataset_types: list[str]
    known_limitations: list[str]
    primary_keys_used: list[str]
    special_parsing_notes: str
    manual_fallback_instructions: str


class CountyAdapter(ABC):
    county_id: str

    @abstractmethod
    def list_available_datasets(self, county_id: str, tax_year: int) -> list[AdapterDataset]:
        raise NotImplementedError

    @abstractmethod
    def acquire_dataset(self, dataset_type: str, tax_year: int) -> AcquiredDataset:
        raise NotImplementedError

    @abstractmethod
    def detect_file_format(self, file: AcquiredDataset) -> str:
        raise NotImplementedError

    @abstractmethod
    def parse_raw_to_staging(self, file: AcquiredDataset) -> list[StagingRow]:
        raise NotImplementedError

    @abstractmethod
    def normalize_staging_to_canonical(
        self,
        dataset_type: str,
        staging_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def validate_dataset(
        self,
        job_id: str,
        tax_year: int,
        dataset_type: str,
        staging_rows: list[dict[str, Any]],
    ) -> list[ValidationFinding]:
        raise NotImplementedError

    @abstractmethod
    def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
        raise NotImplementedError

    @abstractmethod
    def rollback_publish(self, job_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_adapter_metadata(self) -> AdapterMetadata:
        raise NotImplementedError
