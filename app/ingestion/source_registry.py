from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.county_adapters.common.config_loader import (
    CountyAdapterConfig,
    CountyCapabilityConfig,
    CountyDatasetConfig,
    load_county_adapter_config,
    resolve_dataset_year_support,
)

SUPPORTED_COUNTIES = ("harris", "fort_bend")


@dataclass(frozen=True)
class SourceRegistryEntry:
    county_id: str
    dataset_type: str
    source_system_code: str
    source_name: str
    source_type: str
    county_coverage: list[str]
    entity_coverage: list[str]
    official_url: str | None
    fallback_source: str | None
    access_method: str
    file_format: str
    cadence: str
    reliability_tier: str
    auth_required: bool
    manual_fallback_supported: bool
    legal_notes: str
    parser_module_name: str
    adapter_name: str
    active_flag: bool
    supported_years: list[int]
    resolved_tax_year: int | None = None
    availability_status: str = "configured"
    availability_notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CountyCapabilityEntry:
    county_id: str
    capability_code: str
    label: str
    status: str
    source_datasets: list[str] = field(default_factory=list)
    notes: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def get_source_registry_entry(
    *, county_id: str, dataset_type: str, tax_year: int | None = None
) -> SourceRegistryEntry:
    config = load_county_adapter_config(county_id)
    dataset_config = config.dataset_configs.get(dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {county_id}/{dataset_type}.")
    return _build_entry(config=config, dataset_config=dataset_config, tax_year=tax_year)


def get_county_capability_entry(*, county_id: str, capability_code: str) -> CountyCapabilityEntry:
    config = load_county_adapter_config(county_id)
    capability = config.capability_matrix.get(capability_code)
    if capability is None:
        raise ValueError(f"Missing capability config for {county_id}/{capability_code}.")
    return _build_capability_entry(config=config, capability=capability)


def list_source_registry_entries(
    county_id: str | None = None,
    *,
    tax_year: int | None = None,
) -> list[SourceRegistryEntry]:
    county_ids = (county_id,) if county_id is not None else SUPPORTED_COUNTIES
    entries: list[SourceRegistryEntry] = []
    for current_county_id in county_ids:
        config = load_county_adapter_config(current_county_id)
        for dataset_config in config.dataset_configs.values():
            entries.append(
                _build_entry(config=config, dataset_config=dataset_config, tax_year=tax_year)
            )
    return entries


def list_county_capability_entries(county_id: str | None = None) -> list[CountyCapabilityEntry]:
    county_ids = (county_id,) if county_id is not None else SUPPORTED_COUNTIES
    entries: list[CountyCapabilityEntry] = []
    for current_county_id in county_ids:
        config = load_county_adapter_config(current_county_id)
        for capability in config.capability_matrix.values():
            entries.append(_build_capability_entry(config=config, capability=capability))
    return entries


def _build_entry(
    *,
    config: CountyAdapterConfig,
    dataset_config: CountyDatasetConfig,
    tax_year: int | None,
) -> SourceRegistryEntry:
    metadata = dataset_config.metadata
    resolved_tax_year = tax_year
    if resolved_tax_year is None and dataset_config.supported_years:
        resolved_tax_year = max(dataset_config.supported_years)
    year_support = (
        resolve_dataset_year_support(
            config=config,
            dataset_type=dataset_config.dataset_type,
            tax_year=resolved_tax_year,
        )
        if resolved_tax_year is not None
        else None
    )
    return SourceRegistryEntry(
        county_id=config.county_id,
        dataset_type=dataset_config.dataset_type,
        source_system_code=dataset_config.source_system_code,
        source_name=dataset_config.source_name,
        source_type=dataset_config.source_type,
        county_coverage=[config.county_id],
        entity_coverage=list(dataset_config.entity_coverage),
        official_url=dataset_config.source_url,
        fallback_source=metadata.get("fallback_source"),
        access_method=(
            year_support.access_method if year_support is not None else dataset_config.access_method
        ),
        file_format=dataset_config.file_format,
        cadence=dataset_config.cadence,
        reliability_tier=dataset_config.reliability_tier,
        auth_required=dataset_config.auth_required,
        manual_fallback_supported=dataset_config.manual_fallback_supported,
        legal_notes=dataset_config.legal_notes,
        parser_module_name=config.parser_module,
        adapter_name=f"{config.county_id}_county_adapter",
        active_flag=dataset_config.ingestion_ready,
        supported_years=list(dataset_config.supported_years),
        resolved_tax_year=resolved_tax_year,
        availability_status=(
            year_support.availability_status if year_support is not None else "configured"
        ),
        availability_notes=(
            list(year_support.availability_notes) if year_support is not None else []
        ),
    )


def _build_capability_entry(
    *,
    config: CountyAdapterConfig,
    capability: CountyCapabilityConfig,
) -> CountyCapabilityEntry:
    return CountyCapabilityEntry(
        county_id=config.county_id,
        capability_code=capability.capability_code,
        label=capability.label,
        status=capability.status,
        source_datasets=list(capability.source_datasets),
        notes=capability.notes,
        metadata=dict(capability.metadata),
    )
