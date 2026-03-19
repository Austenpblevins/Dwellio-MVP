from __future__ import annotations

from dataclasses import dataclass

from app.county_adapters.common.config_loader import CountyAdapterConfig, CountyDatasetConfig, load_county_adapter_config

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


def get_source_registry_entry(*, county_id: str, dataset_type: str) -> SourceRegistryEntry:
    config = load_county_adapter_config(county_id)
    dataset_config = config.dataset_configs.get(dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {county_id}/{dataset_type}.")
    return _build_entry(config=config, dataset_config=dataset_config)


def list_source_registry_entries(county_id: str | None = None) -> list[SourceRegistryEntry]:
    county_ids = (county_id,) if county_id is not None else SUPPORTED_COUNTIES
    entries: list[SourceRegistryEntry] = []
    for current_county_id in county_ids:
        config = load_county_adapter_config(current_county_id)
        for dataset_config in config.dataset_configs.values():
            entries.append(_build_entry(config=config, dataset_config=dataset_config))
    return entries


def _build_entry(*, config: CountyAdapterConfig, dataset_config: CountyDatasetConfig) -> SourceRegistryEntry:
    metadata = dataset_config.metadata
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
        access_method=dataset_config.access_method,
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
    )
