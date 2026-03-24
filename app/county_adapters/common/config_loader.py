from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config" / "counties"


@dataclass(frozen=True)
class CountyDatasetConfig:
    dataset_type: str
    source_system_code: str
    source_name: str
    source_type: str
    description: str
    source_url: str | None
    file_format: str
    staging_table: str
    access_method: str
    cadence: str
    entity_coverage: list[str]
    supported_years: list[int]
    dependencies: list[str]
    ingestion_ready: bool
    reliability_tier: str
    auth_required: bool
    manual_fallback_supported: bool
    legal_notes: str
    null_handling: dict[str, Any] = field(default_factory=dict)
    transformation_notes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CountyDatasetYearSupport:
    dataset_type: str
    tax_year: int
    access_method: str
    file_format: str
    source_url: str | None
    source_system_code: str
    availability_status: str
    availability_notes: list[str] = field(default_factory=list)
    fixture_path: str | None = None


@dataclass(frozen=True)
class FieldMappingConfig:
    target_field: str
    canonical_field_code: str
    source_field: str | None = None
    source_fields: list[str] = field(default_factory=list)
    transform: str = "identity"
    null_handling: str = "allow_null"
    default_value: Any = None
    transform_options: dict[str, Any] = field(default_factory=dict)
    notes: str = ""


@dataclass(frozen=True)
class SectionTemplateEntryConfig:
    entry_defaults: dict[str, Any] = field(default_factory=dict)
    fields: list[FieldMappingConfig] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SectionMappingConfig:
    mode: str
    source_field: str | None = None
    entry_defaults: dict[str, Any] = field(default_factory=dict)
    fields: list[FieldMappingConfig] = field(default_factory=list)
    item_fields: list[FieldMappingConfig] = field(default_factory=list)
    entries: list[SectionTemplateEntryConfig] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetFieldMappingConfig:
    dataset_type: str
    mapping_version: int
    description: str
    sections: dict[str, SectionMappingConfig]
    transformation_notes: list[str] = field(default_factory=list)
    null_handling: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CountyAdapterConfig:
    county_id: str
    appraisal_district: str
    timezone: str
    datasets: list[str]
    parser_module: str
    notes: str
    metadata: dict[str, Any]
    dataset_configs: dict[str, CountyDatasetConfig] = field(default_factory=dict)
    field_mappings: dict[str, DatasetFieldMappingConfig] = field(default_factory=dict)


def load_county_adapter_config(county_id: str) -> CountyAdapterConfig:
    raw = _load_yaml(CONFIG_DIR / f"{county_id}.yaml")
    dataset_raw = _load_yaml(CONFIG_DIR / county_id / "datasets.yaml")
    field_mapping_raw = _load_yaml(CONFIG_DIR / county_id / "field_mappings.yaml")

    dataset_configs = _build_dataset_configs(dataset_raw.get("datasets", {}))
    field_mappings = _build_field_mappings(field_mapping_raw.get("datasets", {}))
    datasets = list(dataset_configs.keys()) or list(raw.get("datasets", []))

    return CountyAdapterConfig(
        county_id=raw.get("county_id", county_id),
        appraisal_district=raw.get("appraisal_district", ""),
        timezone=raw.get("timezone", "America/Chicago"),
        datasets=datasets,
        parser_module=raw.get("parser_module", ""),
        notes=raw.get("notes", ""),
        metadata=dict(raw.get("metadata", {})),
        dataset_configs=dataset_configs,
        field_mappings=field_mappings,
    )


def resolve_dataset_year_support(
    *,
    config: CountyAdapterConfig,
    dataset_type: str,
    tax_year: int,
) -> CountyDatasetYearSupport:
    dataset_config = config.dataset_configs.get(dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {config.county_id}/{dataset_type}.")
    if tax_year not in dataset_config.supported_years:
        raise ValueError(f"{config.county_id} {dataset_type} does not support tax_year={tax_year}.")

    metadata = dataset_config.metadata
    fixture_years = {int(year) for year in metadata.get("fixture_years", [])}
    manual_backfill_years = {int(year) for year in metadata.get("manual_backfill_years", [])}
    historical_note = metadata.get("historical_backfill_note")

    availability_notes: list[str] = []
    if isinstance(historical_note, str) and historical_note.strip():
        availability_notes.append(historical_note.strip())

    if tax_year in fixture_years:
        availability_status = "fixture_ready"
        access_method = dataset_config.access_method
        fixture_path = metadata.get("fixture_path")
    elif tax_year in manual_backfill_years:
        availability_status = "manual_upload_required"
        access_method = "manual_upload"
        fixture_path = None
    else:
        availability_status = "configured"
        access_method = dataset_config.access_method
        fixture_path = metadata.get("fixture_path")

    return CountyDatasetYearSupport(
        dataset_type=dataset_type,
        tax_year=tax_year,
        access_method=access_method,
        file_format=dataset_config.file_format,
        source_url=dataset_config.source_url,
        source_system_code=dataset_config.source_system_code,
        availability_status=availability_status,
        availability_notes=availability_notes,
        fixture_path=fixture_path,
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing county config file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected mapping root in {path}.")
    return dict(loaded)


def _build_dataset_configs(raw_datasets: dict[str, Any]) -> dict[str, CountyDatasetConfig]:
    dataset_configs: dict[str, CountyDatasetConfig] = {}
    for dataset_type, raw_dataset in raw_datasets.items():
        raw_dataset = dict(raw_dataset or {})
        dataset_configs[dataset_type] = CountyDatasetConfig(
            dataset_type=dataset_type,
            source_system_code=raw_dataset.get("source_system_code", ""),
            source_name=raw_dataset.get("source_name", dataset_type),
            source_type=raw_dataset.get("source_type", "county_bulk_export"),
            description=raw_dataset.get("description", ""),
            source_url=raw_dataset.get("source_url"),
            file_format=raw_dataset.get("file_format", "csv"),
            staging_table=raw_dataset.get("staging_table", ""),
            access_method=raw_dataset.get("access_method", "manual_upload"),
            cadence=raw_dataset.get("cadence", "ad_hoc"),
            entity_coverage=list(raw_dataset.get("entity_coverage", [])),
            supported_years=[int(year) for year in raw_dataset.get("supported_years", [])],
            dependencies=list(raw_dataset.get("dependencies", [])),
            ingestion_ready=bool(raw_dataset.get("ingestion_ready", False)),
            reliability_tier=raw_dataset.get("reliability_tier", "unknown"),
            auth_required=bool(raw_dataset.get("auth_required", False)),
            manual_fallback_supported=bool(raw_dataset.get("manual_fallback_supported", True)),
            legal_notes=raw_dataset.get("legal_notes", ""),
            null_handling=dict(raw_dataset.get("null_handling", {})),
            transformation_notes=list(raw_dataset.get("transformation_notes", [])),
            metadata=dict(raw_dataset.get("metadata", {})),
        )
    return dataset_configs


def _build_field_mappings(raw_datasets: dict[str, Any]) -> dict[str, DatasetFieldMappingConfig]:
    field_mappings: dict[str, DatasetFieldMappingConfig] = {}
    for dataset_type, raw_dataset in raw_datasets.items():
        raw_dataset = dict(raw_dataset or {})
        sections: dict[str, SectionMappingConfig] = {}
        for section_name, raw_section in dict(raw_dataset.get("sections", {})).items():
            raw_section = dict(raw_section or {})
            sections[section_name] = SectionMappingConfig(
                mode=raw_section.get("mode", "object"),
                source_field=raw_section.get("source_field"),
                entry_defaults=dict(raw_section.get("entry_defaults", {})),
                fields=[_build_field_mapping(item) for item in raw_section.get("fields", [])],
                item_fields=[_build_field_mapping(item) for item in raw_section.get("item_fields", [])],
                entries=[_build_section_entry(item) for item in raw_section.get("entries", [])],
                notes=list(raw_section.get("notes", [])),
            )
        field_mappings[dataset_type] = DatasetFieldMappingConfig(
            dataset_type=dataset_type,
            mapping_version=int(raw_dataset.get("mapping_version", 1)),
            description=raw_dataset.get("description", ""),
            sections=sections,
            transformation_notes=list(raw_dataset.get("transformation_notes", [])),
            null_handling=dict(raw_dataset.get("null_handling", {})),
        )
    return field_mappings


def _build_section_entry(raw_entry: dict[str, Any]) -> SectionTemplateEntryConfig:
    raw_entry = dict(raw_entry or {})
    return SectionTemplateEntryConfig(
        entry_defaults=dict(raw_entry.get("entry_defaults", {})),
        fields=[_build_field_mapping(item) for item in raw_entry.get("fields", [])],
        notes=list(raw_entry.get("notes", [])),
    )


def _build_field_mapping(raw_field: dict[str, Any]) -> FieldMappingConfig:
    raw_field = dict(raw_field or {})
    return FieldMappingConfig(
        target_field=raw_field["target_field"],
        canonical_field_code=raw_field["canonical_field_code"],
        source_field=raw_field.get("source_field"),
        source_fields=list(raw_field.get("source_fields", [])),
        transform=raw_field.get("transform", "identity"),
        null_handling=raw_field.get("null_handling", "allow_null"),
        default_value=raw_field.get("default_value"),
        transform_options=dict(raw_field.get("transform_options", {})),
        notes=raw_field.get("notes", ""),
    )
