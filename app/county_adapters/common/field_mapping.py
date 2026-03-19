from __future__ import annotations

import json
from typing import Any

from app.county_adapters.common.config_loader import (
    CountyAdapterConfig,
    DatasetFieldMappingConfig,
    FieldMappingConfig,
    SectionMappingConfig,
    SectionTemplateEntryConfig,
)
from app.utils.hashing import sha256_text
from app.utils.text_normalization import normalize_address_query

FIELD_MAPPING_NOTES = {
    "rule": "Use county config files and county adapters for county-specific mapping logic.",
}


def build_normalized_record(
    *,
    config: CountyAdapterConfig,
    dataset_type: str,
    source_row: dict[str, Any],
) -> dict[str, Any]:
    dataset_mapping = config.field_mappings.get(dataset_type)
    if dataset_mapping is None:
        raise ValueError(f"Missing field mapping config for {config.county_id}/{dataset_type}.")

    normalized: dict[str, Any] = {}
    for section_name, section_mapping in dataset_mapping.sections.items():
        normalized[section_name] = _build_section(section_mapping, source_row)
    return normalized


def required_source_fields(
    *,
    config: CountyAdapterConfig,
    dataset_type: str,
) -> list[str]:
    dataset_config = config.dataset_configs.get(dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {config.county_id}/{dataset_type}.")

    required = set(dataset_config.null_handling.get("required_source_fields", []))
    dataset_mapping = config.field_mappings.get(dataset_type)
    if dataset_mapping is None:
        return sorted(required)

    for section_mapping in dataset_mapping.sections.values():
        _collect_required_fields(section_mapping, required)
    return sorted(required)


def canonical_field_codes(
    *,
    config: CountyAdapterConfig,
    dataset_type: str,
) -> list[str]:
    dataset_mapping = config.field_mappings.get(dataset_type)
    if dataset_mapping is None:
        return []

    field_codes: set[str] = set()
    for section_mapping in dataset_mapping.sections.values():
        for field_mapping in section_mapping.fields:
            field_codes.add(field_mapping.canonical_field_code)
        for field_mapping in section_mapping.item_fields:
            field_codes.add(field_mapping.canonical_field_code)
        for template_entry in section_mapping.entries:
            for field_mapping in template_entry.fields:
                field_codes.add(field_mapping.canonical_field_code)
    return sorted(field_codes)


def _collect_required_fields(section_mapping: SectionMappingConfig, required: set[str]) -> None:
    for field_mapping in section_mapping.fields:
        if field_mapping.null_handling == "reject_row" and field_mapping.source_field is not None:
            required.add(field_mapping.source_field)
    for field_mapping in section_mapping.item_fields:
        if field_mapping.null_handling == "reject_row" and field_mapping.source_field is not None:
            required.add(field_mapping.source_field)
    for template_entry in section_mapping.entries:
        for field_mapping in template_entry.fields:
            if field_mapping.null_handling == "reject_row" and field_mapping.source_field is not None:
                required.add(field_mapping.source_field)


def _build_section(section_mapping: SectionMappingConfig, source_row: dict[str, Any]) -> Any:
    mode = section_mapping.mode
    if mode == "object":
        return _build_object(section_mapping.fields, source_row, dict(section_mapping.entry_defaults))
    if mode == "singleton_list":
        return [_build_object(section_mapping.fields, source_row, dict(section_mapping.entry_defaults))]
    if mode == "template_list":
        return [_build_template_entry(entry, source_row) for entry in section_mapping.entries]
    if mode == "source_list":
        source_items = source_row.get(section_mapping.source_field or "", []) or []
        return [
            _build_object(section_mapping.item_fields, item, dict(section_mapping.entry_defaults), source_row)
            for item in source_items
        ]
    raise ValueError(f"Unsupported mapping mode: {mode}")


def _build_template_entry(entry: SectionTemplateEntryConfig, source_row: dict[str, Any]) -> dict[str, Any]:
    return _build_object(entry.fields, source_row, dict(entry.entry_defaults))


def _build_object(
    field_mappings: list[FieldMappingConfig],
    source_row: dict[str, Any],
    seed: dict[str, Any] | None = None,
    parent_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = dict(seed or {})
    for field_mapping in field_mappings:
        record[field_mapping.target_field] = _resolve_field_value(
            field_mapping=field_mapping,
            source_row=source_row,
            parent_row=parent_row or source_row,
        )
    return record


def _resolve_field_value(
    *,
    field_mapping: FieldMappingConfig,
    source_row: dict[str, Any],
    parent_row: dict[str, Any],
) -> Any:
    transform = field_mapping.transform
    if transform == "identity":
        value = source_row.get(field_mapping.source_field or "")
    elif transform == "source_record_hash":
        value = sha256_text(json.dumps(parent_row, sort_keys=True))
    elif transform == "normalized_address":
        address, city, zip_code = (_source_values(field_mapping, parent_row) + [None, None, None])[:3]
        value = normalize_address_query(f"{address} {city} TX {zip_code}")
    elif transform == "exemption_flag":
        exemptions = source_row.get(field_mapping.source_field or "", []) or []
        exemption_code = field_mapping.transform_options.get("exemption_type_code")
        value = any(item.get("exemption_type_code") == exemption_code for item in exemptions)
    elif transform == "sum_exemption_amounts":
        exemptions = source_row.get(field_mapping.source_field or "", []) or []
        value = sum((item.get("exemption_amount") or 0) for item in exemptions)
    elif transform == "assessed_minus_exemptions":
        assessed_value, exemptions = (_source_values(field_mapping, source_row) + [0, []])[:2]
        exemption_total = sum((item.get("exemption_amount") or 0) for item in (exemptions or []))
        value = max((assessed_value or 0) - exemption_total, 0)
    elif transform == "constant":
        value = field_mapping.default_value
    else:
        raise ValueError(f"Unsupported transform '{transform}' for {field_mapping.canonical_field_code}.")

    if value is None and field_mapping.null_handling == "use_default":
        return field_mapping.default_value
    return value


def _source_values(field_mapping: FieldMappingConfig, source_row: dict[str, Any]) -> list[Any]:
    if field_mapping.source_fields:
        return [source_row.get(field_name) for field_name in field_mapping.source_fields]
    if field_mapping.source_field is not None:
        return [source_row.get(field_mapping.source_field)]
    return []
