from __future__ import annotations

from pathlib import Path

from app.county_adapters.common.base import AcquiredDataset, AdapterDataset
from app.county_adapters.common.config_loader import (
    CountyAdapterConfig,
    CountyDatasetConfig,
    resolve_dataset_year_support,
)
from app.ingestion.source_registry import get_source_registry_entry


def list_available_datasets(*, config: CountyAdapterConfig, county_id: str, tax_year: int) -> list[AdapterDataset]:
    if county_id != config.county_id:
        raise ValueError(f"Adapter for {config.county_id} cannot serve county {county_id}.")

    datasets: list[AdapterDataset] = []
    for dataset_type, dataset_config in config.dataset_configs.items():
        if not dataset_config.ingestion_ready or tax_year not in dataset_config.supported_years:
            continue
        registry_entry = get_source_registry_entry(
            county_id=config.county_id,
            dataset_type=dataset_type,
            tax_year=tax_year,
        )
        datasets.append(
            AdapterDataset(
                dataset_type=dataset_type,
                source_system_code=dataset_config.source_system_code,
                tax_year=tax_year,
                description=f"{dataset_config.description} [{registry_entry.access_method}]",
                source_url=dataset_config.source_url,
            )
        )
    return datasets


def acquire_dataset(*, config: CountyAdapterConfig, dataset_type: str, tax_year: int) -> AcquiredDataset:
    dataset_config = _dataset_config(config=config, dataset_type=dataset_type)
    year_support = resolve_dataset_year_support(
        config=config,
        dataset_type=dataset_type,
        tax_year=tax_year,
    )

    if year_support.access_method == "manual_upload":
        raise ValueError(
            f"{config.county_id} {dataset_type} for tax_year={tax_year} requires manual_upload. "
            "Register the downloaded county file with the manual historical backfill workflow, "
            "then run job_load_staging and job_normalize against that import batch."
        )

    if year_support.access_method != "fixture_csv":
        raise ValueError(
            f"Unsupported Fort Bend acquisition method '{year_support.access_method}' for {dataset_type}."
        )

    fixture_path = year_support.fixture_path
    if not fixture_path:
        raise ValueError(f"Missing fixture_path metadata for {config.county_id}/{dataset_type}.")
    content = _read_fixture(Path(fixture_path))
    return AcquiredDataset(
        dataset_type=dataset_type,
        source_system_code=dataset_config.source_system_code,
        tax_year=tax_year,
        original_filename=f"fort_bend-{dataset_type}-{tax_year}.csv",
        content=content,
        media_type="text/csv",
        source_url=year_support.source_url,
    )


def fetch(*, dataset_type: str = "property_roll", tax_year: int = 2026) -> list[dict]:
    from app.county_adapters.common.config_loader import load_county_adapter_config
    from app.county_adapters.fort_bend.parse import parse_raw_to_staging

    config = load_county_adapter_config("fort_bend")
    acquired = acquire_dataset(config=config, dataset_type=dataset_type, tax_year=tax_year)
    result = parse_raw_to_staging(config=config, acquired=acquired)
    return [row.raw_payload for row in result.staging_rows]


def _dataset_config(*, config: CountyAdapterConfig, dataset_type: str) -> CountyDatasetConfig:
    dataset_config = config.dataset_configs.get(dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {config.county_id}/{dataset_type}.")
    return dataset_config


def _read_fixture(path: Path) -> bytes:
    fixture_path = path if path.is_absolute() else Path(__file__).resolve().parents[3] / path
    return fixture_path.read_bytes()
