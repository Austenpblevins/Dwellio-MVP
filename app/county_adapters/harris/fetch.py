from __future__ import annotations

from pathlib import Path

from app.county_adapters.common.base import AcquiredDataset, AdapterDataset
from app.county_adapters.common.config_loader import (
    CountyAdapterConfig,
    CountyDatasetConfig,
    resolve_dataset_year_support,
)
from app.county_adapters.common.live_acquisition import infer_live_filename, load_live_content
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"


def list_available_datasets(*, config: CountyAdapterConfig, county_id: str, tax_year: int) -> list[AdapterDataset]:
    if county_id != config.county_id:
        raise ValueError(f"Adapter for {config.county_id} cannot serve county {county_id}.")

    datasets: list[AdapterDataset] = []
    for dataset_type, dataset_config in config.dataset_configs.items():
        if not dataset_config.ingestion_ready or tax_year not in dataset_config.supported_years:
            continue
        year_support = resolve_dataset_year_support(
            config=config,
            dataset_type=dataset_type,
            tax_year=tax_year,
        )
        datasets.append(
            AdapterDataset(
                dataset_type=dataset_type,
                source_system_code=dataset_config.source_system_code,
                tax_year=tax_year,
                description=f"{dataset_config.description} [{year_support.access_method}]",
                source_url=year_support.source_url,
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

    if year_support.access_method != "fixture_json":
        if year_support.access_method not in {"live_http", "live_file"}:
            raise ValueError(
                f"Unsupported Harris acquisition method '{year_support.access_method}' for {dataset_type}."
            )
        content = load_live_content(year_support)
        return AcquiredDataset(
            dataset_type=dataset_type,
            source_system_code=dataset_config.source_system_code,
            tax_year=tax_year,
            original_filename=infer_live_filename(
                county_id=config.county_id,
                dataset_type=dataset_type,
                tax_year=tax_year,
                file_format=dataset_config.file_format,
                config=year_support,
            ),
            content=content,
            media_type="application/json",
            source_url=year_support.source_url,
        )

    fixture_path = year_support.fixture_path
    if not fixture_path:
        raise ValueError(f"Missing fixture_path metadata for {config.county_id}/{dataset_type}.")
    content = _read_fixture(Path(fixture_path))
    return AcquiredDataset(
        dataset_type=dataset_type,
        source_system_code=dataset_config.source_system_code,
        tax_year=tax_year,
        original_filename=f"harris-{dataset_type}-{tax_year}.json",
        content=content,
        media_type="application/json",
        source_url=year_support.source_url,
    )


def _dataset_config(*, config: CountyAdapterConfig, dataset_type: str) -> CountyDatasetConfig:
    dataset_config = config.dataset_configs.get(dataset_type)
    if dataset_config is None:
        raise ValueError(f"Missing dataset config for {config.county_id}/{dataset_type}.")
    return dataset_config


def _read_fixture(path: Path) -> bytes:
    fixture_path = path if path.is_absolute() else Path(__file__).resolve().parents[3] / path
    return fixture_path.read_bytes()
