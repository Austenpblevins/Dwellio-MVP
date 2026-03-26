from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from app.county_adapters.common.base import AcquiredDataset, CountyAdapter
from app.county_adapters.common.config_loader import (
    load_county_adapter_config,
    resolve_dataset_year_support,
)
from app.db.connection import get_connection
from app.ingestion.archive import write_raw_archive
from app.ingestion.registry import get_adapter
from app.ingestion.repository import IngestionRepository
from app.utils.storage import build_storage_path


@dataclass(frozen=True)
class ManualImportRegistrationResult:
    county_id: str
    tax_year: int
    dataset_type: str
    import_batch_id: str
    raw_file_id: str
    storage_path: str
    source_system_code: str
    file_format: str
    source_filename: str
    checksum: str


def register_manual_import(
    *,
    county_id: str,
    tax_year: int,
    dataset_type: str,
    source_file_path: str,
    source_url: str | None = None,
    dry_run: bool = False,
) -> ManualImportRegistrationResult:
    config = load_county_adapter_config(county_id)
    year_support = resolve_dataset_year_support(
        config=config,
        dataset_type=dataset_type,
        tax_year=tax_year,
    )
    adapter = get_adapter(county_id)
    source_path = Path(source_file_path).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Missing source file: {source_path}")

    content = source_path.read_bytes()
    acquired = _build_acquired_dataset(
        adapter=adapter,
        county_id=county_id,
        dataset_type=dataset_type,
        tax_year=tax_year,
        source_system_code=year_support.source_system_code,
        source_path=source_path,
        source_url=source_url or year_support.source_url,
        content=content,
    )
    checksum = hashlib.sha256(content).hexdigest()
    storage_path = build_storage_path(
        county_id,
        str(tax_year),
        dataset_type,
        f"{checksum}-{source_path.name}",
    )

    with get_connection() as connection:
        repository = IngestionRepository(connection)
        source_system_id = repository.fetch_source_system_id(acquired.source_system_code)
        import_batch_id = repository.create_import_batch(
            source_system_id=source_system_id,
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            source_filename=source_path.name,
            source_checksum=checksum,
            source_url=acquired.source_url,
            file_format=adapter.detect_file_format(acquired),
            dry_run_flag=dry_run,
        )
        if not dry_run:
            write_raw_archive(storage_path, content)
        raw_file_id = repository.register_raw_file(
            import_batch_id=import_batch_id,
            source_system_id=source_system_id,
            county_id=county_id,
            tax_year=tax_year,
            storage_path=storage_path,
            original_filename=source_path.name,
            checksum=checksum,
            mime_type=acquired.media_type,
            size_bytes=len(content),
            file_kind=dataset_type,
            source_url=acquired.source_url,
            file_format=adapter.detect_file_format(acquired),
        )
        repository.update_import_batch(
            import_batch_id,
            status="fetched",
            row_count=None,
        )
        if dry_run:
            connection.rollback()
        else:
            connection.commit()

    return ManualImportRegistrationResult(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        import_batch_id=import_batch_id,
        raw_file_id=raw_file_id,
        storage_path=storage_path,
        source_system_code=acquired.source_system_code,
        file_format=adapter.detect_file_format(acquired),
        source_filename=source_path.name,
        checksum=checksum,
    )


def _build_acquired_dataset(
    *,
    adapter: CountyAdapter,
    county_id: str,
    dataset_type: str,
    tax_year: int,
    source_system_code: str,
    source_path: Path,
    source_url: str | None,
    content: bytes,
) -> AcquiredDataset:
    suffix = source_path.suffix.lower()
    if suffix == ".json":
        media_type = "application/json"
    elif suffix in {".csv", ".txt"}:
        media_type = "text/csv"
    elif suffix in {".zip", ".gz"}:
        media_type = "application/octet-stream"
    else:
        media_type = "application/octet-stream"

    acquired = AcquiredDataset(
        dataset_type=dataset_type,
        source_system_code=source_system_code,
        tax_year=tax_year,
        original_filename=source_path.name,
        content=content,
        media_type=media_type,
        source_url=source_url,
    )

    try:
        adapter.detect_file_format(acquired)
    except ValueError as exc:
        raise ValueError(
            f"Unsupported file format for {county_id}/{dataset_type}: {source_path.name}. "
            "Provide a file extension that matches the county adapter parser expectations."
        ) from exc

    return acquired
