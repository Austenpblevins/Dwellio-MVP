from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from app.county_adapters.common.base import AcquiredDataset, CountyAdapter
from app.db.connection import get_connection
from app.ingestion.archive import read_raw_archive, write_raw_archive
from app.ingestion.registry import get_adapter
from app.ingestion.repository import IngestionRepository
from app.ingestion.source_registry import get_source_registry_entry
from app.services.tax_assignment import build_tax_assignments
from app.utils.hashing import sha256_text
from app.utils.logging import get_logger
from app.utils.storage import build_storage_path

logger = get_logger(__name__)

PROPERTY_ROLL_NORMALIZE_CHUNK_SIZE = 2000
PROPERTY_ROLL_BULK_NORMALIZE_CHUNK_SIZE = 10000
PROPERTY_ROLL_DEFER_DERIVED_REFRESH_THRESHOLD = 50_000


@dataclass(frozen=True)
class PipelineStepResult:
    county_id: str
    tax_year: int
    dataset_type: str
    import_batch_id: str
    raw_file_id: str | None
    job_run_id: str
    row_count: int
    publish_version: str | None = None
    status: str = "succeeded"
    message: str | None = None


@dataclass(frozen=True)
class PipelineRunResult:
    county_id: str
    tax_year: int
    dataset_type: str
    import_batch_id: str
    rerun_of_import_batch_id: str | None
    fetch_result: PipelineStepResult
    staging_result: PipelineStepResult | None
    normalize_result: PipelineStepResult | None
    skipped_duplicate: bool = False
    skip_reason: str | None = None


@dataclass(frozen=True)
class ImportBatchInspection:
    county_id: str
    tax_year: int
    dataset_type: str
    import_batch_id: str
    status: str
    publish_state: str | None
    publish_version: str | None
    row_count: int | None
    error_count: int | None
    raw_file_count: int
    job_run_count: int
    staging_row_count: int
    lineage_record_count: int
    validation_result_count: int
    validation_error_count: int
    parcel_year_snapshot_count: int = 0
    parcel_assessment_count: int = 0
    parcel_exemption_count: int = 0
    taxing_unit_count: int = 0
    tax_rate_count: int = 0
    parcel_tax_assignment_count: int = 0
    effective_tax_rate_count: int = 0
    deed_record_count: int = 0
    deed_party_count: int = 0
    parcel_owner_period_count: int = 0
    current_owner_rollup_count: int = 0
    failed_records: list[dict[str, Any]] = field(default_factory=list)


class IngestionLifecycleService:
    def __init__(self, adapter: CountyAdapter | None = None) -> None:
        self.adapter = adapter

    def fetch_sources(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str | None = None,
        dry_run: bool = False,
    ) -> list[PipelineStepResult]:
        adapter = self._resolve_adapter(county_id)
        dataset_specs = adapter.list_available_datasets(county_id, tax_year)
        if dataset_type is not None:
            dataset_specs = [spec for spec in dataset_specs if spec.dataset_type == dataset_type]

        results: list[PipelineStepResult] = []
        with get_connection() as connection:
            repository = IngestionRepository(connection)
            for spec in dataset_specs:
                registry_entry = get_source_registry_entry(
                    county_id=county_id,
                    dataset_type=spec.dataset_type,
                    tax_year=tax_year,
                )
                logger.info(
                    "fetch_sources started",
                    extra={
                        "county_id": county_id,
                        "tax_year": tax_year,
                        "dataset_type": spec.dataset_type,
                        "dry_run": dry_run,
                    },
                )
                try:
                    acquired = adapter.acquire_dataset(spec.dataset_type, tax_year)
                except Exception as exc:
                    source_system_id = repository.fetch_source_system_id(spec.source_system_code)
                    import_batch_id = repository.create_import_batch(
                        source_system_id=source_system_id,
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type=spec.dataset_type,
                        source_filename=None,
                        source_checksum=None,
                        source_url=spec.source_url,
                        file_format=registry_entry.file_format,
                        dry_run_flag=dry_run,
                    )
                    repository.update_import_batch(
                        import_batch_id,
                        status="failed",
                        error_count=1,
                        status_reason=f"acquisition_failed: {exc}",
                    )
                    job_run_id = repository.create_job_run(
                        county_id=county_id,
                        tax_year=tax_year,
                        job_name="job_fetch_sources",
                        job_stage="fetch",
                        import_batch_id=import_batch_id,
                        raw_file_id=None,
                        dry_run_flag=dry_run,
                        metadata_json={
                            "dataset_type": spec.dataset_type,
                            "source_url": spec.source_url,
                            "acquisition_mode": registry_entry.access_method,
                        },
                    )
                    repository.complete_job_run(
                        job_run_id,
                        status="failed",
                        error_message=str(exc),
                        metadata_json={
                            "dataset_type": spec.dataset_type,
                            "failure_stage": "acquisition",
                            "acquisition_mode": registry_entry.access_method,
                        },
                    )
                    self._finalize_connection(connection, dry_run=dry_run)
                    raise

                checksum = hashlib.sha256(acquired.content).hexdigest()
                duplicate = None if dry_run else repository.find_duplicate_raw_file(
                    county_id=county_id,
                    tax_year=tax_year,
                    dataset_type=spec.dataset_type,
                    checksum=checksum,
                )
                if duplicate is not None:
                    job_run_id = repository.create_job_run(
                        county_id=county_id,
                        tax_year=tax_year,
                        job_name="job_fetch_sources",
                        job_stage="fetch",
                        import_batch_id=duplicate.import_batch_id,
                        raw_file_id=duplicate.raw_file_id,
                        dry_run_flag=False,
                        metadata_json={
                            "dataset_type": spec.dataset_type,
                            "duplicate_checksum": checksum,
                            "dedupe_action": "skip_existing_batch",
                        },
                    )
                    repository.complete_job_run(
                        job_run_id,
                        status="succeeded",
                        row_count=0,
                        metadata_json={
                            "dataset_type": spec.dataset_type,
                            "duplicate_checksum": checksum,
                            "dedupe_action": "skip_existing_batch",
                            "existing_import_batch_id": duplicate.import_batch_id,
                        },
                    )
                    self._finalize_connection(connection, dry_run=False)
                    results.append(
                        PipelineStepResult(
                            county_id=county_id,
                            tax_year=tax_year,
                            dataset_type=spec.dataset_type,
                            import_batch_id=duplicate.import_batch_id,
                            raw_file_id=duplicate.raw_file_id,
                            job_run_id=job_run_id,
                            row_count=int(duplicate.row_count or 0),
                            status="skipped_duplicate",
                            message=(
                                f"Skipped fetch because checksum already exists in import batch "
                                f"{duplicate.import_batch_id}."
                            ),
                        )
                    )
                    continue

                source_system_id = repository.fetch_source_system_id(acquired.source_system_code)
                import_batch_id = repository.create_import_batch(
                    source_system_id=source_system_id,
                    county_id=county_id,
                    tax_year=tax_year,
                    dataset_type=spec.dataset_type,
                    source_filename=acquired.original_filename,
                    source_checksum=checksum,
                    source_url=acquired.source_url,
                    file_format=adapter.detect_file_format(acquired),
                    dry_run_flag=dry_run,
                )
                storage_path = build_storage_path(
                    county_id,
                    str(tax_year),
                    spec.dataset_type,
                    f"{checksum}-{acquired.original_filename}",
                )
                if not dry_run:
                    write_raw_archive(storage_path, acquired.content)
                raw_file_id = repository.register_raw_file(
                    import_batch_id=import_batch_id,
                    source_system_id=source_system_id,
                    county_id=county_id,
                    tax_year=tax_year,
                    storage_path=storage_path,
                    original_filename=acquired.original_filename,
                    checksum=checksum,
                    mime_type=acquired.media_type,
                    size_bytes=len(acquired.content),
                    file_kind=spec.dataset_type,
                    source_url=acquired.source_url,
                    file_format=adapter.detect_file_format(acquired),
                )
                job_run_id = repository.create_job_run(
                    county_id=county_id,
                    tax_year=tax_year,
                    job_name="job_fetch_sources",
                    job_stage="fetch",
                    import_batch_id=import_batch_id,
                    raw_file_id=raw_file_id,
                    dry_run_flag=dry_run,
                    metadata_json={"dataset_type": spec.dataset_type, "storage_path": storage_path},
                )
                repository.update_import_batch(
                    import_batch_id,
                    status="fetched",
                    row_count=self._estimate_record_count(acquired),
                )
                repository.complete_job_run(
                    job_run_id,
                    status="succeeded",
                    row_count=self._estimate_record_count(acquired),
                    metadata_json={
                        "dataset_type": spec.dataset_type,
                        "storage_path": storage_path,
                        "dry_run": dry_run,
                    },
                )
                self._finalize_connection(connection, dry_run=dry_run)
                logger.info(
                    "fetch_sources completed",
                    extra={
                        "county_id": county_id,
                        "tax_year": tax_year,
                        "dataset_type": spec.dataset_type,
                        "dry_run": dry_run,
                    },
                )
                results.append(
                    PipelineStepResult(
                            county_id=county_id,
                            tax_year=tax_year,
                            dataset_type=spec.dataset_type,
                            import_batch_id=import_batch_id,
                            raw_file_id=raw_file_id,
                            job_run_id=job_run_id,
                            row_count=self._estimate_record_count(acquired),
                            message=f"Fetched {spec.dataset_type} using {registry_entry.access_method}.",
                        )
                    )
        return results

    def load_staging(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        import_batch_id: str | None = None,
        dry_run: bool = False,
    ) -> PipelineStepResult:
        adapter = self._resolve_adapter(county_id)
        with get_connection() as connection:
            repository = IngestionRepository(connection)
            logger.info(
                "load_staging started",
                extra={
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "dataset_type": dataset_type,
                    "import_batch_id": import_batch_id,
                    "dry_run": dry_run,
                },
            )
            batch = repository.find_import_batch(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=import_batch_id,
            )
            raw_content = read_raw_archive(batch.storage_path)
            acquired = AcquiredDataset(
                dataset_type=dataset_type,
                source_system_code=self._lookup_source_system_code(
                    repository, batch.source_system_id
                ),
                tax_year=tax_year,
                original_filename=batch.original_filename,
                content=raw_content,
                media_type=batch.mime_type or self._media_type_for_file_format(batch.file_format),
            )
            job_run_id = repository.create_job_run(
                county_id=county_id,
                tax_year=tax_year,
                job_name="job_load_staging",
                job_stage="staging",
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                dry_run_flag=dry_run,
                metadata_json={"dataset_type": dataset_type},
            )
            staging_rows = adapter.parse_raw_to_staging(acquired)
            inserted = repository.insert_staging_rows(
                import_batch_id=batch.import_batch_id,
                county_id=county_id,
                dataset_type=dataset_type,
                staging_rows=[
                    {"raw_payload": row.raw_payload, "row_hash": row.row_hash}
                    for row in staging_rows
                ],
            )
            findings = [
                {
                    "validation_code": finding.validation_code,
                    "message": finding.message,
                    "severity": finding.severity,
                    "validation_scope": finding.validation_scope,
                    "entity_table": finding.entity_table,
                    "details_json": finding.details_json,
                }
                for finding in adapter.validate_dataset(
                    job_run_id,
                    tax_year,
                    dataset_type,
                    [row.raw_payload for row in staging_rows],
                )
            ]
            repository.insert_validation_results(
                job_run_id=job_run_id,
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                county_id=county_id,
                tax_year=tax_year,
                findings=findings,
            )
            repository.insert_lineage_records(
                {
                    "job_run_id": job_run_id,
                    "import_batch_id": batch.import_batch_id,
                    "raw_file_id": batch.raw_file_id,
                    "relation_type": "raw_to_staging",
                    "source_table": "raw_files",
                    "source_id": batch.raw_file_id,
                    "target_table": item["staging_table"],
                    "target_id": item["staging_row_id"],
                    "source_record_hash": item["row_hash"],
                    "details_json": {"dataset_type": dataset_type},
                }
                for item in inserted
            )
            error_count = sum(1 for finding in findings if finding["severity"] == "error")
            repository.update_import_batch(
                batch.import_batch_id,
                status="staged" if error_count == 0 else "validation_failed",
                row_count=len(inserted),
                error_count=error_count,
                publish_state=None if error_count == 0 else "blocked_validation",
                status_reason=(
                    "staging_validation_passed"
                    if error_count == 0
                    else f"validation_failed: {error_count} error finding(s) blocked publish."
                ),
            )
            repository.complete_job_run(
                job_run_id,
                status="succeeded" if error_count == 0 else "failed",
                row_count=len(inserted),
                error_message=(
                    None if error_count == 0 else "Validation failed during staging load."
                ),
                metadata_json={"dataset_type": dataset_type, "dry_run": dry_run},
            )
            self._finalize_connection(connection, dry_run=dry_run)
            logger.info(
                "load_staging completed",
                extra={
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "dataset_type": dataset_type,
                    "import_batch_id": batch.import_batch_id,
                    "dry_run": dry_run,
                    "row_count": len(inserted),
                    "error_count": error_count,
                },
            )
            return PipelineStepResult(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                job_run_id=job_run_id,
                row_count=len(inserted),
            )

    def normalize(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        import_batch_id: str | None = None,
        dry_run: bool = False,
    ) -> PipelineStepResult:
        adapter = self._resolve_adapter(county_id)
        with get_connection() as connection:
            repository = IngestionRepository(connection)
            logger.info(
                "normalize started",
                extra={
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "dataset_type": dataset_type,
                    "import_batch_id": import_batch_id,
                    "dry_run": dry_run,
                },
            )
            batch = repository.find_import_batch(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=import_batch_id,
            )
            validation_error_count = repository.count_validation_errors(
                import_batch_id=batch.import_batch_id
            )
            if validation_error_count > 0:
                job_run_id = repository.create_job_run(
                    county_id=county_id,
                    tax_year=tax_year,
                    job_name="job_normalize",
                    job_stage="normalize",
                    import_batch_id=batch.import_batch_id,
                    raw_file_id=batch.raw_file_id,
                    dry_run_flag=dry_run,
                    metadata_json={"dataset_type": dataset_type},
                )
                message = (
                    f"Publish blocked because {validation_error_count} validation error finding(s) exist "
                    f"for import batch {batch.import_batch_id}."
                )
                repository.insert_validation_results(
                    job_run_id=job_run_id,
                    import_batch_id=batch.import_batch_id,
                    raw_file_id=batch.raw_file_id,
                    county_id=county_id,
                    tax_year=tax_year,
                    findings=[
                        {
                            "validation_code": "PUBLISH_BLOCKED_VALIDATION_FAILED",
                            "message": message,
                            "severity": "error",
                            "validation_scope": "publish",
                            "entity_table": None,
                            "details_json": {
                                "dataset_type": dataset_type,
                                "validation_error_count": validation_error_count,
                            },
                        }
                    ],
                )
                repository.update_import_batch(
                    batch.import_batch_id,
                    status="publish_blocked",
                    error_count=validation_error_count,
                    publish_state="blocked_validation",
                    status_reason=message,
                )
                repository.complete_job_run(
                    job_run_id,
                    status="failed",
                    row_count=0,
                    error_message=message,
                    metadata_json={
                        "dataset_type": dataset_type,
                        "validation_error_count": validation_error_count,
                        "publish_blocked": True,
                    },
                )
                self._finalize_connection(connection, dry_run=dry_run)
                raise RuntimeError(message)
            job_run_id = repository.create_job_run(
                county_id=county_id,
                tax_year=tax_year,
                job_name="job_normalize",
                job_stage="normalize",
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                dry_run_flag=dry_run,
                metadata_json={"dataset_type": dataset_type},
            )
            staging_row_count = (
                repository.count_staging_rows(
                    import_batch_id=batch.import_batch_id,
                    dataset_type=dataset_type,
                )
                if hasattr(repository, "count_staging_rows")
                else 0
            )
            bulk_property_roll_mode = (
                dataset_type == "property_roll"
                and staging_row_count > PROPERTY_ROLL_DEFER_DERIVED_REFRESH_THRESHOLD
            )
            canonical_targets: list[dict[str, str]] = []
            property_roll_row_count = 0
            rollback_manifest: dict[str, Any]

            if dataset_type == "property_roll":
                rollback_manifest = {"dataset_type": "property_roll", "entries": []}
                for staged_rows in repository.iterate_staging_rows(
                    import_batch_id=batch.import_batch_id,
                    dataset_type=dataset_type,
                    chunk_size=(
                        PROPERTY_ROLL_BULK_NORMALIZE_CHUNK_SIZE
                        if bulk_property_roll_mode
                        else PROPERTY_ROLL_NORMALIZE_CHUNK_SIZE
                    ),
                ):
                    normalized = adapter.normalize_staging_to_canonical(
                        dataset_type,
                        [row["raw_payload"] for row in staged_rows],
                    )
                    property_roll_records = normalized["property_roll"]
                    rollback_chunk = repository.capture_property_roll_rollback_manifest(
                        county_id=county_id,
                        tax_year=tax_year,
                        account_numbers=[
                            record["parcel"]["account_number"] for record in property_roll_records
                        ],
                    )
                    rollback_manifest["entries"].extend(rollback_chunk.get("entries", []))
                    chunk_targets = repository.upsert_property_roll_records(
                        county_id=county_id,
                        tax_year=tax_year,
                        import_batch_id=batch.import_batch_id,
                        job_run_id=job_run_id,
                        source_system_id=batch.source_system_id,
                        normalized_records=property_roll_records,
                        include_detail_tables=not bulk_property_roll_mode,
                    )
                    if not bulk_property_roll_mode:
                        repository.insert_lineage_records(
                            {
                                "job_run_id": job_run_id,
                                "import_batch_id": batch.import_batch_id,
                                "raw_file_id": batch.raw_file_id,
                                "relation_type": "staging_to_canonical",
                                "source_table": staged_rows[index]["staging_table"],
                                "source_id": staged_rows[index]["staging_row_id"],
                                "target_table": target["target_table"],
                                "target_id": target["target_id"],
                                "source_record_hash": staged_rows[index]["row_hash"],
                                "details_json": {
                                    "dataset_type": dataset_type,
                                    "parcel_id": target.get("parcel_id"),
                                    "taxing_unit_id": target.get("taxing_unit_id"),
                                },
                            }
                            for index, target in enumerate(chunk_targets)
                        )
                    property_roll_row_count += len(chunk_targets)
                    if not bulk_property_roll_mode:
                        canonical_targets.extend(chunk_targets)
            elif dataset_type == "tax_rates":
                staged_rows = repository.fetch_staging_rows(
                    import_batch_id=batch.import_batch_id,
                    dataset_type=dataset_type,
                )
                normalized = adapter.normalize_staging_to_canonical(
                    dataset_type,
                    [row["raw_payload"] for row in staged_rows],
                )
                rollback_manifest = repository.capture_tax_rate_rollback_manifest(
                    county_id=county_id,
                    tax_year=tax_year,
                    unit_codes=[
                        record["taxing_unit"]["unit_code"] for record in normalized["tax_rates"]
                    ],
                )
                canonical_targets = repository.upsert_tax_rate_records(
                    county_id=county_id,
                    tax_year=tax_year,
                    import_batch_id=batch.import_batch_id,
                    job_run_id=job_run_id,
                    source_system_id=batch.source_system_id,
                    normalized_records=normalized["tax_rates"],
                )
            elif dataset_type == "deeds":
                staged_rows = repository.fetch_staging_rows(
                    import_batch_id=batch.import_batch_id,
                    dataset_type=dataset_type,
                )
                normalized = adapter.normalize_staging_to_canonical(
                    dataset_type,
                    [row["raw_payload"] for row in staged_rows],
                )
                rollback_manifest = repository.capture_deed_rollback_manifest(
                    county_id=county_id,
                    tax_year=tax_year,
                    normalized_records=normalized["deeds"],
                )
                canonical_targets = repository.upsert_deed_records(
                    county_id=county_id,
                    tax_year=tax_year,
                    import_batch_id=batch.import_batch_id,
                    job_run_id=job_run_id,
                    source_system_id=batch.source_system_id,
                    normalized_records=normalized["deeds"],
                )
            else:
                raise ValueError(f"Unsupported normalize dataset_type={dataset_type}.")
            if dataset_type != "property_roll":
                repository.insert_lineage_records(
                    {
                        "job_run_id": job_run_id,
                        "import_batch_id": batch.import_batch_id,
                        "raw_file_id": batch.raw_file_id,
                        "relation_type": "staging_to_canonical",
                        "source_table": staged_rows[index]["staging_table"],
                        "source_id": staged_rows[index]["staging_row_id"],
                        "target_table": target["target_table"],
                        "target_id": target["target_id"],
                        "source_record_hash": staged_rows[index]["row_hash"],
                        "details_json": {
                            "dataset_type": dataset_type,
                            "parcel_id": target.get("parcel_id"),
                            "taxing_unit_id": target.get("taxing_unit_id"),
                        },
                    }
                    for index, target in enumerate(canonical_targets)
                )

            deferred_steps: list[str] = []
            if not bulk_property_roll_mode:
                self._refresh_tax_assignments(
                    repository=repository,
                    county_id=county_id,
                    tax_year=tax_year,
                    import_batch_id=batch.import_batch_id,
                    job_run_id=job_run_id,
                    source_system_id=batch.source_system_id,
                    force=dataset_type == "tax_rates",
                )
            elif dataset_type == "property_roll":
                deferred_steps.append("tax_assignments")

            if dataset_type in {"property_roll", "deeds"} and not bulk_property_roll_mode:
                self._refresh_owner_reconciliation(
                    repository=repository,
                    county_id=county_id,
                    tax_year=tax_year,
                    parcel_ids=[
                        target["parcel_id"]
                        for target in canonical_targets
                        if target.get("parcel_id") is not None
                    ],
                )
            elif dataset_type == "property_roll" and bulk_property_roll_mode:
                deferred_steps.append("owner_reconciliation")
                deferred_steps.append("lineage_records")

            if dataset_type in {"property_roll", "deeds"}:
                self._refresh_search_documents(
                    repository=repository,
                    county_id=county_id,
                    tax_year=tax_year,
                )
            publish_result = adapter.publish_dataset(job_run_id, tax_year, dataset_type)
            repository.insert_validation_results(
                job_run_id=job_run_id,
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                county_id=county_id,
                tax_year=tax_year,
                findings=[
                    {
                        "validation_code": "PUBLISH_OK",
                        "message": "Dataset published to canonical tables.",
                        "severity": "info",
                        "validation_scope": "publish",
                        "entity_table": (
                            "parcel_year_snapshots"
                            if dataset_type == "property_roll"
                            else "tax_rates" if dataset_type == "tax_rates" else "deed_records"
                        ),
                        "details_json": publish_result.details_json,
                    }
                ],
            )
            if deferred_steps:
                repository.insert_validation_results(
                    job_run_id=job_run_id,
                    import_batch_id=batch.import_batch_id,
                    raw_file_id=batch.raw_file_id,
                    county_id=county_id,
                    tax_year=tax_year,
                    findings=[
                        {
                            "validation_code": "DERIVED_REFRESH_DEFERRED",
                            "message": (
                                "Deferred post-publish refresh steps for bulk property_roll publish "
                                "to keep real-county ingest within PR1 runtime bounds."
                            ),
                            "severity": "info",
                            "validation_scope": "publish",
                            "entity_table": "parcel_year_snapshots",
                            "details_json": {
                                "dataset_type": dataset_type,
                                "deferred_steps": deferred_steps,
                                "staging_row_count": staging_row_count,
                            },
                        }
                    ],
                )
            repository.update_import_batch(
                batch.import_batch_id,
                status="normalized",
                row_count=property_roll_row_count if dataset_type == "property_roll" else len(canonical_targets),
                error_count=0,
                publish_state=publish_result.publish_state,
                publish_version=publish_result.publish_version,
                status_reason=f"published_to_canonical: {dataset_type} publish succeeded.",
            )
            repository.complete_job_run(
                job_run_id,
                status="succeeded",
                row_count=property_roll_row_count if dataset_type == "property_roll" else len(canonical_targets),
                publish_version=publish_result.publish_version,
                metadata_json={
                    "dataset_type": dataset_type,
                    "dry_run": dry_run,
                    "publish_result": publish_result.details_json,
                    "rollback_manifest": rollback_manifest,
                    "deferred_post_publish_steps": deferred_steps,
                },
            )
            self._finalize_connection(connection, dry_run=dry_run)
            logger.info(
                "normalize completed",
                extra={
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "dataset_type": dataset_type,
                    "import_batch_id": batch.import_batch_id,
                    "dry_run": dry_run,
                    "row_count": property_roll_row_count if dataset_type == "property_roll" else len(canonical_targets),
                    "publish_version": publish_result.publish_version,
                },
            )
            return PipelineStepResult(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                job_run_id=job_run_id,
                row_count=property_roll_row_count if dataset_type == "property_roll" else len(canonical_targets),
                publish_version=publish_result.publish_version,
            )

    def rollback_publish(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        import_batch_id: str | None = None,
    ) -> None:
        adapter = self._resolve_adapter(county_id)
        with get_connection() as connection:
            repository = IngestionRepository(connection)
            batch = repository.find_import_batch(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=import_batch_id,
            )
            logger.info(
                "rollback_publish started",
                extra={
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "dataset_type": dataset_type,
                    "import_batch_id": batch.import_batch_id,
                },
            )
            normalize_metadata = repository.fetch_job_run_metadata(
                import_batch_id=batch.import_batch_id,
                job_stage="normalize",
            )
            rollback_manifest = (
                None if normalize_metadata is None else normalize_metadata.get("rollback_manifest")
            )
            if rollback_manifest is None:
                raise ValueError(
                    "Rollback manifest unavailable for this import batch. "
                    "Run a Stage 2 normalize built with rollback support before rolling back."
                )
            job_run_id = repository.create_job_run(
                county_id=county_id,
                tax_year=tax_year,
                job_name="job_normalize",
                job_stage="rollback",
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                dry_run_flag=False,
                metadata_json={"dataset_type": dataset_type},
            )
            if dataset_type == "property_roll":
                rolled_back_count = repository.rollback_property_roll_records(
                    import_batch_id=batch.import_batch_id,
                    tax_year=tax_year,
                    rollback_manifest=rollback_manifest,
                )
            elif dataset_type == "tax_rates":
                rolled_back_count = repository.rollback_tax_rate_records(
                    county_id=county_id,
                    import_batch_id=batch.import_batch_id,
                    tax_year=tax_year,
                    rollback_manifest=rollback_manifest,
                )
            elif dataset_type == "deeds":
                rolled_back_count = repository.rollback_deed_records(
                    county_id=county_id,
                    import_batch_id=batch.import_batch_id,
                    tax_year=tax_year,
                    rollback_manifest=rollback_manifest,
                )
            else:
                raise ValueError(f"Unsupported rollback dataset_type={dataset_type}.")
            adapter.rollback_publish(job_run_id)
            self._refresh_tax_assignments(
                repository=repository,
                county_id=county_id,
                tax_year=tax_year,
                import_batch_id=batch.import_batch_id,
                job_run_id=job_run_id,
                source_system_id=batch.source_system_id,
                force=dataset_type == "tax_rates",
            )
            if dataset_type in {"property_roll", "deeds"}:
                self._refresh_owner_reconciliation(
                    repository=repository,
                    county_id=county_id,
                    tax_year=tax_year,
                    parcel_ids=None,
                )
                self._refresh_search_documents(
                    repository=repository,
                    county_id=county_id,
                    tax_year=tax_year,
                )
            repository.insert_validation_results(
                job_run_id=job_run_id,
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                county_id=county_id,
                tax_year=tax_year,
                findings=[
                    {
                        "validation_code": "ROLLBACK_OK",
                        "message": "Rolled back canonical publish state.",
                        "severity": "info",
                        "validation_scope": "publish",
                        "entity_table": (
                            "parcel_year_snapshots"
                            if dataset_type == "property_roll"
                            else "tax_rates" if dataset_type == "tax_rates" else "deed_records"
                        ),
                        "details_json": {
                            "dataset_type": dataset_type,
                            "rows_rolled_back": rolled_back_count,
                        },
                    }
                ],
            )
            repository.update_import_batch(
                batch.import_batch_id,
                status="rolled_back",
                publish_state="rolled_back",
                status_reason=f"rollback_completed: rolled back {dataset_type} publish and refreshed dependent search state.",
            )
            repository.complete_job_run(
                job_run_id,
                status="succeeded",
                row_count=rolled_back_count,
                metadata_json={"dataset_type": dataset_type, "rows_rolled_back": rolled_back_count},
            )
            connection.commit()
            logger.info(
                "rollback_publish completed",
                extra={
                    "county_id": county_id,
                    "tax_year": tax_year,
                    "dataset_type": dataset_type,
                    "import_batch_id": batch.import_batch_id,
                    "rows_rolled_back": rolled_back_count,
                },
            )

    def run_dataset_lifecycle(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        dry_run: bool = False,
    ) -> PipelineRunResult:
        with get_connection() as connection:
            repository = IngestionRepository(connection)
            rerun_of_import_batch_id = repository.find_latest_import_batch_id(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
            )

        fetch_results = self.fetch_sources(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            dry_run=dry_run,
        )
        if len(fetch_results) != 1:
            raise ValueError(
                f"Expected one fetched dataset for {county_id}/{dataset_type}, got {len(fetch_results)}."
            )
        fetch_result = fetch_results[0]
        if fetch_result.status == "skipped_duplicate":
            return PipelineRunResult(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=fetch_result.import_batch_id,
                rerun_of_import_batch_id=rerun_of_import_batch_id,
                fetch_result=fetch_result,
                staging_result=None,
                normalize_result=None,
                skipped_duplicate=True,
                skip_reason=fetch_result.message,
            )

        staging_result = self.load_staging(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            import_batch_id=fetch_result.import_batch_id,
            dry_run=dry_run,
        )
        normalize_result = self.normalize(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            import_batch_id=fetch_result.import_batch_id,
            dry_run=dry_run,
        )
        return PipelineRunResult(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            import_batch_id=fetch_result.import_batch_id,
            rerun_of_import_batch_id=rerun_of_import_batch_id,
            fetch_result=fetch_result,
            staging_result=staging_result,
            normalize_result=normalize_result,
        )

    def inspect_import_batch(
        self,
        *,
        county_id: str,
        tax_year: int,
        dataset_type: str,
        import_batch_id: str | None = None,
        validation_limit: int = 25,
    ) -> ImportBatchInspection:
        with get_connection() as connection:
            repository = IngestionRepository(connection)
            batch = repository.find_import_batch(
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                import_batch_id=import_batch_id,
            )
            if dataset_type == "property_roll":
                summary = repository.inspect_property_roll_import_batch(
                    import_batch_id=batch.import_batch_id,
                    tax_year=tax_year,
                )
            elif dataset_type == "tax_rates":
                summary = repository.inspect_tax_rates_import_batch(
                    import_batch_id=batch.import_batch_id,
                    tax_year=tax_year,
                )
            elif dataset_type == "deeds":
                summary = repository.inspect_deeds_import_batch(
                    import_batch_id=batch.import_batch_id,
                    tax_year=tax_year,
                )
            else:
                raise ValueError(f"Unsupported inspect dataset_type={dataset_type}.")
            failed_records = repository.fetch_validation_failures(
                import_batch_id=batch.import_batch_id,
                limit=validation_limit,
            )
        return ImportBatchInspection(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            import_batch_id=batch.import_batch_id,
            status=summary["status"],
            publish_state=summary["publish_state"],
            publish_version=summary["publish_version"],
            row_count=summary["row_count"],
            error_count=summary["error_count"],
            raw_file_count=summary["raw_file_count"],
            job_run_count=summary["job_run_count"],
            staging_row_count=summary["staging_row_count"],
            lineage_record_count=summary["lineage_record_count"],
            validation_result_count=summary["validation_result_count"],
            validation_error_count=summary["validation_error_count"],
            parcel_year_snapshot_count=summary.get("parcel_year_snapshot_count", 0),
            parcel_assessment_count=summary.get("parcel_assessment_count", 0),
            parcel_exemption_count=summary.get("parcel_exemption_count", 0),
            taxing_unit_count=summary.get("taxing_unit_count", 0),
            tax_rate_count=summary.get("tax_rate_count", 0),
            parcel_tax_assignment_count=summary.get("parcel_tax_assignment_count", 0),
            effective_tax_rate_count=summary.get("effective_tax_rate_count", 0),
            deed_record_count=summary.get("deed_record_count", 0),
            deed_party_count=summary.get("deed_party_count", 0),
            parcel_owner_period_count=summary.get("parcel_owner_period_count", 0),
            current_owner_rollup_count=summary.get("current_owner_rollup_count", 0),
            failed_records=failed_records,
        )

    def _resolve_adapter(self, county_id: str) -> CountyAdapter:
        return self.adapter if self.adapter is not None else get_adapter(county_id)

    def _estimate_record_count(self, acquired: AcquiredDataset) -> int:
        try:
            decoded = acquired.content.decode("utf-8")
        except UnicodeDecodeError:
            return 1
        if not decoded:
            return 0
        try:
            parsed = json.loads(decoded)
        except json.JSONDecodeError:
            return max(decoded.count("\n"), 1)
        if isinstance(parsed, list):
            return len(parsed)
        return 1

    def _lookup_source_system_code(
        self, repository: IngestionRepository, source_system_id: str
    ) -> str:
        with repository.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_system_code
                FROM source_systems
                WHERE source_system_id = %s
                """,
                (source_system_id,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Missing source_systems code for id {source_system_id}.")
        return row["source_system_code"]

    def _media_type_for_file_format(self, file_format: str | None) -> str:
        media_types = {
            "json": "application/json",
            "csv": "text/csv",
        }
        return media_types.get((file_format or "").lower(), "application/octet-stream")

    def _finalize_connection(self, connection: Any, *, dry_run: bool) -> None:
        if dry_run:
            connection.rollback()
            return
        connection.commit()

    def _refresh_tax_assignments(
        self,
        *,
        repository: IngestionRepository,
        county_id: str,
        tax_year: int,
        import_batch_id: str,
        job_run_id: str,
        source_system_id: str,
        force: bool,
    ) -> None:
        if not force and not repository.has_current_tax_rate_records(
            county_id=county_id, tax_year=tax_year
        ):
            return

        parcel_contexts = repository.fetch_parcel_tax_contexts(
            county_id=county_id, tax_year=tax_year
        )
        taxing_unit_contexts = repository.fetch_taxing_unit_contexts(
            county_id=county_id, tax_year=tax_year
        )
        assignments = build_tax_assignments(
            parcels=parcel_contexts, taxing_units=taxing_unit_contexts
        )
        repository.replace_parcel_tax_assignments(
            county_id=county_id,
            tax_year=tax_year,
            import_batch_id=import_batch_id,
            job_run_id=job_run_id,
            source_system_id=source_system_id,
            assignments=assignments,
        )
        repository.refresh_effective_tax_rates(county_id=county_id, tax_year=tax_year)

    def _refresh_owner_reconciliation(
        self,
        *,
        repository: IngestionRepository,
        county_id: str,
        tax_year: int,
        parcel_ids: list[str] | None,
    ) -> None:
        repository.refresh_owner_reconciliation(
            county_id=county_id,
            tax_year=tax_year,
            parcel_ids=parcel_ids,
        )

    def _refresh_search_documents(
        self,
        *,
        repository: IngestionRepository,
        county_id: str,
        tax_year: int,
    ) -> None:
        repository.refresh_search_documents(
            county_id=county_id,
            tax_year=tax_year,
        )
