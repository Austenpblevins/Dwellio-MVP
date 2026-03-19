from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.county_adapters.common.base import AcquiredDataset, CountyAdapter
from app.db.connection import get_connection
from app.ingestion.archive import read_raw_archive, write_raw_archive
from app.ingestion.registry import get_adapter
from app.ingestion.repository import ImportBatchRecord, IngestionRepository
from app.utils.hashing import sha256_text
from app.utils.logging import get_logger
from app.utils.storage import build_storage_path

logger = get_logger(__name__)


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


@dataclass(frozen=True)
class PipelineRunResult:
    county_id: str
    tax_year: int
    dataset_type: str
    import_batch_id: str
    rerun_of_import_batch_id: str | None
    fetch_result: PipelineStepResult
    staging_result: PipelineStepResult
    normalize_result: PipelineStepResult


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
    parcel_year_snapshot_count: int
    parcel_assessment_count: int
    parcel_exemption_count: int
    failed_records: list[dict[str, Any]]


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
                logger.info(
                    "fetch_sources started",
                    extra={
                        "county_id": county_id,
                        "tax_year": tax_year,
                        "dataset_type": spec.dataset_type,
                        "dry_run": dry_run,
                    },
                )
                acquired = adapter.acquire_dataset(spec.dataset_type, tax_year)
                checksum = sha256_text(acquired.content.decode("utf-8"))
                source_system_id = repository.fetch_source_system_id(acquired.source_system_code)
                import_batch_id = repository.create_import_batch(
                    source_system_id=source_system_id,
                    county_id=county_id,
                    tax_year=tax_year,
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
                source_system_code=self._lookup_source_system_code(repository, batch.source_system_id),
                tax_year=tax_year,
                original_filename=batch.original_filename,
                content=raw_content,
                media_type="application/json",
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
            )
            repository.complete_job_run(
                job_run_id,
                status="succeeded" if error_count == 0 else "failed",
                row_count=len(inserted),
                error_message=None if error_count == 0 else "Validation failed during staging load.",
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
            staged_rows = repository.fetch_staging_rows(
                import_batch_id=batch.import_batch_id,
                dataset_type=dataset_type,
            )
            normalized = adapter.normalize_staging_to_canonical(
                dataset_type,
                [row["raw_payload"] for row in staged_rows],
            )
            if dataset_type != "property_roll":
                raise ValueError(f"Stage 2 normalize currently supports property_roll only, not {dataset_type}.")
            rollback_manifest = repository.capture_property_roll_rollback_manifest(
                county_id=county_id,
                tax_year=tax_year,
                account_numbers=[
                    record["parcel"]["account_number"] for record in normalized["property_roll"]
                ],
            )

            canonical_targets = repository.upsert_property_roll_records(
                county_id=county_id,
                tax_year=tax_year,
                import_batch_id=batch.import_batch_id,
                job_run_id=job_run_id,
                source_system_id=batch.source_system_id,
                normalized_records=normalized["property_roll"],
            )
            repository.insert_lineage_records(
                {
                    "job_run_id": job_run_id,
                    "import_batch_id": batch.import_batch_id,
                    "raw_file_id": batch.raw_file_id,
                    "relation_type": "staging_to_canonical",
                    "source_table": staged_rows[index]["staging_table"],
                    "source_id": staged_rows[index]["staging_row_id"],
                    "target_table": "parcel_year_snapshots",
                    "target_id": target["target_id"],
                    "source_record_hash": staged_rows[index]["row_hash"],
                    "details_json": {"dataset_type": dataset_type, "parcel_id": target["parcel_id"]},
                }
                for index, target in enumerate(canonical_targets)
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
                        "entity_table": "parcel_year_snapshots",
                        "details_json": publish_result.details_json,
                    }
                ],
            )
            repository.update_import_batch(
                batch.import_batch_id,
                status="normalized",
                row_count=len(canonical_targets),
                error_count=0,
                publish_state=publish_result.publish_state,
                publish_version=publish_result.publish_version,
            )
            repository.complete_job_run(
                job_run_id,
                status="succeeded",
                row_count=len(canonical_targets),
                publish_version=publish_result.publish_version,
                metadata_json={
                    "dataset_type": dataset_type,
                    "dry_run": dry_run,
                    "publish_result": publish_result.details_json,
                    "rollback_manifest": rollback_manifest,
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
                    "row_count": len(canonical_targets),
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
                row_count=len(canonical_targets),
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
            rollback_manifest = None if normalize_metadata is None else normalize_metadata.get("rollback_manifest")
            if dataset_type != "property_roll":
                raise ValueError(f"Stage 2 rollback currently supports property_roll only, not {dataset_type}.")
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
            rolled_back_count = repository.rollback_property_roll_records(
                import_batch_id=batch.import_batch_id,
                tax_year=tax_year,
                rollback_manifest=rollback_manifest,
            )
            adapter.rollback_publish(job_run_id)
            repository.insert_validation_results(
                job_run_id=job_run_id,
                import_batch_id=batch.import_batch_id,
                raw_file_id=batch.raw_file_id,
                county_id=county_id,
                tax_year=tax_year,
                findings=[
                    {
                        "validation_code": "ROLLBACK_OK",
                        "message": "Rolled back canonical parcel-year publish state.",
                        "severity": "info",
                        "validation_scope": "publish",
                        "entity_table": "parcel_year_snapshots",
                        "details_json": {"dataset_type": dataset_type, "rows_rolled_back": rolled_back_count},
                    }
                ],
            )
            repository.update_import_batch(
                batch.import_batch_id,
                status="rolled_back",
                publish_state="rolled_back",
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
            raise ValueError(f"Expected one fetched dataset for {county_id}/{dataset_type}, got {len(fetch_results)}.")
        fetch_result = fetch_results[0]

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
            if dataset_type != "property_roll":
                raise ValueError(f"Import inspection currently supports property_roll only, not {dataset_type}.")
            summary = repository.inspect_property_roll_import_batch(
                import_batch_id=batch.import_batch_id,
                tax_year=tax_year,
            )
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
            parcel_year_snapshot_count=summary["parcel_year_snapshot_count"],
            parcel_assessment_count=summary["parcel_assessment_count"],
            parcel_exemption_count=summary["parcel_exemption_count"],
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

    def _lookup_source_system_code(self, repository: IngestionRepository, source_system_id: str) -> str:
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

    def _finalize_connection(self, connection: Any, *, dry_run: bool) -> None:
        if dry_run:
            connection.rollback()
            return
        connection.commit()
