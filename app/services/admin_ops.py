from __future__ import annotations

from typing import Any

from app.county_adapters.common.config_loader import load_county_adapter_config
from app.db.connection import get_connection
from app.ingestion.manual_backfill import register_manual_import
from app.ingestion.service import IngestionLifecycleService
from app.models.admin import (
    AdminCompletenessIssue,
    AdminCompletenessIssuesResponse,
    AdminImportBatchActionRequest,
    AdminImportBatchActions,
    AdminImportBatchDetail,
    AdminImportBatchInspection,
    AdminImportBatchListResponse,
    AdminImportBatchSummary,
    AdminJobRunSummary,
    AdminManualImportRequest,
    AdminMutationResult,
    AdminSourceFileRecord,
    AdminSourceFilesResponse,
    AdminTaxAssignmentIssue,
    AdminTaxAssignmentIssuesResponse,
    AdminValidationFinding,
    AdminValidationResultsResponse,
)


class AdminOpsService:
    def list_import_batches(
        self,
        *,
        county_id: str,
        tax_year: int | None = None,
        dataset_type: str | None = None,
        limit: int = 50,
    ) -> AdminImportBatchListResponse:
        with get_connection() as connection:
            rows = self._fetch_import_batch_rows(
                connection,
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                limit=limit,
            )
        return AdminImportBatchListResponse(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            batches=[self._build_import_batch_summary(row) for row in rows],
        )

    def get_import_batch_detail(self, *, import_batch_id: str) -> AdminImportBatchDetail:
        with get_connection() as connection:
            batch_row = self._fetch_import_batch_detail_row(connection, import_batch_id=import_batch_id)
            if batch_row is None:
                raise ValueError(f"Missing import batch {import_batch_id}.")
            source_files = self._fetch_source_file_rows(connection, import_batch_id=import_batch_id)
            job_runs = self._fetch_job_run_rows(connection, import_batch_id=import_batch_id)
            validation_summary = self._build_validation_summary(
                connection,
                import_batch_id=import_batch_id,
                limit=50,
            )

        inspection = IngestionLifecycleService().inspect_import_batch(
            county_id=batch_row["county_id"],
            tax_year=batch_row["tax_year"],
            dataset_type=batch_row["dataset_type"],
            import_batch_id=import_batch_id,
            validation_limit=25,
        )
        config = load_county_adapter_config(batch_row["county_id"])
        dataset_config = config.dataset_configs.get(batch_row["dataset_type"])

        summary = self._build_import_batch_summary(batch_row)
        return AdminImportBatchDetail(
            batch=summary,
            inspection=AdminImportBatchInspection(
                status=inspection.status,
                publish_state=inspection.publish_state,
                publish_version=inspection.publish_version,
                row_count=inspection.row_count,
                error_count=inspection.error_count,
                raw_file_count=inspection.raw_file_count,
                job_run_count=inspection.job_run_count,
                staging_row_count=inspection.staging_row_count,
                lineage_record_count=inspection.lineage_record_count,
                validation_result_count=inspection.validation_result_count,
                validation_error_count=inspection.validation_error_count,
                parcel_year_snapshot_count=inspection.parcel_year_snapshot_count,
                parcel_assessment_count=inspection.parcel_assessment_count,
                parcel_exemption_count=inspection.parcel_exemption_count,
                taxing_unit_count=inspection.taxing_unit_count,
                tax_rate_count=inspection.tax_rate_count,
                parcel_tax_assignment_count=inspection.parcel_tax_assignment_count,
                effective_tax_rate_count=inspection.effective_tax_rate_count,
                deed_record_count=inspection.deed_record_count,
                deed_party_count=inspection.deed_party_count,
                parcel_owner_period_count=inspection.parcel_owner_period_count,
                current_owner_rollup_count=inspection.current_owner_rollup_count,
            ),
            validation_summary=validation_summary,
            source_files=[self._build_source_file(row) for row in source_files],
            job_runs=[self._build_job_run(row) for row in job_runs],
            actions=AdminImportBatchActions(
                can_publish=summary.status in {"staged", "rolled_back"},
                can_rollback=summary.publish_state == "published",
                manual_fallback_supported=bool(
                    dataset_config.manual_fallback_supported if dataset_config is not None else True
                ),
                manual_fallback_message=(
                    None
                    if dataset_config is None
                    else f"Dataset access method: {dataset_config.access_method}."
                ),
            ),
        )

    def list_validation_results(
        self,
        *,
        import_batch_id: str,
        severity: str | None = None,
        limit: int = 100,
    ) -> AdminValidationResultsResponse:
        with get_connection() as connection:
            return self._build_validation_summary(
                connection,
                import_batch_id=import_batch_id,
                severity=severity,
                limit=limit,
            )

    def list_source_files(
        self,
        *,
        county_id: str,
        tax_year: int | None = None,
        dataset_type: str | None = None,
        limit: int = 100,
    ) -> AdminSourceFilesResponse:
        with get_connection() as connection:
            rows = self._fetch_source_file_rows(
                connection,
                county_id=county_id,
                tax_year=tax_year,
                dataset_type=dataset_type,
                limit=limit,
            )
        return AdminSourceFilesResponse(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            source_files=[self._build_source_file(row) for row in rows],
        )

    def list_completeness_issues(
        self,
        *,
        county_id: str,
        tax_year: int,
        limit: int = 100,
    ) -> AdminCompletenessIssuesResponse:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      county_id,
                      parcel_id,
                      tax_year,
                      account_number,
                      completeness_score,
                      public_summary_ready_flag,
                      admin_review_required,
                      warning_codes
                    FROM parcel_data_completeness_view
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND (admin_review_required OR completeness_score < 100)
                    ORDER BY completeness_score ASC, account_number ASC
                    LIMIT %s
                    """,
                    (county_id, tax_year, limit),
                )
                rows = cursor.fetchall()
        return AdminCompletenessIssuesResponse(
            county_id=county_id,
            tax_year=tax_year,
            issues=[
                AdminCompletenessIssue(
                    county_id=row["county_id"],
                    parcel_id=str(row["parcel_id"]),
                    tax_year=row["tax_year"],
                    account_number=row["account_number"],
                    completeness_score=float(row["completeness_score"]),
                    public_summary_ready_flag=bool(row["public_summary_ready_flag"]),
                    admin_review_required=bool(row["admin_review_required"]),
                    warning_codes=list(row["warning_codes"] or []),
                )
                for row in rows
            ],
        )

    def list_tax_assignment_issues(
        self,
        *,
        county_id: str,
        tax_year: int,
        limit: int = 100,
    ) -> AdminTaxAssignmentIssuesResponse:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      county_id,
                      parcel_id,
                      tax_year,
                      account_number,
                      county_assignment_count,
                      city_assignment_count,
                      school_assignment_count,
                      mud_assignment_count,
                      special_assignment_count,
                      missing_county_assignment,
                      missing_city_assignment,
                      missing_school_assignment,
                      missing_mud_assignment,
                      conflicting_county_assignment,
                      conflicting_city_assignment,
                      conflicting_school_assignment,
                      conflicting_mud_assignment
                    FROM v_parcel_tax_assignment_qa
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND (
                        missing_county_assignment
                        OR missing_city_assignment
                        OR missing_school_assignment
                        OR missing_mud_assignment
                        OR conflicting_county_assignment
                        OR conflicting_city_assignment
                        OR conflicting_school_assignment
                        OR conflicting_mud_assignment
                      )
                    ORDER BY account_number ASC
                    LIMIT %s
                    """,
                    (county_id, tax_year, limit),
                )
                rows = cursor.fetchall()
        return AdminTaxAssignmentIssuesResponse(
            county_id=county_id,
            tax_year=tax_year,
            issues=[
                AdminTaxAssignmentIssue(
                    county_id=row["county_id"],
                    parcel_id=str(row["parcel_id"]),
                    tax_year=row["tax_year"],
                    account_number=row["account_number"],
                    county_assignment_count=row["county_assignment_count"],
                    city_assignment_count=row["city_assignment_count"],
                    school_assignment_count=row["school_assignment_count"],
                    mud_assignment_count=row["mud_assignment_count"],
                    special_assignment_count=row["special_assignment_count"],
                    missing_county_assignment=bool(row["missing_county_assignment"]),
                    missing_city_assignment=bool(row["missing_city_assignment"]),
                    missing_school_assignment=bool(row["missing_school_assignment"]),
                    missing_mud_assignment=bool(row["missing_mud_assignment"]),
                    conflicting_county_assignment=bool(row["conflicting_county_assignment"]),
                    conflicting_city_assignment=bool(row["conflicting_city_assignment"]),
                    conflicting_school_assignment=bool(row["conflicting_school_assignment"]),
                    conflicting_mud_assignment=bool(row["conflicting_mud_assignment"]),
                )
                for row in rows
            ],
        )

    def register_manual_import(
        self,
        *,
        request: AdminManualImportRequest,
    ) -> AdminMutationResult:
        result = register_manual_import(
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            source_file_path=request.source_file_path,
            source_url=request.source_url,
            dry_run=request.dry_run,
        )
        return AdminMutationResult(
            action="manual_import_register",
            county_id=result.county_id,
            tax_year=result.tax_year,
            dataset_type=result.dataset_type,
            import_batch_id=result.import_batch_id,
            message=(
                "Found matching manual import in dry-run mode."
                if request.dry_run and result.skipped_duplicate
                else "Found matching manual import and reused the existing import batch."
                if result.skipped_duplicate
                else "Registered manual import in dry-run mode."
                if request.dry_run
                else "Registered manual import and raw file metadata."
            ),
        )

    def publish_import_batch(
        self,
        *,
        import_batch_id: str,
        request: AdminImportBatchActionRequest,
    ) -> AdminMutationResult:
        result = IngestionLifecycleService().normalize(
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id=import_batch_id,
            dry_run=False,
        )
        return AdminMutationResult(
            action="publish_import_batch",
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id=import_batch_id,
            job_run_id=result.job_run_id,
            publish_version=result.publish_version,
            message="Re-ran normalize and publish flow for the selected import batch.",
        )

    def rollback_import_batch(
        self,
        *,
        import_batch_id: str,
        request: AdminImportBatchActionRequest,
    ) -> AdminMutationResult:
        IngestionLifecycleService().rollback_publish(
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id=import_batch_id,
        )
        return AdminMutationResult(
            action="rollback_import_batch",
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id=import_batch_id,
            message="Rolled back the published canonical state for the selected import batch.",
        )

    def _fetch_import_batch_rows(
        self,
        connection: Any,
        *,
        county_id: str,
        tax_year: int | None = None,
        dataset_type: str | None = None,
        limit: int,
    ) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ib.import_batch_id,
                  ib.county_id,
                  ib.tax_year,
                  COALESCE(ib.dataset_type, MAX(rf.file_kind), 'unknown') AS dataset_type,
                  COALESCE(MAX(ss.source_system_code), '') AS source_system_code,
                  ib.source_filename,
                  ib.file_format,
                  ib.status,
                  ib.status_reason,
                  ib.publish_state,
                  ib.publish_version,
                  ib.row_count,
                  ib.error_count,
                  COUNT(DISTINCT rf.raw_file_id) AS raw_file_count,
                  COUNT(DISTINCT vr.validation_result_id) AS validation_result_count,
                  COUNT(DISTINCT vr.validation_result_id) FILTER (WHERE vr.severity = 'error') AS validation_error_count,
                  latest_job.job_name AS latest_job_name,
                  latest_job.job_stage AS latest_job_stage,
                  latest_job.status AS latest_job_status,
                  latest_job.started_at AS latest_job_started_at,
                  latest_job.finished_at AS latest_job_finished_at,
                  latest_job.error_message AS latest_job_error_message,
                  ib.created_at
                FROM import_batches ib
                LEFT JOIN raw_files rf
                  ON rf.import_batch_id = ib.import_batch_id
                LEFT JOIN source_systems ss
                  ON ss.source_system_id = ib.source_system_id
                LEFT JOIN validation_results vr
                  ON vr.import_batch_id = ib.import_batch_id
                LEFT JOIN LATERAL (
                  SELECT
                    jr.job_name,
                    jr.job_stage,
                    jr.status,
                    jr.started_at,
                    jr.finished_at,
                    jr.error_message
                  FROM job_runs jr
                  WHERE jr.import_batch_id = ib.import_batch_id
                  ORDER BY jr.started_at DESC, jr.job_run_id DESC
                  LIMIT 1
                ) latest_job ON true
                WHERE ib.county_id = %s
                  AND (%s IS NULL OR ib.tax_year = %s)
                  AND (
                    %s IS NULL
                    OR ib.dataset_type = %s
                    OR EXISTS (
                      SELECT 1
                      FROM raw_files rf_filter
                      WHERE rf_filter.import_batch_id = ib.import_batch_id
                        AND rf_filter.file_kind = %s
                    )
                  )
                GROUP BY
                  ib.import_batch_id,
                  ib.county_id,
                  ib.tax_year,
                  ib.dataset_type,
                  ib.source_filename,
                  ib.file_format,
                  ib.status,
                  ib.status_reason,
                  ib.publish_state,
                  ib.publish_version,
                  ib.row_count,
                  ib.error_count,
                  ib.created_at,
                  latest_job.job_name,
                  latest_job.job_stage,
                  latest_job.status,
                  latest_job.started_at,
                  latest_job.finished_at,
                  latest_job.error_message
                ORDER BY ib.created_at DESC, ib.import_batch_id DESC
                LIMIT %s
                """,
                (county_id, tax_year, tax_year, dataset_type, dataset_type, dataset_type, limit),
            )
            return cursor.fetchall()

    def _fetch_import_batch_detail_row(
        self,
        connection: Any,
        *,
        import_batch_id: str,
    ) -> dict[str, Any] | None:
        rows = self._fetch_import_batch_rows(
            connection,
            county_id=self._fetch_county_id_for_batch(connection, import_batch_id=import_batch_id),
            tax_year=None,
            dataset_type=None,
            limit=200,
        )
        for row in rows:
            if str(row["import_batch_id"]) == import_batch_id:
                return row
        return None

    def _fetch_county_id_for_batch(self, connection: Any, *, import_batch_id: str) -> str:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT county_id FROM import_batches WHERE import_batch_id = %s",
                (import_batch_id,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Missing import batch {import_batch_id}.")
        return row["county_id"]

    def _fetch_source_file_rows(
        self,
        connection: Any,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        dataset_type: str | None = None,
        import_batch_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  rf.raw_file_id,
                  rf.import_batch_id,
                  rf.county_id,
                  rf.tax_year,
                  rf.file_kind AS dataset_type,
                  ss.source_system_code,
                  rf.original_filename,
                  rf.storage_path,
                  rf.checksum,
                  rf.mime_type,
                  rf.size_bytes,
                  rf.file_format,
                  rf.source_url,
                  rf.created_at
                FROM raw_files rf
                JOIN source_systems ss
                  ON ss.source_system_id = rf.source_system_id
                WHERE (%s IS NULL OR rf.county_id = %s)
                  AND (%s IS NULL OR rf.tax_year = %s)
                  AND (%s IS NULL OR rf.file_kind = %s)
                  AND (%s IS NULL OR rf.import_batch_id = %s)
                ORDER BY rf.created_at DESC, rf.raw_file_id DESC
                LIMIT %s
                """,
                (
                    county_id,
                    county_id,
                    tax_year,
                    tax_year,
                    dataset_type,
                    dataset_type,
                    import_batch_id,
                    import_batch_id,
                    limit,
                ),
            )
            return cursor.fetchall()

    def _fetch_job_run_rows(self, connection: Any, *, import_batch_id: str) -> list[dict[str, Any]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  job_run_id,
                  job_name,
                  job_stage,
                  status,
                  started_at,
                  finished_at,
                  row_count,
                  error_message,
                  metadata_json
                FROM job_runs
                WHERE import_batch_id = %s
                ORDER BY started_at DESC, job_run_id DESC
                LIMIT 50
                """,
                (import_batch_id,),
            )
            return cursor.fetchall()

    def _build_validation_summary(
        self,
        connection: Any,
        *,
        import_batch_id: str,
        severity: str | None = None,
        limit: int,
    ) -> AdminValidationResultsResponse:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  COUNT(*) AS total_count,
                  COUNT(*) FILTER (WHERE severity = 'error') AS error_count,
                  COUNT(*) FILTER (WHERE severity = 'warning') AS warning_count,
                  COUNT(*) FILTER (WHERE severity = 'info') AS info_count
                FROM validation_results
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            counts = cursor.fetchone()
            cursor.execute(
                """
                SELECT
                  validation_result_id,
                  validation_code,
                  message,
                  severity,
                  validation_scope,
                  entity_table,
                  details_json,
                  created_at
                FROM validation_results
                WHERE import_batch_id = %s
                  AND (%s IS NULL OR severity = %s)
                ORDER BY created_at DESC, validation_result_id DESC
                LIMIT %s
                """,
                (import_batch_id, severity, severity, limit),
            )
            rows = cursor.fetchall()
        return AdminValidationResultsResponse(
            import_batch_id=import_batch_id,
            total_count=int(counts["total_count"] or 0),
            error_count=int(counts["error_count"] or 0),
            warning_count=int(counts["warning_count"] or 0),
            info_count=int(counts["info_count"] or 0),
            findings=[self._build_validation_finding(row) for row in rows],
        )

    def _build_import_batch_summary(self, row: dict[str, Any]) -> AdminImportBatchSummary:
        return AdminImportBatchSummary(
            import_batch_id=str(row["import_batch_id"]),
            county_id=row["county_id"],
            tax_year=row["tax_year"],
            dataset_type=row["dataset_type"],
            source_system_code=row["source_system_code"],
            source_filename=row["source_filename"],
            file_format=row.get("file_format"),
            status=row["status"],
            status_reason=row.get("status_reason"),
            publish_state=row.get("publish_state"),
            publish_version=row.get("publish_version"),
            row_count=row.get("row_count"),
            error_count=row.get("error_count"),
            raw_file_count=int(row["raw_file_count"] or 0),
            validation_result_count=int(row["validation_result_count"] or 0),
            validation_error_count=int(row["validation_error_count"] or 0),
            latest_job_name=row.get("latest_job_name"),
            latest_job_stage=row.get("latest_job_stage"),
            latest_job_status=row.get("latest_job_status"),
            latest_job_started_at=row.get("latest_job_started_at"),
            latest_job_finished_at=row.get("latest_job_finished_at"),
            latest_job_error_message=row.get("latest_job_error_message"),
            created_at=row.get("created_at"),
        )

    def _build_source_file(self, row: dict[str, Any]) -> AdminSourceFileRecord:
        return AdminSourceFileRecord(
            raw_file_id=str(row["raw_file_id"]),
            import_batch_id=str(row["import_batch_id"]),
            county_id=row["county_id"],
            tax_year=row["tax_year"],
            dataset_type=row["dataset_type"],
            source_system_code=row["source_system_code"],
            original_filename=row["original_filename"],
            storage_path=row["storage_path"],
            checksum=row["checksum"],
            mime_type=row.get("mime_type"),
            size_bytes=row["size_bytes"],
            file_format=row.get("file_format"),
            source_url=row.get("source_url"),
            created_at=row.get("created_at"),
        )

    def _build_job_run(self, row: dict[str, Any]) -> AdminJobRunSummary:
        return AdminJobRunSummary(
            job_run_id=str(row["job_run_id"]),
            job_name=row["job_name"],
            job_stage=row["job_stage"],
            status=row["status"],
            started_at=row.get("started_at"),
            finished_at=row.get("finished_at"),
            row_count=row.get("row_count"),
            error_message=row.get("error_message"),
            metadata_json=dict(row.get("metadata_json") or {}),
        )

    def _build_validation_finding(self, row: dict[str, Any]) -> AdminValidationFinding:
        return AdminValidationFinding(
            validation_result_id=str(row["validation_result_id"]),
            validation_code=row["validation_code"],
            message=row["message"],
            severity=row["severity"],
            validation_scope=row["validation_scope"],
            entity_table=row.get("entity_table"),
            details_json=dict(row.get("details_json") or {}),
            created_at=row.get("created_at"),
        )
