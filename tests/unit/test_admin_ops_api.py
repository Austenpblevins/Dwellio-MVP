from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.models.admin import (
    AdminImportBatchActions,
    AdminImportBatchDetail,
    AdminImportBatchInspection,
    AdminIngestionStepRun,
    AdminIngestionStepSummary,
    AdminImportBatchListResponse,
    AdminImportBatchSummary,
    AdminManualImportRequest,
    AdminValidationResultsResponse,
    AdminMutationResult,
)


def test_admin_ops_requires_token() -> None:
    client = TestClient(app)

    response = client.get("/admin/ops/import-batches", params={"county_id": "harris"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Admin token required."


def test_admin_ops_rejects_invalid_token() -> None:
    client = TestClient(app)

    response = client.get(
        "/admin/ops/import-batches",
        params={"county_id": "harris"},
        headers={"x-dwellio-admin-token": "wrong-token"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Invalid admin token."


def test_admin_ops_import_batches_route_returns_internal_payload(monkeypatch) -> None:
    def stub_get_import_batches(
        county_id: str,
        *,
        tax_year: int | None = None,
        dataset_type: str | None = None,
        limit: int = 50,
    ) -> AdminImportBatchListResponse:
        assert county_id == "harris"
        assert tax_year == 2025
        assert dataset_type == "property_roll"
        assert limit == 20
        return AdminImportBatchListResponse(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            batches=[
                AdminImportBatchSummary(
                    import_batch_id="batch-1",
                    county_id=county_id,
                    tax_year=2025,
                    dataset_type="property_roll",
                    source_system_code="HCAD_BULK",
                    source_filename="harris_2025_property_roll.csv",
                    file_format="csv",
                    status="normalized",
                status_reason="published_to_canonical: property_roll publish succeeded.",
                publish_state="published",
                publish_version="v1",
                row_count=123,
                error_count=0,
                validation_warning_count=2,
                publish_control_warning_count=1,
                latest_job_duration_ms=18000,
                maintenance_status="failed",
                maintenance_failed_step_name="search_refresh",
                maintenance_latest_step_name="search_refresh",
                maintenance_latest_duration_ms=42000,
                maintenance_attempt_count=2,
                maintenance_retry_count=1,
                raw_file_count=1,
                validation_result_count=3,
                validation_error_count=0,
            )
            ],
        )

    monkeypatch.setattr("app.api.routes.admin.get_import_batches", stub_get_import_batches)

    client = TestClient(app)
    response = client.get(
        "/admin/ops/import-batches",
        params={"county_id": "harris", "tax_year": 2025, "dataset_type": "property_roll", "limit": 20},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_scope"] == "internal"
    assert payload["batches"][0]["dataset_type"] == "property_roll"
    assert payload["batches"][0]["publish_state"] == "published"
    assert payload["batches"][0]["status_reason"] == "published_to_canonical: property_roll publish succeeded."
    assert payload["batches"][0]["maintenance_status"] == "failed"
    assert payload["batches"][0]["validation_warning_count"] == 2
    assert payload["batches"][0]["publish_control_warning_count"] == 1
    assert payload["batches"][0]["latest_job_duration_ms"] == 18000
    assert payload["batches"][0]["maintenance_latest_step_name"] == "search_refresh"
    assert payload["batches"][0]["maintenance_latest_duration_ms"] == 42000
    assert payload["batches"][0]["maintenance_attempt_count"] == 2
    assert payload["batches"][0]["maintenance_retry_count"] == 1


def test_admin_manual_register_route_uses_existing_backfill_path(monkeypatch) -> None:
    def stub_manual_import(request: AdminManualImportRequest) -> AdminMutationResult:
        assert request.county_id == "harris"
        assert request.tax_year == 2025
        assert request.dataset_type == "property_roll"
        assert request.source_file_path == "/tmp/harris_2025_property_roll.csv"
        assert request.source_url == "https://example.test/harris_2025_property_roll.csv"
        assert request.dry_run is True
        return AdminMutationResult(
            action="manual_import_register",
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id="batch-2",
            message="Registered manual import in dry-run mode.",
        )

    monkeypatch.setattr("app.api.routes.admin.post_manual_import_register", stub_manual_import)

    client = TestClient(app)
    response = client.post(
        "/admin/ops/manual-import/register",
        json={
            "county_id": "harris",
            "tax_year": 2025,
            "dataset_type": "property_roll",
            "source_file_path": "/tmp/harris_2025_property_roll.csv",
            "source_url": "https://example.test/harris_2025_property_roll.csv",
            "dry_run": True,
        },
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "manual_import_register"
    assert payload["import_batch_id"] == "batch-2"


def test_admin_import_batch_detail_route_returns_step_runs(monkeypatch) -> None:
    def stub_get_import_batch_detail(import_batch_id: str) -> AdminImportBatchDetail:
        assert import_batch_id == "batch-1"
        return AdminImportBatchDetail(
            batch=AdminImportBatchSummary(
                import_batch_id="batch-1",
                county_id="harris",
                tax_year=2025,
                dataset_type="property_roll",
                source_system_code="HCAD_BULK",
                status="normalized",
                publish_state="published",
                maintenance_status="failed",
                maintenance_failed_step_name="search_refresh",
                raw_file_count=1,
                validation_result_count=1,
                validation_error_count=0,
            ),
            inspection=AdminImportBatchInspection(
                status="normalized",
                publish_state="published",
                raw_file_count=1,
                job_run_count=1,
                staging_row_count=1,
                lineage_record_count=1,
                validation_result_count=1,
                validation_error_count=0,
            ),
            validation_summary=AdminValidationResultsResponse(
                import_batch_id="batch-1",
                total_count=1,
                error_count=0,
                warning_count=0,
                info_count=1,
                findings=[],
            ),
            source_files=[],
            job_runs=[],
            step_runs=[
                AdminIngestionStepRun(
                    step_run_id="step-1",
                    step_name="search_refresh",
                    status="failed",
                    attempt_number=2,
                    duration_ms=42000,
                    is_retry=True,
                    error_message="search refresh failed",
                )
            ],
            step_summary=[
                AdminIngestionStepSummary(
                    step_name="search_refresh",
                    latest_status="failed",
                    latest_attempt_number=2,
                    attempt_count=2,
                    retry_count=1,
                    failed_attempt_count=1,
                    latest_duration_ms=42000,
                    last_error_message="search refresh failed",
                )
            ],
            actions=AdminImportBatchActions(
                can_publish=False,
                can_rollback=True,
                can_retry_maintenance=True,
                manual_fallback_supported=True,
            ),
        )

    monkeypatch.setattr("app.api.routes.admin.get_import_batch_detail", stub_get_import_batch_detail)

    client = TestClient(app)
    response = client.get(
        "/admin/ops/import-batches/batch-1",
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["step_runs"][0]["step_name"] == "search_refresh"
    assert payload["step_runs"][0]["status"] == "failed"
    assert payload["step_runs"][0]["duration_ms"] == 42000
    assert payload["step_runs"][0]["is_retry"] is True
    assert payload["step_summary"][0]["step_name"] == "search_refresh"
    assert payload["step_summary"][0]["attempt_count"] == 2
    assert payload["actions"]["can_retry_maintenance"] is True


def test_admin_publish_route_returns_mutation_result(monkeypatch) -> None:
    def stub_publish(import_batch_id: str, request) -> AdminMutationResult:
        assert import_batch_id == "batch-3"
        assert request.county_id == "fort_bend"
        assert request.tax_year == 2025
        assert request.dataset_type == "tax_rates"
        return AdminMutationResult(
            action="publish_import_batch",
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id=import_batch_id,
            job_run_id="job-1",
            publish_version="publish-v2",
            message="Re-ran normalize and publish flow for the selected import batch.",
        )

    monkeypatch.setattr("app.api.routes.admin.post_publish_import_batch", stub_publish)

    client = TestClient(app)
    response = client.post(
        "/admin/ops/import-batches/batch-3/publish",
        json={"county_id": "fort_bend", "tax_year": 2025, "dataset_type": "tax_rates"},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "publish_import_batch"
    assert payload["job_run_id"] == "job-1"


def test_admin_retry_maintenance_route_returns_mutation_result(monkeypatch) -> None:
    def stub_retry(import_batch_id: str, request) -> AdminMutationResult:
        assert import_batch_id == "batch-4"
        assert request.county_id == "harris"
        assert request.tax_year == 2026
        assert request.dataset_type == "property_roll"
        return AdminMutationResult(
            action="retry_post_commit_maintenance",
            county_id=request.county_id,
            tax_year=request.tax_year,
            dataset_type=request.dataset_type,
            import_batch_id=import_batch_id,
            job_run_id="job-2",
            publish_version="publish-v3",
            message="Retried post-commit maintenance for the selected import batch.",
        )

    monkeypatch.setattr("app.api.routes.admin.post_retry_import_batch_maintenance", stub_retry)

    client = TestClient(app)
    response = client.post(
        "/admin/ops/import-batches/batch-4/retry-maintenance",
        json={"county_id": "harris", "tax_year": 2026, "dataset_type": "property_roll"},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "retry_post_commit_maintenance"
    assert payload["job_run_id"] == "job-2"
