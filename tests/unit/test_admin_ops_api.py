from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.models.admin import (
    AdminImportBatchListResponse,
    AdminImportBatchSummary,
    AdminManualImportRequest,
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
