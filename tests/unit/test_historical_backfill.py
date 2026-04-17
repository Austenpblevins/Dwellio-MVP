from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from app.ingestion.historical_backfill import HistoricalBackfillOrchestrator
from app.ingestion.manual_backfill import ManualImportRegistrationResult
from app.ingestion.service import PipelineStepResult


def _write_ready_manifest(
    tmp_path: Path,
    *,
    county_id: str,
    tax_year: int,
    dataset_type: str,
    ready_file: Path,
    validation_status: str = "passed",
) -> Path:
    manifest_path = tmp_path / f"{county_id}_{dataset_type}_{tax_year}.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "county_id": county_id,
                "tax_year": tax_year,
                "dataset_type": dataset_type,
                "raw_files": [{"logical_name": dataset_type, "path": "/tmp/source", "size_bytes": 1, "checksum_sha256": "raw"}],
                "output_files": [
                    {
                        "path": str(ready_file),
                        "size_bytes": ready_file.stat().st_size,
                        "checksum_sha256": hashlib.sha256(ready_file.read_bytes()).hexdigest(),
                        "row_count": 1,
                    }
                ],
                "row_count": 1,
                "validation": {
                    "status": validation_status,
                    "parse_issue_count": 0,
                    "validation_error_count": 0,
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return manifest_path


def test_resolve_ready_dataset_uses_manifest_and_checksum(tmp_path: Path) -> None:
    ready_file = tmp_path / "harris_property_roll_2024.json"
    ready_file.write_text("[]", encoding="utf-8")
    manifest_path = _write_ready_manifest(
        tmp_path,
        county_id="harris",
        tax_year=2024,
        dataset_type="property_roll",
        ready_file=ready_file,
    )

    orchestrator = HistoricalBackfillOrchestrator()
    resolved = orchestrator.resolve_ready_dataset(
        ready_root=str(tmp_path),
        county_id="harris",
        tax_year=2024,
        dataset_type="property_roll",
    )

    assert resolved.source_file_path == str(ready_file)
    assert resolved.manifest_path == str(manifest_path)


def test_resolve_ready_dataset_requires_validation_passed_manifest(tmp_path: Path) -> None:
    ready_file = tmp_path / "harris_property_roll_2024.json"
    ready_file.write_text("[]", encoding="utf-8")
    _write_ready_manifest(
        tmp_path,
        county_id="harris",
        tax_year=2024,
        dataset_type="property_roll",
        ready_file=ready_file,
        validation_status="failed",
    )

    orchestrator = HistoricalBackfillOrchestrator()
    with pytest.raises(ValueError, match="not validation-passed"):
        orchestrator.resolve_ready_dataset(
            ready_root=str(tmp_path),
            county_id="harris",
            tax_year=2024,
            dataset_type="property_roll",
        )


def test_run_skips_already_published_duplicate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ready_file = tmp_path / "fort_bend_tax_rates_2024.csv"
    ready_file.write_text("unit_code,rate_value\n001,0.02\n", encoding="utf-8")
    manifest_path = _write_ready_manifest(
        tmp_path,
        county_id="fort_bend",
        tax_year=2024,
        dataset_type="tax_rates",
        ready_file=ready_file,
    )

    monkeypatch.setattr(
        "app.ingestion.historical_backfill.register_manual_import",
        lambda **kwargs: ManualImportRegistrationResult(
            county_id="fort_bend",
            tax_year=2024,
            dataset_type="tax_rates",
            import_batch_id="batch-existing",
            raw_file_id="raw-existing",
            storage_path="fort_bend/2024/tax_rates/example.csv",
            source_system_code="FBCAD_EXPORT",
            file_format="csv",
            source_filename="fort_bend_tax_rates_2024.csv",
            checksum="checksum",
            skipped_duplicate=True,
            existing_status="normalized",
            existing_publish_state="published",
        ),
    )

    class StubService:
        def load_staging(self, **kwargs):
            raise AssertionError("load_staging should not run for an already published duplicate.")

        def normalize(self, **kwargs):
            raise AssertionError("normalize should not run for an already published duplicate.")

    orchestrator = HistoricalBackfillOrchestrator(service=StubService())  # type: ignore[arg-type]
    results = orchestrator.run(
        counties=["fort_bend"],
        tax_years=[2024],
        dataset_types=["tax_rates"],
        ready_root=str(tmp_path),
    )

    assert len(results) == 1
    assert results[0].skipped_duplicate is True
    assert results[0].manifest_path == str(manifest_path)
    assert results[0].staging_result is None
    assert results[0].normalize_result is None


def test_run_loads_and_normalizes_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ready_file = tmp_path / "harris_property_roll_2024.json"
    ready_file.write_text("[]", encoding="utf-8")
    manifest_path = _write_ready_manifest(
        tmp_path,
        county_id="harris",
        tax_year=2024,
        dataset_type="property_roll",
        ready_file=ready_file,
    )
    calls: list[tuple[str, str, int, str]] = []

    monkeypatch.setattr(
        "app.ingestion.historical_backfill.register_manual_import",
        lambda **kwargs: ManualImportRegistrationResult(
            county_id="harris",
            tax_year=2024,
            dataset_type="property_roll",
            import_batch_id="batch-1",
            raw_file_id="raw-1",
            storage_path="harris/2024/property_roll/example.json",
            source_system_code="HCAD_BULK",
            file_format="json",
            source_filename="harris_property_roll_2024.json",
            checksum="checksum",
        ),
    )

    class StubService:
        def load_staging(self, **kwargs) -> PipelineStepResult:
            calls.append(("load_staging", kwargs["county_id"], kwargs["tax_year"], kwargs["dataset_type"]))
            return PipelineStepResult(
                county_id="harris",
                tax_year=2024,
                dataset_type="property_roll",
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                job_run_id="job-stage",
                row_count=3,
            )

        def normalize(self, **kwargs) -> PipelineStepResult:
            calls.append(("normalize", kwargs["county_id"], kwargs["tax_year"], kwargs["dataset_type"]))
            return PipelineStepResult(
                county_id="harris",
                tax_year=2024,
                dataset_type="property_roll",
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                job_run_id="job-normalize",
                row_count=3,
                publish_version="harris-2024-property_roll-jobnorm",
            )

    orchestrator = HistoricalBackfillOrchestrator(service=StubService())  # type: ignore[arg-type]
    results = orchestrator.run(
        counties=["harris"],
        tax_years=[2024],
        dataset_types=["property_roll"],
        ready_root=str(tmp_path),
    )

    assert calls == [
        ("load_staging", "harris", 2024, "property_roll"),
        ("normalize", "harris", 2024, "property_roll"),
    ]
    assert results[0].manifest_path == str(manifest_path)
    assert results[0].normalize_result is not None
    assert results[0].normalize_result.publish_version == "harris-2024-property_roll-jobnorm"
