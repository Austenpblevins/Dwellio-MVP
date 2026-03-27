from __future__ import annotations

from pathlib import Path

import pytest

from app.ingestion.historical_backfill import HistoricalBackfillOrchestrator
from app.ingestion.manual_backfill import ManualImportRegistrationResult
from app.ingestion.service import PipelineStepResult


def test_resolve_ready_file_finds_expected_extension(tmp_path: Path) -> None:
    ready_file = tmp_path / "harris_property_roll_2024.json"
    ready_file.write_text("[]", encoding="utf-8")

    orchestrator = HistoricalBackfillOrchestrator()
    resolved = orchestrator.resolve_ready_file(
        ready_root=str(tmp_path),
        county_id="harris",
        tax_year=2024,
        dataset_type="property_roll",
    )

    assert resolved == ready_file


def test_run_skips_already_published_duplicate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ready_file = tmp_path / "fort_bend_tax_rates_2024.csv"
    ready_file.write_text("unit_code,rate_value\n001,0.02\n", encoding="utf-8")

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
    assert results[0].staging_result is None
    assert results[0].normalize_result is None


def test_run_loads_and_normalizes_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ready_file = tmp_path / "harris_property_roll_2024.json"
    ready_file.write_text("[]", encoding="utf-8")
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
    assert results[0].normalize_result is not None
    assert results[0].normalize_result.publish_version == "harris-2024-property_roll-jobnorm"
