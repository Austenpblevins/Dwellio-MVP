from __future__ import annotations

from contextlib import contextmanager

from app.county_adapters.common.base import AcquiredDataset
from app.ingestion.service import IngestionLifecycleService, PipelineStepResult


class DummyConnection:
    pass


@contextmanager
def dummy_connection():
    yield DummyConnection()


class StubRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    def find_latest_import_batch_id(self, *, county_id: str, tax_year: int, dataset_type: str) -> str | None:
        return "prior-batch"


class RecordingConnection:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


def test_estimate_record_count_uses_json_list_length() -> None:
    service = IngestionLifecycleService()
    acquired = AcquiredDataset(
        dataset_type="property_roll",
        source_system_code="HCAD_BULK",
        tax_year=2026,
        original_filename="fixture.json",
        content=b'[{"row": 1}, {"row": 2}]',
        media_type="application/json",
    )

    assert service._estimate_record_count(acquired) == 2


def test_estimate_record_count_falls_back_to_line_count() -> None:
    service = IngestionLifecycleService()
    acquired = AcquiredDataset(
        dataset_type="property_roll",
        source_system_code="HCAD_BULK",
        tax_year=2026,
        original_filename="fixture.txt",
        content=b"row-1\nrow-2\nrow-3",
        media_type="text/plain",
    )

    assert service._estimate_record_count(acquired) == 2


def test_finalize_connection_commits_for_non_dry_run() -> None:
    service = IngestionLifecycleService()
    connection = RecordingConnection()

    service._finalize_connection(connection, dry_run=False)

    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0


def test_finalize_connection_rolls_back_for_dry_run() -> None:
    service = IngestionLifecycleService()
    connection = RecordingConnection()

    service._finalize_connection(connection, dry_run=True)

    assert connection.commit_calls == 0
    assert connection.rollback_calls == 1


def test_run_dataset_lifecycle_chains_fetch_staging_and_normalize(monkeypatch) -> None:
    service = IngestionLifecycleService()

    monkeypatch.setattr("app.ingestion.service.get_connection", dummy_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr(
        service,
        "fetch_sources",
        lambda **kwargs: [
            PipelineStepResult(
                county_id="harris",
                tax_year=2026,
                dataset_type="property_roll",
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                job_run_id="job-fetch",
                row_count=2,
            )
        ],
    )
    monkeypatch.setattr(
        service,
        "load_staging",
        lambda **kwargs: PipelineStepResult(
            county_id="harris",
            tax_year=2026,
            dataset_type="property_roll",
            import_batch_id="batch-1",
            raw_file_id="raw-1",
            job_run_id="job-stage",
            row_count=2,
        ),
    )
    monkeypatch.setattr(
        service,
        "normalize",
        lambda **kwargs: PipelineStepResult(
            county_id="harris",
            tax_year=2026,
            dataset_type="property_roll",
            import_batch_id="batch-1",
            raw_file_id="raw-1",
            job_run_id="job-normalize",
            row_count=2,
            publish_version="harris-2026-property_roll-abcd1234",
        ),
    )

    result = service.run_dataset_lifecycle(
        county_id="harris",
        tax_year=2026,
        dataset_type="property_roll",
    )

    assert result.import_batch_id == "batch-1"
    assert result.rerun_of_import_batch_id == "prior-batch"
    assert result.normalize_result.publish_version == "harris-2026-property_roll-abcd1234"
