from __future__ import annotations

from contextlib import contextmanager

from app.county_adapters.common.base import AcquiredDataset, StagingRow
from app.ingestion.repository import ImportBatchRecord
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


@contextmanager
def recording_connection():
    yield RecordingConnection()


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


def test_load_staging_preserves_archived_media_type(monkeypatch) -> None:
    service = IngestionLifecycleService()

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="fort_bend/2026/property_roll/example.csv",
                original_filename="fort_bend-property_roll-2026.csv",
                file_kind="property_roll",
                mime_type="text/csv",
                file_format="csv",
            )

        def create_job_run(self, **kwargs) -> str:
            return "job-stage"

        def insert_staging_rows(self, **kwargs) -> list[dict[str, str]]:
            return [
                {
                    "staging_table": "stg_county_property_raw",
                    "staging_row_id": "stage-1",
                    "row_hash": "hash-1",
                }
            ]

        def insert_validation_results(self, **kwargs) -> None:
            return None

        def insert_lineage_records(self, records) -> None:
            list(records)

        def update_import_batch(self, *args, **kwargs) -> None:
            return None

        def complete_job_run(self, *args, **kwargs) -> None:
            return None

    class CapturingAdapter:
        def __init__(self) -> None:
            self.media_type: str | None = None

        def parse_raw_to_staging(self, file: AcquiredDataset) -> list[StagingRow]:
            self.media_type = file.media_type
            return [
                StagingRow(
                    table_name="stg_county_property_raw",
                    raw_payload={"account_id": "2002002002001", "market_value": 400000, "exemptions": []},
                    row_hash="hash-1",
                )
            ]

        def validate_dataset(self, *args, **kwargs) -> list[object]:
            return []

    adapter = CapturingAdapter()
    service.adapter = adapter  # type: ignore[assignment]

    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr("app.ingestion.service.read_raw_archive", lambda storage_path: b"account_id\n2002002002001\n")
    monkeypatch.setattr(
        service,
        "_lookup_source_system_code",
        lambda repository, source_system_id: "FBCAD_EXPORT",
    )

    result = service.load_staging(
        county_id="fort_bend",
        tax_year=2026,
        dataset_type="property_roll",
    )

    assert result.import_batch_id == "batch-1"
    assert adapter.media_type == "text/csv"


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
