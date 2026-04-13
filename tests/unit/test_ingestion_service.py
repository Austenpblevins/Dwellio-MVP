from __future__ import annotations

from contextlib import contextmanager

import pytest

from app.county_adapters.common.base import AcquiredDataset, PublishResult, StagingRow
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

    def find_latest_import_batch_id(
        self, *, county_id: str, tax_year: int, dataset_type: str
    ) -> str | None:
        return "prior-batch"


class RecordingConnection:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self) -> SavepointCursor:
        return SavepointCursor()

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


@contextmanager
def recording_connection():
    yield RecordingConnection()


class SavepointCursor:
    def __enter__(self) -> SavepointCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params=None) -> None:
        return None


@contextmanager
def savepoint_connection():
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
                    raw_payload={
                        "account_id": "2002002002001",
                        "market_value": 400000,
                        "exemptions": [],
                    },
                    row_hash="hash-1",
                )
            ]

        def validate_dataset(self, *args, **kwargs) -> list[object]:
            return []

    adapter = CapturingAdapter()
    service.adapter = adapter  # type: ignore[assignment]

    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr(
        "app.ingestion.service.read_raw_archive",
        lambda storage_path: b"account_id\n2002002002001\n",
    )
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


def test_normalize_supports_deeds_and_refreshes_owner_reconciliation(monkeypatch) -> None:
    service = IngestionLifecycleService()
    refresh_calls: list[dict[str, object]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="harris/2026/deeds/example.json",
                original_filename="harris-deeds-2026.json",
                file_kind="deeds",
                mime_type="application/json",
                file_format="json",
            )

        def create_job_run(self, **kwargs) -> str:
            return "job-normalize"

        def count_validation_errors(self, **kwargs) -> int:
            return 0

        def fetch_staging_rows(self, **kwargs) -> list[dict[str, object]]:
            return [
                {
                    "staging_table": "stg_sales_raw",
                    "staging_row_id": "stage-1",
                    "raw_payload": {"instrument_number": "INST-1"},
                    "row_hash": "hash-1",
                }
            ]

        def capture_deed_rollback_manifest(self, **kwargs) -> dict[str, object]:
            return {
                "dataset_type": "deeds",
                "deed_entries": [{"instrument_number": "INST-1", "prior_state": None}],
                "parcel_entries": [],
            }

        def upsert_deed_records(self, **kwargs) -> list[dict[str, str]]:
            return [
                {"target_table": "deed_records", "target_id": "deed-1", "parcel_id": "parcel-1"}
            ]

        def insert_lineage_records(self, records) -> None:
            list(records)

        def insert_validation_results(self, **kwargs) -> None:
            return None

        def update_import_batch(self, *args, **kwargs) -> None:
            return None

        def complete_job_run(self, *args, **kwargs) -> None:
            return None

    class StubAdapter:
        def normalize_staging_to_canonical(
            self, dataset_type: str, staging_rows: list[dict[str, object]]
        ):
            assert dataset_type == "deeds"
            assert staging_rows == [{"instrument_number": "INST-1"}]
            return {
                "deeds": [
                    {
                        "deed_record": {"instrument_number": "INST-1", "metadata_json": {}},
                        "deed_parties": [],
                        "linked_account_number": "1001001001001",
                        "linked_cad_property_id": "HCAD-1001",
                        "linked_alias_values": [],
                        "source_record_hash": "hash-1",
                    }
                ]
            }

        def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
            return PublishResult(
                publish_version=f"harris-{tax_year}-{dataset_type}-{job_id[:8]}",
                details_json={"dataset_type": dataset_type},
            )

    service.adapter = StubAdapter()  # type: ignore[assignment]
    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr(service, "_refresh_tax_assignments", lambda **kwargs: None)
    monkeypatch.setattr(
        service, "_refresh_owner_reconciliation", lambda **kwargs: refresh_calls.append(kwargs)
    )
    monkeypatch.setattr(service, "_refresh_search_documents", lambda **kwargs: None)

    result = service.normalize(
        county_id="harris",
        tax_year=2026,
        dataset_type="deeds",
    )

    assert result.row_count == 1
    assert result.publish_version == "harris-2026-deeds-job-norm"
    assert refresh_calls[0]["parcel_ids"] == ["parcel-1"]


def test_normalize_blocks_publish_when_validation_failed(monkeypatch) -> None:
    service = IngestionLifecycleService()
    updates: list[dict[str, object]] = []
    completed_runs: list[dict[str, object]] = []
    inserted_findings: list[dict[str, object]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="harris/2025/property_roll/example.json",
                original_filename="harris-property_roll-2025.json",
                file_kind="property_roll",
                mime_type="application/json",
                file_format="json",
            )

        def count_validation_errors(self, **kwargs) -> int:
            return 2

        def create_job_run(self, **kwargs) -> str:
            return "job-normalize"

        def insert_validation_results(self, **kwargs) -> None:
            inserted_findings.extend(kwargs["findings"])

        def update_import_batch(self, *args, **kwargs) -> None:
            updates.append(kwargs)

        def complete_job_run(self, *args, **kwargs) -> None:
            completed_runs.append(kwargs)

    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)

    try:
        service.normalize(
            county_id="harris",
            tax_year=2025,
            dataset_type="property_roll",
        )
    except RuntimeError as exc:
        assert "Publish blocked because 2 validation error finding(s) exist" in str(exc)
    else:
        raise AssertionError("Expected normalize() to block publish when validation errors exist.")

    assert updates[0]["status"] == "publish_blocked"
    assert updates[0]["publish_state"] == "blocked_validation"
    assert "Publish blocked because 2 validation error finding(s) exist" in str(
        updates[0]["status_reason"]
    )
    assert completed_runs[0]["status"] == "failed"
    assert inserted_findings[0]["validation_code"] == "PUBLISH_BLOCKED_VALIDATION_FAILED"


def test_normalize_blocks_publish_when_publish_controls_fail(monkeypatch) -> None:
    service = IngestionLifecycleService()
    updates: list[dict[str, object]] = []
    completed_runs: list[dict[str, object]] = []
    inserted_findings: list[dict[str, object]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="harris/2025/property_roll/example.json",
                original_filename="harris-property_roll-2025.json",
                file_kind="property_roll",
                mime_type="application/json",
                file_format="json",
            )

        def count_validation_errors(self, **kwargs) -> int:
            return 0

        def create_job_run(self, **kwargs) -> str:
            return "job-normalize"

        def count_staging_rows(self, **kwargs) -> int:
            return 1

        def count_property_roll_rows_for_import_batch(self, **kwargs) -> int:
            return 0

        def iterate_staging_rows(self, **kwargs):
            yield [
                {
                    "staging_table": "stg_county_property_raw",
                    "staging_row_id": "stage-1",
                    "raw_payload": {"account_number": "1001001001001"},
                    "row_hash": "hash-1",
                }
            ]

        def capture_property_roll_rollback_manifest(self, **kwargs) -> dict[str, object]:
            return {"dataset_type": "property_roll", "entries": []}

        def insert_validation_results(self, **kwargs) -> None:
            inserted_findings.extend(kwargs["findings"])

        def update_import_batch(self, *args, **kwargs) -> None:
            updates.append(kwargs)

        def complete_job_run(self, *args, **kwargs) -> None:
            completed_runs.append(kwargs)

    class StubAdapter:
        def normalize_staging_to_canonical(self, dataset_type: str, staging_rows):
            assert dataset_type == "property_roll"
            return {
                "property_roll": [
                    {
                        "parcel": {
                            "account_number": "1001001001001",
                            "situs_address": "123 Main St",
                            "situs_city": "Houston",
                            "situs_zip": "77001",
                            "owner_name": "Jane Doe",
                            "source_record_hash": "hash-1",
                        },
                        "address": {
                            "situs_address": "123 Main St",
                            "situs_city": "Houston",
                            "situs_zip": "77001",
                            "normalized_address": "123 MAIN ST",
                        },
                        "characteristics": {},
                        "improvements": [],
                        "land_segments": [],
                        "value_components": [],
                        "assessment": {"market_value": 300000},
                        "exemptions": [],
                    }
                ]
            }

    monkeypatch.setattr("app.ingestion.service.get_connection", savepoint_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    service.adapter = StubAdapter()  # type: ignore[assignment]

    with pytest.raises(RuntimeError) as exc_info:
        service.normalize(
            county_id="harris",
            tax_year=2025,
            dataset_type="property_roll",
        )

    assert "publish-control error finding(s)" in str(exc_info.value)
    assert updates[0]["status"] == "publish_blocked"
    assert updates[0]["publish_state"] == "blocked_publish_controls"
    assert completed_runs[0]["status"] == "failed"
    assert inserted_findings[0]["validation_code"] == "ROLLBACK_MANIFEST_MISSING_ACCOUNT"


def test_normalize_tax_rates_allows_unit_only_harris_rows(monkeypatch) -> None:
    service = IngestionLifecycleService()
    upsert_calls: list[dict[str, object]] = []
    tax_refresh_calls: list[dict[str, object]] = []
    updates: list[dict[str, object]] = []
    completed_runs: list[dict[str, object]] = []
    inserted_findings: list[dict[str, object]] = []
    lineage_records: list[dict[str, object]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="harris/2026/tax_rates/example.json",
                original_filename="harris-tax_rates-2026.json",
                file_kind="tax_rates",
                mime_type="application/json",
                file_format="json",
            )

        def count_validation_errors(self, **kwargs) -> int:
            return 0

        def create_job_run(self, **kwargs) -> str:
            return "job-normalize"

        def count_staging_rows(self, **kwargs) -> int:
            return 2

        def fetch_staging_rows(self, **kwargs):
            return [
                {
                    "staging_table": "stg_county_tax_rate_raw",
                    "staging_row_id": "stage-1",
                    "raw_payload": {"unit_code": "A31"},
                    "row_hash": "hash-1",
                },
                {
                    "staging_table": "stg_county_tax_rate_raw",
                    "staging_row_id": "stage-2",
                    "raw_payload": {"unit_code": "A76"},
                    "row_hash": "hash-2",
                },
            ]

        def capture_tax_rate_rollback_manifest(self, **kwargs) -> dict[str, object]:
            return {
                "dataset_type": "tax_rates",
                "entries": [
                    {"unit_code": "A31", "prior_state": None},
                    {"unit_code": "A76", "prior_state": None},
                ],
            }

        def upsert_tax_rate_records(self, **kwargs):
            upsert_calls.append(kwargs)
            return [
                {
                    "target_table": "tax_rates",
                    "target_id": "rate-a31",
                    "taxing_unit_id": "tu-a31",
                },
                {
                    "target_table": "taxing_units",
                    "target_id": "tu-a76",
                    "taxing_unit_id": "tu-a76",
                },
            ]

        def insert_lineage_records(self, records) -> None:
            lineage_records.extend(records)

        def insert_validation_results(self, **kwargs) -> None:
            inserted_findings.extend(kwargs["findings"])

        def update_import_batch(self, *args, **kwargs) -> None:
            updates.append(kwargs)

        def complete_job_run(self, *args, **kwargs) -> None:
            completed_runs.append(kwargs)

    class StubAdapter:
        def normalize_staging_to_canonical(self, dataset_type: str, staging_rows):
            assert dataset_type == "tax_rates"
            assert staging_rows == [{"unit_code": "A31"}, {"unit_code": "A76"}]
            return {
                "tax_rates": [
                    {
                        "taxing_unit": {
                            "unit_type_code": "mud",
                            "unit_code": "A31",
                            "unit_name": "NEWPORT MUD DA 2",
                            "metadata_json": {
                                "rate_bearing_status": "rate_bearing",
                                "assignment_hints": {
                                    "account_numbers": ["0451420000005"],
                                    "source": "real_acct_jurs_special_family_bridge",
                                },
                            },
                        },
                        "tax_rate": {
                            "rate_component": "ad_valorem",
                            "rate_value": 0.007422,
                            "rate_per_100": 0.7422,
                            "is_current": True,
                        },
                    },
                    {
                        "taxing_unit": {
                            "unit_type_code": "mud",
                            "unit_code": "A76",
                            "unit_name": "HC MUD 568",
                            "metadata_json": {
                                "rate_bearing_status": "caveated_rate_row_deferred",
                                "assignment_eligible_without_rate": True,
                                "normalization_caveat_codes": ["contradictory_basis_year_activity"],
                            },
                        },
                        "tax_rate": None,
                    },
                ]
            }

        def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
            return PublishResult(
                publish_version=f"harris-{tax_year}-{dataset_type}-{job_id[:8]}",
                details_json={"dataset_type": dataset_type},
            )

    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr(service, "_build_publish_control_findings", lambda **kwargs: [])
    monkeypatch.setattr(
        service,
        "_refresh_tax_assignments",
        lambda **kwargs: tax_refresh_calls.append(kwargs),
    )
    service.adapter = StubAdapter()  # type: ignore[assignment]

    result = service.normalize(
        county_id="harris",
        tax_year=2026,
        dataset_type="tax_rates",
    )

    assert result.row_count == 2
    assert upsert_calls
    normalized_records = upsert_calls[0]["normalized_records"]
    assert normalized_records[0]["tax_rate"]["rate_value"] == 0.007422
    assert normalized_records[1]["tax_rate"] is None
    assert normalized_records[1]["taxing_unit"]["metadata_json"]["assignment_eligible_without_rate"] is True
    assert lineage_records[1]["target_table"] == "taxing_units"
    assert tax_refresh_calls[0]["force"] is True
    assert updates[-1]["status"] == "normalized"
    assert completed_runs[-1]["status"] == "succeeded"
    assert inserted_findings[-1]["validation_code"] == "PUBLISH_OK"


def test_normalize_bulk_property_roll_reruns_when_improvement_summaries_are_missing(
    monkeypatch,
) -> None:
    service = IngestionLifecycleService()
    upsert_calls: list[dict[str, object]] = []
    tax_refresh_calls: list[dict[str, object]] = []
    search_refresh_calls: list[dict[str, object]] = []
    updates: list[dict[str, object]] = []
    completed_runs: list[dict[str, object]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="harris/2025/property_roll/example.json",
                original_filename="harris-property_roll-2025.json",
                file_kind="property_roll",
                mime_type="application/json",
                file_format="json",
            )

        def count_validation_errors(self, **kwargs) -> int:
            return 0

        def create_job_run(self, **kwargs) -> str:
            return "job-normalize"

        def count_staging_rows(self, **kwargs) -> int:
            return 50_001

        def count_property_roll_rows_for_import_batch(self, **kwargs) -> int:
            return 1

        def count_property_roll_improvement_rows_for_import_batch(self, **kwargs) -> int:
            return 0

        def iterate_staging_rows(self, **kwargs):
            yield [
                {
                    "staging_table": "stg_county_property_raw",
                    "staging_row_id": "stage-1",
                    "raw_payload": {"account_number": "1001001001001"},
                    "row_hash": "hash-1",
                }
            ]

        def capture_property_roll_rollback_manifest(self, **kwargs) -> dict[str, object]:
            return {
                "dataset_type": "property_roll",
                "entries": [{"account_number": "1001001001001", "prior_state": None}],
            }

        def upsert_property_roll_records(self, **kwargs):
            upsert_calls.append(kwargs)
            return [{"target_table": "parcel_year_snapshots", "target_id": "snap-1", "parcel_id": "parcel-1"}]

        def insert_validation_results(self, **kwargs) -> None:
            return None

        def update_import_batch(self, *args, **kwargs) -> None:
            updates.append(kwargs)

        def complete_job_run(self, *args, **kwargs) -> None:
            completed_runs.append(kwargs)

    class StubAdapter:
        def normalize_staging_to_canonical(self, dataset_type: str, staging_rows):
            assert dataset_type == "property_roll"
            assert staging_rows == [{"account_number": "1001001001001"}]
            return {
                "property_roll": [
                    {
                        "parcel": {
                            "account_number": "1001001001001",
                            "situs_address": "123 Main St",
                            "situs_city": "Houston",
                            "situs_zip": "77001",
                            "owner_name": "Jane Doe",
                            "source_record_hash": "hash-1",
                        },
                        "address": {
                            "situs_address": "123 Main St",
                            "situs_city": "Houston",
                            "situs_zip": "77001",
                            "normalized_address": "123 MAIN ST",
                        },
                        "characteristics": {"homestead_flag": False},
                        "improvements": [{"living_area_sf": 2150, "year_built": 2004}],
                        "land_segments": [{"land_sf": 5000}],
                        "value_components": [],
                        "assessment": {"market_value": 300000},
                        "exemptions": [],
                    }
                ]
            }

        def publish_dataset(self, job_id: str, tax_year: int, dataset_type: str) -> PublishResult:
            return PublishResult(
                publish_version=f"harris-{tax_year}-{dataset_type}-{job_id[:8]}",
                details_json={"dataset_type": dataset_type},
            )

    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr(service, "_build_publish_control_findings", lambda **kwargs: [])
    monkeypatch.setattr(
        service,
        "_refresh_tax_assignments",
        lambda **kwargs: tax_refresh_calls.append(kwargs),
    )
    monkeypatch.setattr(service, "_refresh_owner_reconciliation", lambda **kwargs: None)
    monkeypatch.setattr(
        service, "_refresh_search_documents", lambda **kwargs: search_refresh_calls.append(kwargs)
    )
    service.adapter = StubAdapter()  # type: ignore[assignment]

    result = service.normalize(
        county_id="harris",
        tax_year=2025,
        dataset_type="property_roll",
    )

    assert result.row_count == 1
    assert upsert_calls[0]["include_detail_tables"] is False
    assert tax_refresh_calls
    assert tax_refresh_calls[0]["import_batch_id"] == "batch-1"
    assert search_refresh_calls
    assert updates[-1]["status"] == "normalized"
    assert completed_runs[-1]["status"] == "succeeded"
    assert completed_runs[-1]["metadata_json"]["rollback_manifest"] == {
        "dataset_type": "property_roll",
        "entries": [{"account_number": "1001001001001", "prior_state": None}],
    }
    assert completed_runs[-1]["metadata_json"]["rollback_manifest_summary"] == {
        "dataset_type": "property_roll",
        "storage_mode": "summary_only_bulk_property_roll",
        "entry_count": 1,
        "sample_account_numbers": ["1001001001001"],
    }
    assert completed_runs[-1]["metadata_json"]["post_commit_tax_assignment_refresh"] is True
    assert completed_runs[-1]["metadata_json"]["post_commit_search_refresh"] is True


def test_build_bulk_property_roll_manifest_metadata_returns_small_summary() -> None:
    service = IngestionLifecycleService()

    summary = service._build_bulk_property_roll_manifest_metadata(
        {
            "dataset_type": "property_roll",
            "entries": [
                {"account_number": "1"},
                {"account_number": "2"},
                {"account_number": "3"},
                {"account_number": "4"},
                {"account_number": "5"},
                {"account_number": "6"},
            ],
        }
    )

    assert summary == {
        "dataset_type": "property_roll",
        "storage_mode": "summary_only_bulk_property_roll",
        "entry_count": 6,
        "sample_account_numbers": ["1", "2", "3", "4", "5"],
    }


def test_rollback_property_roll_refreshes_search_documents(monkeypatch) -> None:
    service = IngestionLifecycleService()
    search_refresh_calls: list[dict[str, object]] = []
    rollback_calls: list[dict[str, object]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def find_import_batch(self, **kwargs) -> ImportBatchRecord:
            return ImportBatchRecord(
                import_batch_id="batch-1",
                raw_file_id="raw-1",
                source_system_id="source-1",
                storage_path="harris/2025/property_roll/example.json",
                original_filename="harris-property_roll-2025.json",
                file_kind="property_roll",
                mime_type="application/json",
                file_format="json",
            )

        def fetch_job_run_metadata(self, **kwargs) -> dict[str, object]:
            return {
                "rollback_manifest": {
                    "dataset_type": "property_roll",
                    "entries": [{"account_number": "1001001001001", "prior_state": None}],
                },
                "rollback_manifest_summary": {
                    "dataset_type": "property_roll",
                    "storage_mode": "summary_only_bulk_property_roll",
                    "entry_count": 1,
                    "sample_account_numbers": ["1001001001001"],
                },
            }

        def create_job_run(self, **kwargs) -> str:
            return "job-rollback"

        def rollback_property_roll_records(self, **kwargs) -> int:
            rollback_calls.append(kwargs)
            return 2

        def insert_validation_results(self, **kwargs) -> None:
            return None

        def update_import_batch(self, *args, **kwargs) -> None:
            return None

        def complete_job_run(self, *args, **kwargs) -> None:
            return None

    class StubAdapter:
        def rollback_publish(self, job_id: str) -> None:
            return None

    service.adapter = StubAdapter()  # type: ignore[assignment]
    monkeypatch.setattr("app.ingestion.service.get_connection", recording_connection)
    monkeypatch.setattr("app.ingestion.service.IngestionRepository", StubRepository)
    monkeypatch.setattr(service, "_refresh_tax_assignments", lambda **kwargs: None)
    monkeypatch.setattr(service, "_refresh_owner_reconciliation", lambda **kwargs: None)
    monkeypatch.setattr(
        service,
        "_refresh_search_documents",
        lambda **kwargs: search_refresh_calls.append(kwargs),
    )

    service.rollback_publish(
        county_id="harris",
        tax_year=2025,
        dataset_type="property_roll",
    )

    assert len(search_refresh_calls) == 1
    assert search_refresh_calls[0]["county_id"] == "harris"
    assert search_refresh_calls[0]["tax_year"] == 2025
    assert rollback_calls == [
        {
            "import_batch_id": "batch-1",
            "tax_year": 2025,
            "rollback_manifest": {
                "dataset_type": "property_roll",
                "entries": [{"account_number": "1001001001001", "prior_state": None}],
            },
        }
    ]


def test_refresh_tax_assignments_prefers_set_based_repository_path() -> None:
    service = IngestionLifecycleService()

    class StubRepository:
        def __init__(self) -> None:
            self.called = False
            self.refreshed = False

        def has_current_tax_rate_records(self, **kwargs) -> bool:
            return True

        def refresh_parcel_tax_assignments_set_based(self, **kwargs) -> int:
            self.called = True
            return 5

        def refresh_effective_tax_rates(self, **kwargs) -> int:
            self.refreshed = True
            return 5

    repository = StubRepository()

    service._refresh_tax_assignments(
        repository=repository,  # type: ignore[arg-type]
        county_id="harris",
        tax_year=2025,
        import_batch_id="batch-1",
        job_run_id="job-1",
        source_system_id="source-1",
        force=False,
    )

    assert repository.called is True
    assert repository.refreshed is True
