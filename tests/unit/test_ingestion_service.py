from __future__ import annotations

from app.county_adapters.common.base import AcquiredDataset
from app.ingestion.service import IngestionLifecycleService


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
