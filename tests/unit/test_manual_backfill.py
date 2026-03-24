from __future__ import annotations

from pathlib import Path

import pytest

from app.ingestion.manual_backfill import register_manual_import


def test_register_manual_import_registers_batch_and_raw_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_file = tmp_path / "harris-2025-property-roll.json"
    source_file.write_text('[{"account_number": "1001001001001"}]', encoding="utf-8")

    writes: list[tuple[str, bytes]] = []

    class StubRepository:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        def fetch_source_system_id(self, source_system_code: str) -> str:
            assert source_system_code == "HCAD_BULK"
            return "source-id"

        def create_import_batch(self, **kwargs) -> str:
            assert kwargs["county_id"] == "harris"
            assert kwargs["tax_year"] == 2025
            assert kwargs["source_filename"] == source_file.name
            assert kwargs["file_format"] == "json"
            return "batch-1"

        def register_raw_file(self, **kwargs) -> str:
            assert kwargs["import_batch_id"] == "batch-1"
            assert kwargs["file_kind"] == "property_roll"
            assert kwargs["file_format"] == "json"
            return "raw-1"

        def update_import_batch(self, import_batch_id: str, **kwargs) -> None:
            assert import_batch_id == "batch-1"
            assert kwargs["status"] == "fetched"

    class StubConnection:
        def __enter__(self) -> StubConnection:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.manual_backfill.get_connection", lambda: StubConnection())
    monkeypatch.setattr("app.ingestion.manual_backfill.IngestionRepository", StubRepository)
    monkeypatch.setattr(
        "app.ingestion.manual_backfill.write_raw_archive",
        lambda storage_path, content: writes.append((storage_path, content)),
    )

    result = register_manual_import(
        county_id="harris",
        tax_year=2025,
        dataset_type="property_roll",
        source_file_path=str(source_file),
    )

    assert result.import_batch_id == "batch-1"
    assert result.raw_file_id == "raw-1"
    assert result.source_system_code == "HCAD_BULK"
    assert writes and writes[0][0].startswith("harris/2025/property_roll/")


def test_register_manual_import_rejects_unsupported_file_extension(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_file = tmp_path / "fort-bend-2025-property-roll.xlsx"
    source_file.write_bytes(b"not-a-supported-format")

    with pytest.raises(ValueError) as exc_info:
        register_manual_import(
            county_id="fort_bend",
            tax_year=2025,
            dataset_type="property_roll",
            source_file_path=str(source_file),
            dry_run=True,
        )

    assert "Unsupported file format" in str(exc_info.value)
