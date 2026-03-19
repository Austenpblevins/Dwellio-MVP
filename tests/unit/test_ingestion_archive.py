from __future__ import annotations

from pathlib import Path

from app.core.config import get_settings
from app.ingestion.archive import read_raw_archive, write_raw_archive


def test_raw_archive_round_trip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("DWELLIO_RAW_ARCHIVE_ROOT", str(tmp_path))
    get_settings.cache_clear()

    write_raw_archive("harris/2026/property_roll/test.json", b'{"ok": true}')
    assert read_raw_archive("harris/2026/property_roll/test.json") == b'{"ok": true}'

    get_settings.cache_clear()
