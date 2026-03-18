from __future__ import annotations

from app.county_adapters.harris.adapter import HarrisCountyAdapter, build_harris_fixture_rows


def parse(raw_records: list[dict] | None = None) -> list[dict]:
    records = raw_records or build_harris_fixture_rows()
    adapter = HarrisCountyAdapter()
    acquired = adapter.acquire_dataset("property_roll", 2026)
    if raw_records is not None:
        return records
    return [row.raw_payload for row in adapter.parse_raw_to_staging(acquired)]
