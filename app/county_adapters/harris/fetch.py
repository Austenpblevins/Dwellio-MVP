from __future__ import annotations

from app.county_adapters.harris.adapter import HarrisCountyAdapter


def fetch(*, dataset_type: str = "property_roll", tax_year: int = 2026) -> list[dict]:
    acquired = HarrisCountyAdapter().acquire_dataset(dataset_type, tax_year)
    return [{"original_filename": acquired.original_filename, "bytes": len(acquired.content)}]
