from __future__ import annotations

from app.county_adapters.fort_bend.adapter import FortBendCountyAdapter


def fetch(*, dataset_type: str = "property_roll", tax_year: int = 2026) -> list[dict]:
    _ = FortBendCountyAdapter().list_available_datasets("fort_bend", tax_year)
    raise NotImplementedError(f"Fort Bend fetch for {dataset_type} is deferred to a later stage.")
