from __future__ import annotations

from app.county_adapters.harris.adapter import HarrisCountyAdapter


def normalize(parsed_records: list[dict]) -> dict[str, object]:
    return HarrisCountyAdapter().normalize_staging_to_canonical("property_roll", parsed_records)
