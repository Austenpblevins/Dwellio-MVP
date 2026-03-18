from __future__ import annotations

from app.county_adapters.common.base import CountyAdapter
from app.county_adapters.fort_bend.adapter import FortBendCountyAdapter
from app.county_adapters.harris.adapter import HarrisCountyAdapter


def get_adapter(county_id: str) -> CountyAdapter:
    registry: dict[str, CountyAdapter] = {
        "harris": HarrisCountyAdapter(),
        "fort_bend": FortBendCountyAdapter(),
    }
    try:
        return registry[county_id]
    except KeyError as exc:
        raise ValueError(f"Unsupported county adapter: {county_id}") from exc
