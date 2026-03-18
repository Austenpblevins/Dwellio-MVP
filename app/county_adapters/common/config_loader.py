from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config" / "counties"


@dataclass(frozen=True)
class CountyAdapterConfig:
    county_id: str
    appraisal_district: str
    timezone: str
    datasets: list[str]
    parser_module: str
    notes: str
    metadata: dict[str, Any]


def load_county_adapter_config(county_id: str) -> CountyAdapterConfig:
    path = CONFIG_DIR / f"{county_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Missing county config file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    return CountyAdapterConfig(
        county_id=raw.get("county_id", county_id),
        appraisal_district=raw.get("appraisal_district", ""),
        timezone=raw.get("timezone", "America/Chicago"),
        datasets=list(raw.get("datasets", [])),
        parser_module=raw.get("parser_module", ""),
        notes=raw.get("notes", ""),
        metadata=dict(raw.get("metadata", {})),
    )

