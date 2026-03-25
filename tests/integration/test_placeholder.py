from __future__ import annotations

from pathlib import Path


def test_stage15_fixture_inventory_exists() -> None:
    root = Path(__file__).resolve().parents[1] / "fixtures"

    assert (root / "README.md").exists()
    assert (root / "stage15_workflow_samples.json").exists()
    assert (root / "gis" / "spatial_assignment.geojson").exists()
