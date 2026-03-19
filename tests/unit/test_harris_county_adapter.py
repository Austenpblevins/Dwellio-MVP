from __future__ import annotations

from app.county_adapters.harris.adapter import HarrisCountyAdapter


def test_harris_adapter_lists_property_roll_dataset() -> None:
    adapter = HarrisCountyAdapter()
    datasets = adapter.list_available_datasets("harris", 2026)
    assert len(datasets) == 1
    assert datasets[0].dataset_type == "property_roll"
    assert datasets[0].source_system_code == "HCAD_BULK"


def test_harris_adapter_parse_and_normalize_fixture() -> None:
    adapter = HarrisCountyAdapter()
    acquired = adapter.acquire_dataset("property_roll", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    assert len(staging_rows) == 2

    normalized = adapter.normalize_staging_to_canonical(
        "property_roll",
        [row.raw_payload for row in staging_rows],
    )
    property_roll = normalized["property_roll"]
    assert len(property_roll) == 2
    assert property_roll[0]["parcel"]["account_number"] == "1001001001001"
    assert property_roll[0]["characteristics"]["homestead_flag"] is True
    assert property_roll[1]["exemptions"][1]["exemption_type_code"] == "over65"
