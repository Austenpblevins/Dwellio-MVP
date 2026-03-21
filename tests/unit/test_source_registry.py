from __future__ import annotations

from app.ingestion.source_registry import get_source_registry_entry, list_source_registry_entries


def test_get_harris_source_registry_entry() -> None:
    entry = get_source_registry_entry(county_id="harris", dataset_type="property_roll")
    assert entry.source_system_code == "HCAD_BULK"
    assert entry.source_name == "Harris CAD Property Roll"
    assert entry.access_method == "fixture_json"
    assert entry.active_flag is True
    assert entry.supported_years == [2026]


def test_get_harris_tax_rate_source_registry_entry() -> None:
    entry = get_source_registry_entry(county_id="harris", dataset_type="tax_rates")
    assert entry.source_system_code == "HCAD_TAX_RATES"
    assert entry.file_format == "json"
    assert entry.active_flag is True


def test_get_harris_deed_source_registry_entry() -> None:
    entry = get_source_registry_entry(county_id="harris", dataset_type="deeds")
    assert entry.source_system_code == "DEED_FEED"
    assert entry.file_format == "json"
    assert entry.active_flag is True


def test_list_source_registry_entries_includes_active_fort_bend_dataset() -> None:
    entries = {
        (entry.county_id, entry.dataset_type): entry for entry in list_source_registry_entries()
    }
    assert ("harris", "property_roll") in entries
    assert ("harris", "tax_rates") in entries
    assert ("harris", "deeds") in entries
    assert ("fort_bend", "property_roll") in entries
    assert ("fort_bend", "tax_rates") in entries
    assert ("fort_bend", "deeds") in entries
    fort_bend_entry = entries[("fort_bend", "property_roll")]
    assert fort_bend_entry.active_flag is True
    assert fort_bend_entry.access_method == "fixture_csv"
    assert fort_bend_entry.file_format == "csv"
