from __future__ import annotations

from app.ingestion.source_registry import (
    get_county_capability_entry,
    get_source_registry_entry,
    list_county_capability_entries,
    list_source_registry_entries,
)


def test_get_harris_source_registry_entry() -> None:
    entry = get_source_registry_entry(county_id="harris", dataset_type="property_roll")
    assert entry.source_system_code == "HCAD_BULK"
    assert entry.source_name == "Harris CAD Property Roll"
    assert entry.access_method == "fixture_json"
    assert entry.active_flag is True
    assert entry.supported_years == [2022, 2023, 2024, 2025, 2026]
    assert entry.resolved_tax_year == 2026
    assert entry.availability_status == "fixture_ready"


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


def test_get_harris_historical_source_registry_entry_resolves_manual_upload() -> None:
    entry = get_source_registry_entry(county_id="harris", dataset_type="property_roll", tax_year=2025)

    assert entry.access_method == "manual_upload"
    assert entry.resolved_tax_year == 2025
    assert entry.availability_status == "manual_upload_required"


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


def test_get_harris_county_capability_entry() -> None:
    entry = get_county_capability_entry(
        county_id="harris",
        capability_code="parcel_level_over65",
    )

    assert entry.label == "Parcel-level over65 signal"
    assert entry.status == "limited"
    assert entry.source_datasets == ["property_roll"]


def test_list_county_capability_entries_includes_fort_bend_over65_support() -> None:
    entries = {
        (entry.county_id, entry.capability_code): entry
        for entry in list_county_capability_entries()
    }

    assert ("harris", "search_refresh_runtime") in entries
    assert ("fort_bend", "parcel_level_over65") in entries
    assert entries[("fort_bend", "parcel_level_over65")].status == "supported"
    assert ("harris", "instant_quote_over65_reliability") in entries
    assert ("fort_bend", "instant_quote_profile_support_level") in entries
    assert entries[("harris", "instant_quote_over65_reliability")].status == "limited"
    assert entries[("fort_bend", "instant_quote_profile_support_level")].status == "summary_only"
