from __future__ import annotations

from app.county_adapters.harris.adapter import HarrisCountyAdapter


def test_harris_adapter_lists_property_roll_dataset() -> None:
    adapter = HarrisCountyAdapter()
    datasets = adapter.list_available_datasets("harris", 2026)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}
    assert set(dataset_lookup) == {"property_roll", "tax_rates"}
    assert dataset_lookup["property_roll"].source_system_code == "HCAD_BULK"
    assert dataset_lookup["tax_rates"].source_system_code == "HCAD_TAX_RATES"
    assert "[fixture_json]" in dataset_lookup["property_roll"].description


def test_harris_adapter_parse_and_normalize_tax_rate_fixture() -> None:
    adapter = HarrisCountyAdapter()
    acquired = adapter.acquire_dataset("tax_rates", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    assert len(staging_rows) == 6

    normalized = adapter.normalize_staging_to_canonical(
        "tax_rates",
        [row.raw_payload for row in staging_rows],
    )
    tax_rates = normalized["tax_rates"]
    assert tax_rates[0]["taxing_unit"]["unit_type_code"] == "county"
    assert tax_rates[2]["taxing_unit"]["unit_name"] == "Houston ISD"
    assert tax_rates[5]["tax_rate"]["rate_value"] == 0.0003


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


def test_harris_adapter_validation_surfaces_failed_record_details() -> None:
    adapter = HarrisCountyAdapter()
    findings = adapter.validate_dataset(
        "job-test",
        2026,
        "property_roll",
        [
            {
                "situs_address": "101 Main St",
                "situs_city": "Houston",
                "situs_zip": "77002",
                "market_value": None,
                "exemptions": [{"exemption_type_code": "homestead", "exemption_amount": -10}],
            }
        ],
    )

    error_codes = {finding.validation_code for finding in findings if finding.severity == "error"}
    assert "MISSING_ACCOUNT_NUMBER" in error_codes
    assert "NEGATIVE_EXEMPTION_AMOUNT" in error_codes
    assert "MISSING_MARKET_VALUE" in error_codes
    missing_account = next(finding for finding in findings if finding.validation_code == "MISSING_ACCOUNT_NUMBER")
    assert missing_account.details_json["failed_record"]["situs_address"] == "101 Main St"


def test_harris_adapter_metadata_reflects_stage4_scope() -> None:
    metadata = HarrisCountyAdapter().get_adapter_metadata()
    assert metadata.county_id == "harris"
    assert metadata.supported_years == [2026]
    assert "fixture-backed" in metadata.known_limitations[0]
