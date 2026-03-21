from __future__ import annotations

from app.county_adapters.fort_bend.adapter import FortBendCountyAdapter


def test_fort_bend_adapter_lists_property_roll_dataset() -> None:
    adapter = FortBendCountyAdapter()
    datasets = adapter.list_available_datasets("fort_bend", 2026)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}
    assert set(dataset_lookup) == {"property_roll", "tax_rates"}
    assert dataset_lookup["property_roll"].source_system_code == "FBCAD_EXPORT"
    assert dataset_lookup["tax_rates"].source_system_code == "FBCAD_TAX_RATES"
    assert "[fixture_csv]" in dataset_lookup["property_roll"].description


def test_fort_bend_adapter_parse_and_normalize_tax_rate_fixture() -> None:
    adapter = FortBendCountyAdapter()
    acquired = adapter.acquire_dataset("tax_rates", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    assert len(staging_rows) == 8
    assert staging_rows[0].raw_payload["assignment_hints"]["county_ids"] == ["fort_bend"]
    assert staging_rows[7].raw_payload["assignment_hints"]["zip_codes"] == ["77479", "77406"]

    normalized = adapter.normalize_staging_to_canonical(
        "tax_rates",
        [row.raw_payload for row in staging_rows],
    )
    tax_rates = normalized["tax_rates"]
    assert tax_rates[0]["taxing_unit"]["unit_name"] == "Fort Bend County"
    assert tax_rates[4]["taxing_unit"]["unit_code"] == "LCISD"
    assert tax_rates[7]["tax_rate"]["rate_per_100"] == 0.045


def test_fort_bend_adapter_parse_and_normalize_fixture() -> None:
    adapter = FortBendCountyAdapter()
    acquired = adapter.acquire_dataset("property_roll", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    assert len(staging_rows) == 2
    assert staging_rows[0].raw_payload["hs_amt"] == 100000
    assert staging_rows[1].raw_payload["pool_ind"] is True

    normalized = adapter.normalize_staging_to_canonical(
        "property_roll",
        [row.raw_payload for row in staging_rows],
    )
    property_roll = normalized["property_roll"]
    assert len(property_roll) == 2
    assert property_roll[0]["parcel"]["account_number"] == "2002002002001"
    assert property_roll[0]["assessment"]["certified_value"] == 392000
    assert property_roll[0]["exemptions"][0]["raw_exemption_code"] == "hs_amt"
    assert property_roll[1]["exemptions"][1]["exemption_type_code"] == "over65"
    assert property_roll[1]["exemptions"][1]["raw_exemption_code"] == "ov65_amt"


def test_fort_bend_adapter_validation_surfaces_failed_record_details() -> None:
    adapter = FortBendCountyAdapter()
    findings = adapter.validate_dataset(
        "job-test",
        2026,
        "property_roll",
        [
            {
                "site_address": "505 Broken Creek",
                "site_city": "Richmond",
                "site_zip": "77406",
                "market_value": None,
                "exemptions": [{"exemption_type_code": "homestead", "exemption_amount": -10}],
            }
        ],
    )

    error_codes = {finding.validation_code for finding in findings if finding.severity == "error"}
    assert "MISSING_ACCOUNT_ID" in error_codes
    assert "NEGATIVE_EXEMPTION_AMOUNT" in error_codes
    assert "MISSING_MARKET_VALUE" in error_codes
    missing_account = next(
        finding for finding in findings if finding.validation_code == "MISSING_ACCOUNT_ID"
    )
    assert missing_account.details_json["failed_record"]["site_address"] == "505 Broken Creek"


def test_fort_bend_adapter_metadata_reflects_stage5_scope() -> None:
    metadata = FortBendCountyAdapter().get_adapter_metadata()
    assert metadata.county_id == "fort_bend"
    assert metadata.supported_years == [2026]
    assert "fixture-backed" in metadata.known_limitations[0]
