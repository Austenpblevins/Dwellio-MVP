from __future__ import annotations

from app.county_adapters.fort_bend.adapter import FortBendCountyAdapter


def test_fort_bend_adapter_lists_property_roll_dataset() -> None:
    adapter = FortBendCountyAdapter()
    datasets = adapter.list_available_datasets("fort_bend", 2026)
    assert len(datasets) == 1
    assert datasets[0].dataset_type == "property_roll"
    assert datasets[0].source_system_code == "FBCAD_EXPORT"
    assert "[fixture_csv]" in datasets[0].description


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
    assert property_roll[1]["exemptions"][1]["exemption_type_code"] == "over65"


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
    missing_account = next(finding for finding in findings if finding.validation_code == "MISSING_ACCOUNT_ID")
    assert missing_account.details_json["failed_record"]["site_address"] == "505 Broken Creek"


def test_fort_bend_adapter_metadata_reflects_stage5_scope() -> None:
    metadata = FortBendCountyAdapter().get_adapter_metadata()
    assert metadata.county_id == "fort_bend"
    assert metadata.supported_years == [2026]
    assert "fixture-backed" in metadata.known_limitations[0]
