from __future__ import annotations

import json

from app.county_adapters.harris.adapter import HarrisCountyAdapter
from app.county_adapters.harris.normalize import normalize_tax_rates


def test_harris_adapter_lists_property_roll_dataset() -> None:
    adapter = HarrisCountyAdapter()
    datasets = adapter.list_available_datasets("harris", 2026)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}
    assert set(dataset_lookup) == {"property_roll", "tax_rates", "deeds"}
    assert dataset_lookup["property_roll"].source_system_code == "HCAD_BULK"
    assert dataset_lookup["tax_rates"].source_system_code == "HCAD_TAX_RATES"
    assert dataset_lookup["deeds"].source_system_code == "DEED_FEED"
    assert "[fixture_json]" in dataset_lookup["property_roll"].description


def test_harris_adapter_lists_historical_dataset_as_manual_upload() -> None:
    adapter = HarrisCountyAdapter()
    datasets = adapter.list_available_datasets("harris", 2025)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}

    assert dataset_lookup["property_roll"].source_system_code == "HCAD_BULK"
    assert "[manual_upload]" in dataset_lookup["property_roll"].description


def test_harris_adapter_live_file_override_for_historical_year(
    monkeypatch,
    tmp_path,
) -> None:
    source_path = tmp_path / "harris_property_roll_2025.json"
    source_path.write_text(json.dumps([{"account_number": "1001001001001"}]), encoding="utf-8")
    monkeypatch.setenv(
        "DWELLIO_HARRIS_PROPERTY_ROLL_2025_SOURCE_FILE_PATH",
        str(source_path),
    )

    adapter = HarrisCountyAdapter()
    datasets = adapter.list_available_datasets("harris", 2025)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}
    assert "[live_file]" in dataset_lookup["property_roll"].description

    acquired = adapter.acquire_dataset("property_roll", 2025)
    assert acquired.original_filename == source_path.name
    assert json.loads(acquired.content.decode("utf-8"))[0]["account_number"] == "1001001001001"


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


def test_harris_normalize_tax_rates_keeps_a31_as_billable_mud_defined_area() -> None:
    result = normalize_tax_rates(
        staging_rows=[
            {
                "unit_type_code": "special",
                "unit_code": "A31",
                "unit_name": "NEWPORT MUD DA 2",
                "rate_component": "ad_valorem",
                "rate_value": 0.007422,
                "rate_per_100": 0.7422,
                "aliases": ["Newport MUD Defined Area 2"],
                "assignment_hints": {
                    "account_numbers": ["0451420000005", "0451420000024"],
                    "source": "real_acct_jurs_special_family_bridge",
                },
                "source_jurisdiction_type_code": "A",
                "harris_family_label": "defined_area_levy",
            }
        ]
    )

    record = result.normalized_records[0]
    assert record["taxing_unit"]["unit_type_code"] == "mud"
    assert record["taxing_unit"]["unit_code"] == "A31"
    assert record["taxing_unit"]["metadata_json"]["harris_family_label"] == "defined_area_levy"
    assert record["taxing_unit"]["metadata_json"]["rate_bearing_status"] == "rate_bearing"
    assert (
        record["taxing_unit"]["metadata_json"]["assignment_hints"]["source"]
        == "real_acct_jurs_special_family_bridge"
    )
    assert record["tax_rate"]["rate_value"] == 0.007422


def test_harris_normalize_tax_rates_keeps_a76_as_caveated_unit_without_rate_row() -> None:
    result = normalize_tax_rates(
        staging_rows=[
            {
                "unit_type_code": "mud",
                "unit_code": "A76",
                "unit_name": "HC MUD 568",
                "aliases": ["Harris County Municipal Utility District No. 568"],
                "assignment_hints": {
                    "account_numbers": ["0402100000030"],
                    "source": "real_acct_jurs_special_family_bridge",
                },
                "rate_bearing_status": "caveated_rate_row_deferred",
                "normalization_caveat_codes": ["contradictory_basis_year_activity"],
            }
        ]
    )

    record = result.normalized_records[0]
    metadata_json = record["taxing_unit"]["metadata_json"]
    assert record["taxing_unit"]["unit_type_code"] == "mud"
    assert metadata_json["rate_bearing_status"] == "caveated_rate_row_deferred"
    assert metadata_json["assignment_eligible_without_rate"] is True
    assert metadata_json["normalization_caveat_codes"] == ["contradictory_basis_year_activity"]
    assert record["tax_rate"] is None


def test_harris_adapter_parse_and_normalize_deed_fixture() -> None:
    adapter = HarrisCountyAdapter()
    acquired = adapter.acquire_dataset("deeds", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    assert len(staging_rows) == 2

    normalized = adapter.normalize_staging_to_canonical(
        "deeds",
        [row.raw_payload for row in staging_rows],
    )
    deeds = normalized["deeds"]
    assert deeds[0]["deed_record"]["instrument_number"] == "HCAD-DEED-2026-0001"
    assert deeds[0]["linked_account_number"] == "1001001001001"
    assert deeds[0]["linked_cad_property_id"] == "HCAD-1001"
    assert deeds[0]["deed_parties"][0]["party_role"] == "grantor"
    assert deeds[1]["deed_record"]["grantee_summary"] == "Casey Purchaser & Morgan Purchaser"
    assert len(deeds[1]["deed_parties"]) == 3


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
    assert property_roll[0]["exemptions"][0]["raw_exemption_code"] == "HS"
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
    missing_account = next(
        finding for finding in findings if finding.validation_code == "MISSING_ACCOUNT_NUMBER"
    )
    assert missing_account.details_json["failed_record"]["situs_address"] == "101 Main St"


def test_harris_tax_rate_validation_allows_overlay_and_caveated_unit_only_rows() -> None:
    adapter = HarrisCountyAdapter()
    findings = adapter.validate_dataset(
        "job-test",
        2026,
        "tax_rates",
        [
            {
                "unit_type_code": "special",
                "unit_code": "A20",
                "unit_name": "TIRZ 1- CITY OF LAPORTE ANNEX 2",
                "rate_bearing_status": "linked_to_other_taxing_unit",
            },
            {
                "unit_type_code": "mud",
                "unit_code": "A76",
                "unit_name": "HC MUD 568",
                "rate_bearing_status": "caveated_rate_row_deferred",
            },
        ],
    )

    error_codes = {finding.validation_code for finding in findings if finding.severity == "error"}
    assert "INVALID_RATE_VALUE" not in error_codes
    assert "MISSING_RATE_VALUE" not in error_codes


def test_harris_adapter_deed_validation_surfaces_missing_grantee() -> None:
    adapter = HarrisCountyAdapter()
    findings = adapter.validate_dataset(
        "job-test",
        2026,
        "deeds",
        [{"instrument_number": "INST-1", "recording_date": "2026-01-01", "grantees": []}],
    )

    error_codes = {finding.validation_code for finding in findings if finding.severity == "error"}
    assert "MISSING_GRANTEE_PARTY" in error_codes


def test_harris_adapter_metadata_reflects_stage4_scope() -> None:
    metadata = HarrisCountyAdapter().get_adapter_metadata()
    assert metadata.county_id == "harris"
    assert metadata.supported_years == [2022, 2023, 2024, 2025, 2026]
    assert "fixture-backed" in metadata.known_limitations[0]
    assert "deeds" in metadata.supported_dataset_types
