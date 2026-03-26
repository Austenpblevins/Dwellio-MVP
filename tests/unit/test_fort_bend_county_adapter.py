from __future__ import annotations

import io

from app.county_adapters.fort_bend.adapter import FortBendCountyAdapter


def test_fort_bend_adapter_lists_property_roll_dataset() -> None:
    adapter = FortBendCountyAdapter()
    datasets = adapter.list_available_datasets("fort_bend", 2026)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}
    assert set(dataset_lookup) == {"property_roll", "tax_rates", "deeds"}
    assert dataset_lookup["property_roll"].source_system_code == "FBCAD_EXPORT"
    assert dataset_lookup["tax_rates"].source_system_code == "FBCAD_TAX_RATES"
    assert dataset_lookup["deeds"].source_system_code == "DEED_FEED"
    assert "[fixture_csv]" in dataset_lookup["property_roll"].description


def test_fort_bend_adapter_lists_historical_dataset_as_manual_upload() -> None:
    adapter = FortBendCountyAdapter()
    datasets = adapter.list_available_datasets("fort_bend", 2025)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}

    assert dataset_lookup["property_roll"].source_system_code == "FBCAD_EXPORT"
    assert "[manual_upload]" in dataset_lookup["property_roll"].description


def test_fort_bend_adapter_live_http_override_retries_with_headers(monkeypatch) -> None:
    observed_requests: list[object] = []

    class FakeResponse(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()
            return None

    attempts = {"count": 0}

    def fake_urlopen(request, timeout):
        observed_requests.append((request, timeout))
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise OSError("temporary failure")
        return FakeResponse(
            b"unit_type_code,unit_code,unit_name,rate_component,rate_value\ncounty,FBC,Fort Bend County,maintenance,0.01\n"
        )

    monkeypatch.setenv(
        "DWELLIO_FORT_BEND_TAX_RATES_2025_SOURCE_URL",
        "https://example.test/fort_bend_tax_rates_2025.csv",
    )
    monkeypatch.setenv(
        "DWELLIO_FORT_BEND_TAX_RATES_2025_HEADERS_JSON",
        '{"X-Test-Header":"yes"}',
    )
    monkeypatch.setenv("DWELLIO_FORT_BEND_TAX_RATES_2025_RETRY_ATTEMPTS", "2")
    monkeypatch.setenv("DWELLIO_FORT_BEND_TAX_RATES_2025_BACKOFF_SECONDS", "0")
    monkeypatch.setattr(
        "app.county_adapters.common.live_acquisition.urlopen",
        fake_urlopen,
    )

    adapter = FortBendCountyAdapter()
    datasets = adapter.list_available_datasets("fort_bend", 2025)
    dataset_lookup = {dataset.dataset_type: dataset for dataset in datasets}
    assert "[live_http]" in dataset_lookup["tax_rates"].description

    acquired = adapter.acquire_dataset("tax_rates", 2025)
    assert attempts["count"] == 2
    request, timeout = observed_requests[-1]
    assert request.full_url == "https://example.test/fort_bend_tax_rates_2025.csv"
    assert request.headers["X-test-header"] == "yes"
    assert timeout == 30.0
    assert acquired.original_filename == "fort_bend_tax_rates_2025.csv"


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


def test_fort_bend_adapter_parse_and_normalize_deed_fixture() -> None:
    adapter = FortBendCountyAdapter()
    acquired = adapter.acquire_dataset("deeds", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    assert len(staging_rows) == 2
    assert staging_rows[0].raw_payload["account_number"] == "2002002002001"
    assert len(staging_rows[1].raw_payload["grantees"]) == 2

    normalized = adapter.normalize_staging_to_canonical(
        "deeds",
        [row.raw_payload for row in staging_rows],
    )
    deeds = normalized["deeds"]
    assert deeds[0]["linked_cad_property_id"] == "FBCAD-2001"
    assert deeds[1]["deed_record"]["grantee_summary"] == "Jordan Buyer & Avery Buyer"
    assert len(deeds[1]["deed_parties"]) == 3


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


def test_fort_bend_adapter_deed_validation_surfaces_missing_grantee() -> None:
    adapter = FortBendCountyAdapter()
    findings = adapter.validate_dataset(
        "job-test",
        2026,
        "deeds",
        [{"instrument_number": "INST-1", "recording_date": "2026-01-01", "grantees": []}],
    )

    error_codes = {finding.validation_code for finding in findings if finding.severity == "error"}
    assert "MISSING_GRANTEE_PARTY" in error_codes


def test_fort_bend_adapter_metadata_reflects_stage5_scope() -> None:
    metadata = FortBendCountyAdapter().get_adapter_metadata()
    assert metadata.county_id == "fort_bend"
    assert metadata.supported_years == [2022, 2023, 2024, 2025, 2026]
    assert "fixture-backed" in metadata.known_limitations[0]
    assert "deeds" in metadata.supported_dataset_types
