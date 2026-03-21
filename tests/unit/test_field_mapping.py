from __future__ import annotations

from app.county_adapters.common.config_loader import load_county_adapter_config
from app.county_adapters.common.field_mapping import (
    build_normalized_record,
    canonical_field_codes,
    required_source_fields,
)
from app.county_adapters.fort_bend.adapter import build_fort_bend_fixture_rows
from app.county_adapters.harris.adapter import build_harris_fixture_rows


def test_harris_field_mapping_builds_expected_sections() -> None:
    config = load_county_adapter_config("harris")
    source_row = build_harris_fixture_rows()[0]

    normalized = build_normalized_record(
        config=config,
        dataset_type="property_roll",
        source_row=source_row,
    )

    assert normalized["parcel"]["account_number"] == "1001001001001"
    assert normalized["address"]["normalized_address"] == "101 MAIN ST HOUSTON TX 77002"
    assert normalized["characteristics"]["homestead_flag"] is True
    assert normalized["improvements"][0]["building_label"] == "Main"
    assert normalized["land_segments"][0]["segment_num"] == 1
    assert normalized["value_components"][2]["taxable_value"] == 230000
    assert normalized["assessment"]["exemption_value_total"] == 100000
    assert normalized["exemptions"][0]["exemption_type_code"] == "homestead"
    assert normalized["exemptions"][0]["raw_exemption_code"] == "HS"


def test_harris_required_source_fields_are_config_driven() -> None:
    config = load_county_adapter_config("harris")

    assert required_source_fields(config=config, dataset_type="property_roll") == [
        "account_number",
        "situs_address",
        "situs_city",
        "situs_zip",
    ]


def test_harris_canonical_field_codes_are_reviewable() -> None:
    config = load_county_adapter_config("harris")
    field_codes = canonical_field_codes(config=config, dataset_type="property_roll")

    assert "property_roll.parcel.account_number" in field_codes
    assert "property_roll.assessment.market_value" in field_codes
    assert "property_roll.exemptions.exemption_amount" in field_codes
    assert "property_roll.exemptions.raw_exemption_code" in field_codes


def test_fort_bend_field_mapping_builds_expected_sections() -> None:
    config = load_county_adapter_config("fort_bend")
    source_row = build_fort_bend_fixture_rows()[1]

    normalized = build_normalized_record(
        config=config,
        dataset_type="property_roll",
        source_row=source_row,
    )

    assert normalized["parcel"]["account_number"] == "2002002002002"
    assert normalized["address"]["normalized_address"] == "404 ELM GROVE RICHMOND TX 77406"
    assert normalized["characteristics"]["homestead_flag"] is True
    assert normalized["improvements"][0]["pool_flag"] is True
    assert normalized["value_components"][2]["taxable_value"] == 195000
    assert normalized["assessment"]["exemption_value_total"] == 110000
    assert normalized["exemptions"][0]["raw_exemption_code"] == "hs_amt"
    assert normalized["exemptions"][1]["exemption_type_code"] == "over65"


def test_fort_bend_required_source_fields_are_config_driven() -> None:
    config = load_county_adapter_config("fort_bend")

    assert required_source_fields(config=config, dataset_type="property_roll") == [
        "account_id",
        "site_address",
        "site_city",
        "site_zip",
    ]
