from __future__ import annotations

from app.services.tax_assignment import (
    ParcelTaxContext,
    TaxingUnitContext,
    build_tax_assignments,
)


def test_tax_assignment_engine_assigns_expected_single_and_multi_units() -> None:
    parcels = [
        ParcelTaxContext(
            parcel_id="parcel-1",
            county_id="harris",
            tax_year=2026,
            account_number="1001001001001",
            situs_city="Houston",
            situs_zip="77002",
            school_district_name="Houston ISD",
            subdivision_name="Downtown",
            neighborhood_code="HOU-001",
        )
    ]
    taxing_units = [
        TaxingUnitContext(
            taxing_unit_id="tu-county",
            county_id="harris",
            tax_year=2026,
            unit_type_code="county",
            unit_code="HAR-COUNTY",
            unit_name="Harris County",
            metadata_json={"assignment_hints": {"county_ids": ["harris"]}},
        ),
        TaxingUnitContext(
            taxing_unit_id="tu-city",
            county_id="harris",
            tax_year=2026,
            unit_type_code="city",
            unit_code="HOU-CITY",
            unit_name="Houston",
            metadata_json={"assignment_hints": {"cities": ["Houston"]}},
        ),
        TaxingUnitContext(
            taxing_unit_id="tu-school",
            county_id="harris",
            tax_year=2026,
            unit_type_code="school",
            unit_code="HISD",
            unit_name="Houston ISD",
            metadata_json={"assignment_hints": {"school_district_names": ["Houston ISD"]}},
        ),
        TaxingUnitContext(
            taxing_unit_id="tu-mud",
            county_id="harris",
            tax_year=2026,
            unit_type_code="mud",
            unit_code="DT-MUD",
            unit_name="Downtown Utility District",
            metadata_json={"assignment_hints": {"subdivisions": ["Downtown"]}},
        ),
        TaxingUnitContext(
            taxing_unit_id="tu-special-a",
            county_id="harris",
            tax_year=2026,
            unit_type_code="special",
            unit_code="HAR-FCD",
            unit_name="Flood Control",
            metadata_json={"assignment_hints": {"zip_codes": ["77002"]}},
        ),
        TaxingUnitContext(
            taxing_unit_id="tu-special-b",
            county_id="harris",
            tax_year=2026,
            unit_type_code="special",
            unit_code="HAR-LGC",
            unit_name="Local Government Corp",
            metadata_json={"assignment_hints": {"zip_codes": ["77002"]}},
        ),
    ]

    assignments = build_tax_assignments(parcels=parcels, taxing_units=taxing_units)

    assert {assignment.taxing_unit_id for assignment in assignments} == {
        "tu-county",
        "tu-city",
        "tu-school",
        "tu-mud",
        "tu-special-a",
        "tu-special-b",
    }
    reasons = {assignment.taxing_unit_id: assignment.assignment_reason_code for assignment in assignments}
    assert reasons["tu-city"] == "match_city"
    assert reasons["tu-school"] == "match_school_district_name"


def test_tax_assignment_engine_prefers_more_specific_single_assignment_match() -> None:
    parcels = [
        ParcelTaxContext(
            parcel_id="parcel-1",
            county_id="fort_bend",
            tax_year=2026,
            account_number="2002002002001",
            situs_city="Sugar Land",
            situs_zip="77479",
            school_district_name="Fort Bend ISD",
            subdivision_name="Lake Pointe",
            neighborhood_code="FB-010",
        )
    ]
    taxing_units = [
        TaxingUnitContext(
            taxing_unit_id="mud-subdivision",
            county_id="fort_bend",
            tax_year=2026,
            unit_type_code="mud",
            unit_code="LP-MUD",
            unit_name="Lake Pointe Utility District",
            metadata_json={"assignment_hints": {"subdivisions": ["Lake Pointe"], "priority": 80}},
        ),
        TaxingUnitContext(
            taxing_unit_id="mud-account",
            county_id="fort_bend",
            tax_year=2026,
            unit_type_code="mud",
            unit_code="LP-MUD-ALT",
            unit_name="Lake Pointe Utility District Alternate",
            metadata_json={"assignment_hints": {"account_numbers": ["2002002002001"], "priority": 120}},
        ),
    ]

    assignments = build_tax_assignments(parcels=parcels, taxing_units=taxing_units)

    assert len(assignments) == 1
    assert assignments[0].taxing_unit_id == "mud-account"
    assert assignments[0].assignment_method == "source_direct"
    assert assignments[0].is_primary is True


def test_tax_assignment_engine_uses_special_family_bridge_for_a31_and_skips_overlay_codes() -> None:
    parcels = [
        ParcelTaxContext(
            parcel_id="parcel-a31",
            county_id="harris",
            tax_year=2026,
            account_number="0451420000005",
            situs_city="Crosby",
            situs_zip="77532",
            school_district_name="Crosby ISD",
        )
    ]
    taxing_units = [
        TaxingUnitContext(
            taxing_unit_id="tu-a31",
            county_id="harris",
            tax_year=2026,
            unit_type_code="mud",
            unit_code="A31",
            unit_name="NEWPORT MUD DA 2",
            metadata_json={
                "rate_bearing_status": "rate_bearing",
                "assignment_hints": {
                    "account_numbers": ["0451420000005"],
                    "source": "real_acct_jurs_special_family_bridge",
                },
            },
        ),
        TaxingUnitContext(
            taxing_unit_id="tu-a20",
            county_id="harris",
            tax_year=2026,
            unit_type_code="special",
            unit_code="A20",
            unit_name="TIRZ 1- CITY OF LAPORTE ANNEX 2",
            metadata_json={
                "rate_bearing_status": "linked_to_other_taxing_unit",
                "assignment_hints": {
                    "account_numbers": ["0451420000005"],
                    "source": "real_acct_jurs_special_family_bridge",
                },
            },
        ),
    ]

    assignments = build_tax_assignments(parcels=parcels, taxing_units=taxing_units)

    assert [assignment.taxing_unit_id for assignment in assignments] == ["tu-a31"]
    assert assignments[0].assignment_reason_code == "match_account_number"
    assert assignments[0].match_basis_json["hint_source"] == "real_acct_jurs_special_family_bridge"


def test_tax_assignment_engine_allows_caveated_unit_identity_without_rate_row() -> None:
    parcels = [
        ParcelTaxContext(
            parcel_id="parcel-a76",
            county_id="harris",
            tax_year=2026,
            account_number="0402100000030",
            situs_city="Houston",
            situs_zip="77001",
        )
    ]
    taxing_units = [
        TaxingUnitContext(
            taxing_unit_id="tu-a76",
            county_id="harris",
            tax_year=2026,
            unit_type_code="mud",
            unit_code="A76",
            unit_name="HC MUD 568",
            metadata_json={
                "rate_bearing_status": "caveated_rate_row_deferred",
                "assignment_hints": {
                    "account_numbers": ["0402100000030"],
                    "source": "real_acct_jurs_special_family_bridge",
                },
            },
        )
    ]

    assignments = build_tax_assignments(parcels=parcels, taxing_units=taxing_units)

    assert len(assignments) == 1
    assert assignments[0].taxing_unit_id == "tu-a76"
    assert assignments[0].match_basis_json["rate_bearing_status"] == "caveated_rate_row_deferred"
