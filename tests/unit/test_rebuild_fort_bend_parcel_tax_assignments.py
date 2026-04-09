from __future__ import annotations

import csv
from pathlib import Path

from infra.scripts.rebuild_fort_bend_parcel_tax_assignments import (
    FortBendParcelScopeRow,
    FortBendTaxUnitRow,
    build_preferred_school_unit_name_map,
    build_assignment_rows,
    load_entity_code_sets,
)


def test_load_entity_code_sets_deduplicates_by_quick_ref(tmp_path: Path) -> None:
    entity_export = tmp_path / "EntityExport.txt"
    with entity_export.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["OwnerQuickRefID", "EntityCode"])
        writer.writeheader()
        writer.writerow({"OwnerQuickRefID": "P100", "EntityCode": "G01"})
        writer.writerow({"OwnerQuickRefID": "P100", "EntityCode": "G01"})
        writer.writerow({"OwnerQuickRefID": "P100", "EntityCode": "D01"})
        writer.writerow({"OwnerQuickRefID": "P200", "EntityCode": "T206"})

    entity_export_2 = tmp_path / "EntityExport_2.txt"
    with entity_export_2.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["OwnerQuickRefID", "EntityCode"])
        writer.writeheader()
        writer.writerow({"OwnerQuickRefID": "P100", "EntityCode": "M215"})
        writer.writerow({"OwnerQuickRefID": "P300", "EntityCode": "G01"})

    result = load_entity_code_sets(entity_export_paths=[entity_export, entity_export_2])

    assert result == {
        "P100": {"G01", "D01", "M215"},
        "P200": {"T206"},
        "P300": {"G01"},
    }


def test_build_assignment_rows_matches_only_existing_rate_bearing_units() -> None:
    parcel_scope = [
        FortBendParcelScopeRow(
            parcel_id="parcel-1",
            account_number="001",
            cad_property_id="P100",
        ),
        FortBendParcelScopeRow(
            parcel_id="parcel-2",
            account_number="002",
            cad_property_id="P200",
        ),
    ]
    tax_units = {
        "G01": FortBendTaxUnitRow(
            taxing_unit_id="tu-g01",
            unit_code="G01",
            unit_name="Fort Bend General",
            unit_type_code="county",
        ),
        "D01": FortBendTaxUnitRow(
            taxing_unit_id="tu-d01",
            unit_code="D01",
            unit_name="Fort Bend Drainage",
            unit_type_code="special",
        ),
        "M215": FortBendTaxUnitRow(
            taxing_unit_id="tu-m215",
            unit_code="M215",
            unit_name="Fort Bend MUD 162",
            unit_type_code="mud",
        ),
    }

    assignments, unmatched = build_assignment_rows(
        entity_code_sets={
            "P100": {"G01", "D01", "T206"},
            "P200": {"G01", "M215"},
            "P999": {"G01"},
        },
        parcel_scope=parcel_scope,
        tax_units_by_code=tax_units,
    )

    assert {(row["parcel_id"], row["unit_code"]) for row in assignments} == {
        ("parcel-1", "D01"),
        ("parcel-1", "G01"),
        ("parcel-2", "G01"),
        ("parcel-2", "M215"),
    }
    assert unmatched == {"T206": 1}


def test_build_preferred_school_unit_name_map_prefers_s_code_units() -> None:
    tax_units = {
        "FBISD": FortBendTaxUnitRow(
            taxing_unit_id="tu-fbisd",
            unit_code="FBISD",
            unit_name="Fort Bend ISD",
            unit_type_code="school",
        ),
        "S07": FortBendTaxUnitRow(
            taxing_unit_id="tu-s07",
            unit_code="S07",
            unit_name="Fort Bend ISD",
            unit_type_code="school",
        ),
        "S01": FortBendTaxUnitRow(
            taxing_unit_id="tu-s01",
            unit_code="S01",
            unit_name="Lamar CISD",
            unit_type_code="school",
        ),
        "J07": FortBendTaxUnitRow(
            taxing_unit_id="tu-j07",
            unit_code="J07",
            unit_name="Houston Com Col Missouri City",
            unit_type_code="school",
        ),
    }

    result = build_preferred_school_unit_name_map(tax_units_by_code=tax_units)

    assert set(result) == {"FORT BEND ISD", "LAMAR CISD"}
    assert result["FORT BEND ISD"].unit_code == "S07"
    assert result["LAMAR CISD"].unit_code == "S01"
