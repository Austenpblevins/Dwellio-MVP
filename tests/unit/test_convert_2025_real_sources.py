from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

from infra.scripts.convert_2025_real_sources import (
    convert_fort_bend,
    convert_harris,
    resolve_outputs,
    verify_outputs,
    HarrisRawPaths,
    FortBendRawPaths,
    _open_sqlite,
)
from infra.scripts.prepare_manual_county_files import prepare_manual_county_files


def test_convert_real_2025_sources_generates_adapter_ready_files(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    ready_dir = tmp_path / "ready"
    ready_dir.mkdir(parents=True)

    harris_paths = _write_harris_raw_files(raw_root)
    fort_bend_paths = _write_fort_bend_raw_files(raw_root)
    outputs = resolve_outputs(ready_dir)

    connection = _open_sqlite(tmp_path / "conversion.sqlite3")
    try:
        harris_counts = convert_harris(
            connection=connection,
            tax_year=2025,
            raw_paths=harris_paths,
            property_roll_output=outputs.harris_property_roll,
            tax_rates_output=outputs.harris_tax_rates,
        )
        fort_bend_counts = convert_fort_bend(
            connection=connection,
            tax_year=2025,
            raw_paths=fort_bend_paths,
            property_roll_output=outputs.fort_bend_property_roll,
            tax_rates_output=outputs.fort_bend_tax_rates,
        )
    finally:
        connection.close()

    assert harris_counts == {"property_roll": 1, "tax_rates": 2}
    assert fort_bend_counts == {"property_roll": 1, "tax_rates": 3}

    harris_property_roll = json.loads(outputs.harris_property_roll.read_text(encoding="utf-8"))
    assert harris_property_roll[0]["account_number"] == "0021440000001"
    assert harris_property_roll[0]["market_value"] == 484857
    assert harris_property_roll[0]["school_district_name"] == "HOUSTON ISD"
    assert harris_property_roll[0]["property_type_code"] == "sfr"
    assert harris_property_roll[0]["exemptions"][0]["exemption_type_code"] == "homestead"
    assert harris_property_roll[0]["exemptions"][0]["raw_exemption_code"] == "RES"

    with outputs.fort_bend_property_roll.open("r", encoding="utf-8", newline="") as handle:
        fort_bend_rows = list(csv.DictReader(handle))
    assert fort_bend_rows[0]["account_id"] == "5910-04-022-0700-907"
    assert fort_bend_rows[0]["school_district"] == "Fort Bend ISD"
    assert fort_bend_rows[0]["market_value"] == "213077"

    results = verify_outputs(outputs=outputs, tax_year=2025)
    assert all(result.parse_issue_count == 0 for result in results)
    assert all(result.validation_error_count == 0 for result in results)


def test_prepare_manual_county_files_generates_2026_outputs_from_canonical_layout(tmp_path: Path) -> None:
    raw_root = tmp_path / "2026" / "raw"
    ready_root = tmp_path / "2026" / "ready"
    ready_root.mkdir(parents=True)

    _copy_to_canonical_layout(
        harris_paths=_write_harris_raw_files(tmp_path / "legacy_harris"),
        fort_bend_paths=_write_fort_bend_raw_files(tmp_path / "legacy_fort_bend"),
        raw_root=raw_root,
    )

    results = prepare_manual_county_files(
        county_ids=["harris", "fort_bend"],
        tax_year=2026,
        dataset_types=["property_roll", "tax_rates"],
        raw_root=raw_root,
        ready_root=ready_root,
    )

    result_lookup = {(result.county_id, result.dataset_type): result for result in results}
    assert sorted(result_lookup) == [
        ("fort_bend", "property_roll"),
        ("fort_bend", "tax_rates"),
        ("harris", "property_roll"),
        ("harris", "tax_rates"),
    ]

    harris_property = json.loads((ready_root / "harris_property_roll_2026.json").read_text(encoding="utf-8"))
    assert harris_property[0]["account_number"] == "0021440000001"
    assert harris_property[0]["school_district_name"] == "HOUSTON ISD"
    assert harris_property[0]["exemptions"][0]["exemption_type_code"] == "homestead"

    with (ready_root / "fort_bend_tax_rates_2026.csv").open("r", encoding="utf-8", newline="") as handle:
        fort_bend_tax_rows = list(csv.DictReader(handle))
    assert fort_bend_tax_rows[0]["effective_from"] == "2026-01-01"

    manifest = json.loads((ready_root / "harris_property_roll_2026.manifest.json").read_text(encoding="utf-8"))
    assert manifest["county_id"] == "harris"
    assert manifest["dataset_type"] == "property_roll"
    assert manifest["validation"]["status"] == "passed"
    assert {item["logical_name"] for item in manifest["raw_files"]} == {
        "real_acct",
        "owners",
        "building_res",
        "land",
        "tax_rates",
        "jur_exempt",
        "jur_exempt_cd",
        "jur_exemption_dscr",
        "exemption_category_desc",
    }
    assert manifest["output_files"][0]["row_count"] == 1
    assert result_lookup[("harris", "property_roll")].verification is not None
    assert result_lookup[("harris", "property_roll")].verification.validation_error_count == 0


def test_prepare_manual_county_files_supports_explicit_override_for_noncanonical_name(tmp_path: Path) -> None:
    raw_root = tmp_path / "2026" / "raw"
    ready_root = tmp_path / "2026" / "ready"
    county_root = raw_root / "fort_bend"
    county_root.mkdir(parents=True)

    fort_bend_paths = _write_fort_bend_raw_files(tmp_path / "legacy_override")
    custom_tax_rates = county_root / "Fort Bend Tax Rate Source - Revised.csv"
    shutil.copyfile(fort_bend_paths.tax_rates, custom_tax_rates)

    results = prepare_manual_county_files(
        county_ids=["fort_bend"],
        tax_year=2026,
        dataset_types=["tax_rates"],
        raw_root=raw_root,
        ready_root=ready_root,
        raw_file_overrides={"fort_bend": {"tax_rates": custom_tax_rates}},
    )

    assert len(results) == 1
    assert results[0].dataset_type == "tax_rates"
    assert results[0].output_path == ready_root / "fort_bend_tax_rates_2026.csv"
    manifest = json.loads((ready_root / "fort_bend_tax_rates_2026.manifest.json").read_text(encoding="utf-8"))
    assert manifest["raw_files"][0]["logical_name"] == "tax_rates"
    assert manifest["raw_files"][0]["path"] == str(custom_tax_rates)
    assert manifest["validation"]["status"] == "passed"


def test_prepare_manual_county_files_accepts_comma_delimited_fort_bend_owner_and_exemption_exports(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "2026" / "raw"
    ready_root = tmp_path / "2026" / "ready"
    county_root = raw_root / "fort_bend"
    county_root.mkdir(parents=True)

    fort_bend_paths = _write_fort_bend_raw_files(tmp_path / "legacy_fort_bend_commas")

    shutil.copyfile(fort_bend_paths.property_export, county_root / "PropertyExport.txt")
    shutil.copyfile(fort_bend_paths.residential_segments, county_root / "WebsiteResidentialSegs.csv")
    shutil.copyfile(fort_bend_paths.tax_rates, county_root / "Fort Bend Tax Rate Source.csv")

    _rewrite_delimited_copy(
        source_path=fort_bend_paths.owner_export,
        target_path=county_root / "OwnerExport.txt",
        input_delimiter="\t",
        output_delimiter=",",
    )
    _rewrite_delimited_copy(
        source_path=fort_bend_paths.exemption_export,
        target_path=county_root / "ExemptionExport.txt",
        input_delimiter="\t",
        output_delimiter=",",
    )

    results = prepare_manual_county_files(
        county_ids=["fort_bend"],
        tax_year=2026,
        dataset_types=["property_roll"],
        raw_root=raw_root,
        ready_root=ready_root,
    )

    assert len(results) == 1
    assert results[0].dataset_type == "property_roll"
    with (ready_root / "fort_bend_property_roll_2026.csv").open(
        "r",
        encoding="utf-8",
        newline="",
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["account_id"] == "5910-04-022-0700-907"
    assert rows[0]["market_value"] == "213077"

def test_fort_bend_property_summary_square_footage_recovers_missing_residential_segment_area(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "legacy_fort_bend_property_summary"
    ready_dir = tmp_path / "ready"
    ready_dir.mkdir(parents=True)

    fort_bend_paths = _write_fort_bend_raw_files(raw_root)
    _rewrite_fort_bend_residential_segment_areas(fort_bend_paths.residential_segments, area_value="")
    property_summary_export = raw_root / "PropertyDataExport4558080.txt"
    property_summary_export.write_text(
        "RecordType,PropertyID,QuickRefID,PropertyNumber,SquareFootage\n"
        "1,50090,R100000,5910-04-022-0700-907,1216\n",
        encoding="utf-8",
    )

    outputs = resolve_outputs(ready_dir)
    connection = _open_sqlite(tmp_path / "conversion.sqlite3")
    try:
        counts = convert_fort_bend(
            connection=connection,
            tax_year=2025,
            raw_paths=FortBendRawPaths(
                property_export=fort_bend_paths.property_export,
                owner_export=fort_bend_paths.owner_export,
                exemption_export=fort_bend_paths.exemption_export,
                residential_segments=fort_bend_paths.residential_segments,
                tax_rates=fort_bend_paths.tax_rates,
                property_summary_export=property_summary_export,
            ),
            property_roll_output=outputs.fort_bend_property_roll,
            tax_rates_output=outputs.fort_bend_tax_rates,
        )
    finally:
        connection.close()

    assert counts["property_roll"] == 1
    with outputs.fort_bend_property_roll.open("r", encoding="utf-8", newline="") as handle:
        fort_bend_rows = list(csv.DictReader(handle))
    assert fort_bend_rows[0]["bldg_sqft"] == "1216"
    assert fort_bend_rows[0]["gross_component_area_sf"] == ""
    assert fort_bend_rows[0]["living_area_source"] == "property_summary_export"


def test_harris_building_index_uses_authoritative_tab_split_living_area(tmp_path: Path) -> None:
    connection = _open_sqlite(tmp_path / "harris_building.sqlite3")
    try:
        connection.executescript(
            """
            CREATE TABLE harris_building_lookup (
                acct TEXT PRIMARY KEY,
                primary_area REAL NOT NULL DEFAULT 0,
                living_area_sf INTEGER,
                year_built INTEGER,
                effective_year_built INTEGER,
                effective_age INTEGER,
                quality_code TEXT,
                condition_code TEXT,
                property_use_code TEXT
            );
            """
        )
        building_res = tmp_path / "building_res.txt"
        building_res.write_text(
            "acct\tproperty_use_cd\tbld_num\timpr_tp\timpr_mdl_cd\tstructure\tstructure_dscr\t"
            "dpr_val\tcama_replacement_cost\taccrued_depr_pct\tqa_cd\tdscr\tdate_erected\t"
            "eff\tyr_remodel\tyr_roll\tappr_by\tappr_dt\tnotes\tim_sq_ft\tact_ar\theat_ar\t"
            "gross_ar\teff_ar\tbase_ar\tperimeter\tpct\tbld_adj\trcnld\tsize_index\tlump_sum_adj\n"
            "1161530010003            \tA1\t1\t1001\t101 \tR  \tResidential\t2277763\t"
            "3673811\t0.620000\tX \tSuperior\t1988\t1988\t2004\t1985\tHTS\t01/01/2004\t"
            "note text\t7674\t9572\t7674\t9572\t8052\t7674\t980\t1.00\t1.2400\t"
            "1836906.00\t0.70000\t145022\n",
            encoding="utf-8",
        )

        from infra.scripts.prepare_manual_county_files import _index_harris_buildings

        _index_harris_buildings(connection, source_path=building_res, tax_year=2026)
        row = connection.execute(
            "SELECT living_area_sf FROM harris_building_lookup WHERE acct = ?",
            ("1161530010003",),
        ).fetchone()
    finally:
        connection.close()

    assert row is not None
    assert row["living_area_sf"] == 7674

def test_fort_bend_property_summary_square_footage_overrides_segment_total_and_preserves_gross_area(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "legacy_fort_bend_property_summary"
    ready_dir = tmp_path / "ready"
    ready_dir.mkdir(parents=True)

    fort_bend_paths = _write_fort_bend_raw_files(raw_root)
    property_summary_export = raw_root / "PropertyDataExport4558080.txt"
    property_summary_export.write_text(
        "RecordType,PropertyID,QuickRefID,PropertyNumber,SquareFootage\n"
        "1,50090,R100000,5910-04-022-0700-907,1216\n",
        encoding="utf-8",
    )

    outputs = resolve_outputs(ready_dir)
    connection = _open_sqlite(tmp_path / "conversion.sqlite3")
    try:
        counts = convert_fort_bend(
            connection=connection,
            tax_year=2025,
            raw_paths=FortBendRawPaths(
                property_export=fort_bend_paths.property_export,
                owner_export=fort_bend_paths.owner_export,
                exemption_export=fort_bend_paths.exemption_export,
                residential_segments=fort_bend_paths.residential_segments,
                tax_rates=fort_bend_paths.tax_rates,
                property_summary_export=property_summary_export,
            ),
            property_roll_output=outputs.fort_bend_property_roll,
            tax_rates_output=outputs.fort_bend_tax_rates,
        )
    finally:
        connection.close()

    assert counts["property_roll"] == 1
    with outputs.fort_bend_property_roll.open("r", encoding="utf-8", newline="") as handle:
        fort_bend_rows = list(csv.DictReader(handle))
    assert fort_bend_rows[0]["bldg_sqft"] == "1216"
    assert fort_bend_rows[0]["gross_component_area_sf"] == "1305"
    assert fort_bend_rows[0]["living_area_source"] == "property_summary_export"


def _rewrite_delimited_copy(
    *,
    source_path: Path,
    target_path: Path,
    input_delimiter: str,
    output_delimiter: str,
) -> None:
    with source_path.open("r", encoding="utf-8", newline="") as source_handle:
        reader = csv.DictReader(source_handle, delimiter=input_delimiter)
        fieldnames = reader.fieldnames
        assert fieldnames is not None
        rows = list(reader)

    with target_path.open("w", encoding="utf-8", newline="") as target_handle:
        writer = csv.DictWriter(target_handle, fieldnames=fieldnames, delimiter=output_delimiter)
        writer.writeheader()
        writer.writerows(rows)


def _rewrite_fort_bend_residential_segment_areas(source_path: Path, *, area_value: str) -> None:
    with source_path.open("r", encoding="utf-8", newline="") as source_handle:
        reader = csv.DictReader(source_handle)
        fieldnames = reader.fieldnames
        assert fieldnames is not None
        rows = list(reader)
    for row in rows:
        row["fArea"] = area_value
        row["vTSGRSeg_AdjArea"] = area_value
    with source_path.open("w", encoding="utf-8", newline="") as target_handle:
        writer = csv.DictWriter(target_handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_harris_raw_files(raw_root: Path) -> HarrisRawPaths:
    acct_owner_dir = raw_root / "2025 Harris_Real_acct_owner"
    building_land_dir = raw_root / "2025 Harris_Real_building_land"
    tax_dir = raw_root / "2025 Harris Roll Source_Real_jur_exempt"
    exempt_dir = raw_root / "2025 Harris_Real_jur_exempt"
    desc_dir = raw_root / "2025 Harris_Code_description_real"
    acct_owner_dir.mkdir(parents=True)
    building_land_dir.mkdir(parents=True)
    tax_dir.mkdir(parents=True)
    exempt_dir.mkdir(parents=True)
    desc_dir.mkdir(parents=True)

    real_acct = acct_owner_dir / "real_acct.txt"
    real_acct.write_text(
        "\t".join(
            [
                "acct",
                "school_dist",
                "site_addr_1",
                "site_addr_2",
                "site_addr_3",
                "state_class",
                "Neighborhood_Code",
                "land_ar",
                "acreage",
                "land_val",
                "bld_val",
                "x_features_val",
                "assessed_val",
                "tot_appr_val",
                "tot_mkt_val",
                "prior_tot_appr_val",
                "prior_tot_mkt_val",
                "certified_date",
                "lgl_1",
                "lgl_2",
                "mailto",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "0021440000001",
                "01",
                "2120 LIVE OAK ST",
                "HOUSTON",
                "77003",
                "A1",
                "8400.12",
                "5000",
                "0.1148",
                "220000",
                "264857",
                "0",
                "484857",
                "342370",
                "484857",
                "311246",
                "510178",
                "2025-07-20",
                "LT 1 BLK 426",
                "SSBB",
                "CURRENT OWNER",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    owners = acct_owner_dir / "owners.txt"
    owners.write_text(
        "acct\tln_num\tname\taka\tpct_own\n"
        "0021440000001\t1\tTREWICK MICHAEL & MEGONE\t\t1.0000\n",
        encoding="utf-8",
    )

    building_res = building_land_dir / "building_res.txt"
    building_res.write_text(
        "\t".join(
            [
                "acct",
                "property_use_cd",
                "qa_cd",
                "dscr",
                "date_erected",
                "eff",
                "im_sq_ft",
                "heat_ar",
                "gross_ar",
                "eff_ar",
                "act_ar",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "0021440000001",
                "A1",
                "B",
                "Good",
                "2004",
                "2004",
                "2537",
                "2537",
                "2537",
                "2537",
                "2537",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    land = building_land_dir / "land.txt"
    land.write_text(
        "acct\tnum\tuse_cd\tuse_dscr\tinf_cd\tinf_dscr\tinf_adj\ttp\tuts\tsz_fact\tinf_fact\tcond\tovr_dscr\ttot_adj\tunit_prc\tadj_unit_prc\tval\tovr_val\n"
        "0021440000001\t1\t8005\tLand Neighborhood Section 5\t4600\t\t1.0000\tSF\t5000.0000\t1.0000\t1.0000\t1.0000\t\t1.0000\t0\t0\t220000\t0\n",
        encoding="utf-8",
    )

    tax_rates = tax_dir / "jur_tax_dist_exempt_value_rate.txt"
    tax_rates.write_text(
        "RP_TYPE\ttax_dist\tname\texempt_cd\tprop\tcurr\texempt_val\texempt_rate\n"
        "Real\t001\tHOUSTON ISD\tHS\t0.868300\t0.878300\t65000\t0.000000\n"
        "Real\t040\tHARRIS COUNTY\tHS\t0.350000\t0.340000\t0\t0.000000\n",
        encoding="utf-8",
    )

    jur_exempt = exempt_dir / "jur_exempt.txt"
    jur_exempt.write_text(
        "acct\ttax_district\texempt_cat\texempt_val\n"
        "0021440000001\t001\tRES\tPending\n",
        encoding="utf-8",
    )

    jur_exempt_cd = exempt_dir / "jur_exempt_cd.txt"
    jur_exempt_cd.write_text(
        "acct\texempt_cat\n"
        "0021440000001\tRES\n",
        encoding="utf-8",
    )

    jur_exemption_dscr = exempt_dir / "jur_exemption_dscr.txt"
    jur_exemption_dscr.write_text(
        "exempt_cat\texemption_dscr\n"
        "RES\tResidential Homestead\n",
        encoding="utf-8",
    )

    exemption_category_desc = desc_dir / "desc_r_14_exemption_category.txt"
    exemption_category_desc.write_text(
        "Category\tDescription\n"
        "RES\tResidential Homestead\n",
        encoding="utf-8",
    )

    return HarrisRawPaths(
        real_acct=real_acct,
        owners=owners,
        building_res=building_res,
        land=land,
        tax_rates=tax_rates,
        jur_exempt=jur_exempt,
        jur_exempt_cd=jur_exempt_cd,
        jur_exemption_dscr=jur_exemption_dscr,
        exemption_category_desc=exemption_category_desc,
    )


def _copy_to_canonical_layout(
    *,
    harris_paths: HarrisRawPaths,
    fort_bend_paths: FortBendRawPaths,
    raw_root: Path,
) -> None:
    harris_root = raw_root / "harris"
    fort_bend_root = raw_root / "fort_bend"
    harris_root.mkdir(parents=True)
    fort_bend_root.mkdir(parents=True)

    shutil.copyfile(harris_paths.real_acct, harris_root / "real_acct.txt")
    shutil.copyfile(harris_paths.owners, harris_root / "owners.txt")
    shutil.copyfile(harris_paths.building_res, harris_root / "building_res.txt")
    shutil.copyfile(harris_paths.land, harris_root / "land.txt")
    shutil.copyfile(harris_paths.tax_rates, harris_root / "jur_tax_dist_exempt_value_rate.txt")
    shutil.copyfile(harris_paths.jur_exempt, harris_root / "jur_exempt.txt")
    shutil.copyfile(harris_paths.jur_exempt_cd, harris_root / "jur_exempt_cd.txt")
    shutil.copyfile(harris_paths.jur_exemption_dscr, harris_root / "jur_exemption_dscr.txt")
    shutil.copyfile(harris_paths.exemption_category_desc, harris_root / "desc_r_14_exemption_category.txt")

    shutil.copyfile(fort_bend_paths.property_export, fort_bend_root / "PropertyExport.txt")
    shutil.copyfile(fort_bend_paths.owner_export, fort_bend_root / "OwnerExport.txt")
    shutil.copyfile(fort_bend_paths.exemption_export, fort_bend_root / "ExemptionExport.txt")
    shutil.copyfile(fort_bend_paths.residential_segments, fort_bend_root / "WebsiteResidentialSegs.csv")
    shutil.copyfile(fort_bend_paths.tax_rates, fort_bend_root / "Fort Bend Tax Rate Source.csv")


def _write_fort_bend_raw_files(raw_root: Path) -> FortBendRawPaths:
    extracted_dir = raw_root / "2025 Fort Bend_Certified Export-EXTRACTED"
    extracted_dir.mkdir(parents=True)

    property_export = extracted_dir / "2025_07_17_1800_PropertyExport.txt"
    property_export.write_text(
        ",".join(
            [
                "RecordType",
                "PropertyQuickRefID",
                "PropertyNumber",
                "PropertyTypeCode",
                "SitusStreetAddress",
                "SitusCity",
                "SitusState",
                "SitusZip",
                "LegalDesc",
                "AbstractCode",
                "Block",
                "Tract",
                "Acres",
                "Entities",
                "StateCodeLand",
                "StateCodeImp",
                "ProdUseCode",
                "ARBProtestFlag",
                "CADFlag",
                "ApplyTUPct",
                "ChangeCode",
                "Comment",
                "TaxYear",
                "SupplementNumber",
            ]
        )
        + "\n"
        + ",".join(
            [
                "1",
                "R100000",
                "5910-04-022-0700-907",
                "RR",
                "2110 Hilton Head DR",
                "Missouri City",
                "TX",
                "77459",
                "\"QUAIL VALLEY EAST SEC 4, BLOCK 22, LOT 7\"",
                "5910-04",
                "22",
                "",
                "0.1148",
                "\"C09,D01,G01,J07,M27,S07\"",
                "A1",
                "A1",
                "",
                "0",
                "\"C09,CAD,D01,G01,J07,M27,S07\"",
                "",
                "M",
                "",
                "2025",
                "CERT",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    owner_export = extracted_dir / "2025_07_17_1800_OwnerExport.txt"
    owner_export.write_text(
        "\t".join(
            [
                "RecordType",
                "PropertyQuickRefID",
                "PartyQuickRefID",
                "OwnerQuickRefID",
                "OwnerPropertyNumber",
                "OwnerName",
                "OwnerAddress1",
                "OwnerAddress2",
                "OwnerAddress3",
                "OwnerCity",
                "OwnerState",
                "OwnerPostalCode",
                "OwnerCountry",
                "AddressType",
                "UndeliverableFlag",
                "OwnershipPercentage",
                "ConfidentialFlag",
                "CAAgentID",
                "ARBAgentID",
                "EntityAgentID",
                "DBA",
                "ImpHSValue",
                "ImpNHSValue",
                "LandHSValue",
                "LandNHSValue",
                "AgMktValue",
                "AgUseValue",
                "TimMktValue",
                "TimUseValue",
                "HSCapAdj",
                "PersonalValue",
                "MineralValue",
                "AutoValue",
                "NewImpHS",
                "RenditionPenaltyPct",
                "CurrentOwnerName",
                "CurrentOwnerAddress1",
                "CurrentOwnerAddress2",
                "CurrentOwnerAddress3",
                "CurrentOwnerCity",
                "CurrentOwnerState",
                "CurrentOwnerPostalCode",
                "CurrentOwnerCountry",
                "CurrentAddressType",
                "CBLCapAdj",
                "",
                "CurrentPartyQuickRefID",
                "CurrentOwnerQuickRefID",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "2",
                "R100000",
                "O0895068",
                "R100000",
                "5910-04-022-0700-907",
                "Shearita Denene Singleton Revocable Living Trust",
                "200 S Paulsen AVE",
                "",
                "",
                "Compton",
                "CA",
                "90220-6712",
                "",
                "S",
                "0",
                "100",
                "0",
                "",
                "",
                "",
                "",
                "183177",
                "0",
                "29900",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0",
                "",
                "",
                "",
                "0",
                "",
                "Shearita Denene Singleton Revocable Living Trust",
                "200 S Paulsen AVE",
                "",
                "",
                "Compton",
                "CA",
                "90220-6712",
                "",
                "S",
                "0",
                "",
                "O0895068",
                "R100000",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exemption_export = extracted_dir / "2025_07_17_1800_ExemptionExport.txt"
    exemption_export.write_text(
        "RecordType\tOwnerQuickRefID\tExemptionCode\tApplicationDate\tEffectiveDate\tTerminationDate\tSurvivingSpouseIndicator\tExemptionPct\tType\tLevel\tChildren\tDVO65Flag\tTransferDate\tTransferAmount\tBICode\tBIPercentage\tSPEXInfo\tTaxingUnitsOA\tTaxingUnitsDP\tDSSTRDate\tDSSTPDate\tDSSTYDate\n"
        "3\tR100000\tHS\t2/2/2011\t1/1/2009\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n",
        encoding="utf-8",
    )

    residential_segments = raw_root / "WebsiteResidentialSegs-7-22.csv"
    residential_segments.write_text(
        ",".join(
            [
                "vwPropertyGeneral_AdHocTaxYear",
                "PropertyStatusCode",
                "QuickRefID",
                "PropertyNumber",
                "PropertyTypeCode",
                "PropertyID",
                "InstanceID",
                "cama_TSGRSeg_AdHocTaxYear",
                "NodeID",
                "TreeNodeID",
                "LBound",
                "ParentNodeID",
                "LPParentNodeID",
                "FirstPage",
                "fActYear",
                "fAddlFixtures",
                "fApexMapping",
                "fApprMethod",
                "fArea",
                "fAreaFactor",
                "fBedrooms",
                "fCapPercent",
                "fCapStatus",
                "fCapYear",
                "fCDU",
                "fChangeFinder",
                "fComment",
                "fCondition",
                "fConstStyle",
                "fEconCd",
                "fEconomic",
                "fEffYear",
                "fExtFinish",
                "fFireplace",
                "fFlatValue",
                "fFlooring",
                "fFoundation",
                "fFunctional",
                "fFuntionalCd",
                "fHeatAC",
                "fIntFinish",
                "fNumHalfBath",
                "fNumQuaterBath",
                "fPercentComplete",
                "fPercentGood",
                "fPerimeter",
                "fPhysical",
                "fPhysicalCd",
                "fPlumbing",
                "fRemodComments",
                "fRemodType",
                "fRemodYear",
                "fRoof",
                "fRooms",
                "fSegClass",
                "fSegType",
                "fUpgrade",
                "fUpgradeReason",
                "fUpgradeReasons",
                "fVectors",
                "vTSGRSeg_AdjArea",
                "vTSGRSeg_AdjFactor",
                "vTSGRSeg_CalcPctGood",
                "vTSGRSeg_DefaultCDU",
                "vTSGRSeg_EconPercent",
                "vTSGRSeg_FixedIncrement",
                "vTSGRSeg_FixedIncUnAdj",
                "vTSGRSeg_FPValue",
                "vTSGRSeg_FuncPercent",
                "vTSGRSeg_ImpNum",
                "vTSGRSeg_ImpSegNum",
                "vTSGRSeg_MktUnitPrice",
                "vTSGRSeg_NbhdPercent",
                "vTSGRSeg_PercentGood",
                "vTSGRSeg_PhysPercent",
                "vTSGRSeg_SegmentValNbhd",
                "vTSGRSeg_SegmentValue",
                "vTSGRSeg_SegmentValueUnadj",
                "vTSGRSeg_SegNewValue",
                "vTSGRSeg_SegNewYear",
                "vTSGRSeg_SumPorch",
                "vTSGRSeg_TableClass",
                "vTSGRSeg_TempVar",
                "vTSGRSeg_UnitPrice",
                "vTSGRSeg_UnitPrUnAdj",
                "vTSGRSeg_YearNewValue",
                "vTSGRSeg_FireplaceValue",
                "vTSGRSeg_UpgradeValue",
            ]
        )
        + "\n"
        + ",".join(
            [
                "2025",
                "A",
                "R100000",
                "5910-04-022-0700-907",
                "RR",
                "50090",
                "505669",
                "2025",
                "11",
                "85973000",
                "9",
                "85972999",
                "85972998",
                "1",
                "1976",
                "",
                "21",
                "RMS",
                "1305",
                "",
                "4",
                "",
                "",
                "",
                "21",
                "",
                "",
                "0",
                "CV",
                "",
                "",
                "1976",
                "",
                "",
                "0",
                "",
                "",
                "",
                "",
                "",
                "",
                "1",
                "",
                "",
                "70",
                "164",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "RA2",
                "",
                "",
                "",
                "",
                "",
                "1305",
                "0.57",
                "57",
                "",
                "100",
                "18322",
                "14427",
                "18322",
                "100",
                "1",
                "1",
                "127",
                "57",
                "100",
                "189875",
                "108229",
                "149508",
                "",
                "",
                "0",
                "",
                "",
                "131.46",
                "103.51",
                "",
                "9699",
                "0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    tax_rates = raw_root / "2025 Fort Bend Tax Rate Source.csv"
    tax_rates.write_text(
        "Code,Taxing Unit,Type,Contact Name,State Number,Total Rate,I&S Rate,M&O Rate,Prev Rate\n"
        "C09,City of Missouri City,City,Fort Bend County,07910903,1.1594830,0.6496190,0.5098640,0.6496190\n"
        "S07,Fort Bend ISD,School,Fort Bend County,07990100,1.1200000,0.2500000,0.8700000,1.1000000\n"
        "G01,Fort Bend County,County General,Fort Bend County,07900000,0.4200000,0.0000000,0.4200000,0.4200000\n",
        encoding="utf-8",
    )

    return FortBendRawPaths(
        property_export=property_export,
        owner_export=owner_export,
        exemption_export=exemption_export,
        residential_segments=residential_segments,
        tax_rates=tax_rates,
    )
