from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from app.county_adapters.common.base import AcquiredDataset
from app.county_adapters.common.config_loader import load_county_adapter_config
from app.county_adapters.fort_bend.parse import parse_raw_to_staging as parse_fort_bend
from app.county_adapters.fort_bend.validation import validate_property_roll as validate_fb_property_roll
from app.county_adapters.fort_bend.validation import validate_tax_rates as validate_fb_tax_rates
from app.county_adapters.harris.parse import parse_raw_to_staging as parse_harris
from app.county_adapters.harris.validation import validate_property_roll as validate_harris_property_roll
from app.county_adapters.harris.validation import validate_tax_rates as validate_harris_tax_rates

csv.field_size_limit(10_000_000)

HARRIS_PROPERTY_ROLL_FILENAME = "harris_property_roll_2025.json"
HARRIS_TAX_RATES_FILENAME = "harris_tax_rates_2025.json"
FORT_BEND_PROPERTY_ROLL_FILENAME = "fort_bend_property_roll_2025.csv"
FORT_BEND_TAX_RATES_FILENAME = "fort_bend_tax_rates_2025.csv"

FORT_BEND_PROPERTY_HEADERS = [
    "account_id",
    "property_id",
    "site_address",
    "site_city",
    "site_zip",
    "owner_name",
    "class_cd",
    "neighborhood_cd",
    "subdivision",
    "school_district",
    "bldg_sqft",
    "yr_built",
    "eff_yr_built",
    "eff_age",
    "bed_cnt",
    "bath_full",
    "bath_half",
    "story_count",
    "quality_grade",
    "condition_grade",
    "garage_capacity",
    "pool_ind",
    "land_sqft",
    "land_acres",
    "land_market_value",
    "improvement_market_value",
    "market_value",
    "assessed_value",
    "notice_value",
    "appraised_value",
    "certified_value",
    "prior_market_value",
    "prior_assessed_value",
    "hs_amt",
    "ov65_amt",
]

FORT_BEND_TAX_RATE_HEADERS = [
    "unit_type_code",
    "unit_code",
    "unit_name",
    "rate_component",
    "rate_value",
    "rate_per_100",
    "effective_from",
    "effective_to",
    "cities",
    "school_district_names",
    "subdivisions",
    "zip_codes",
    "account_numbers",
    "aliases",
    "priority",
    "allow_multiple",
]


@dataclass(frozen=True)
class HarrisRawPaths:
    real_acct: Path
    owners: Path
    building_res: Path
    land: Path
    tax_rates: Path


@dataclass(frozen=True)
class FortBendRawPaths:
    property_export: Path
    owner_export: Path
    exemption_export: Path
    residential_segments: Path
    tax_rates: Path


@dataclass(frozen=True)
class ConversionOutputs:
    harris_property_roll: Path
    harris_tax_rates: Path
    fort_bend_property_roll: Path
    fort_bend_tax_rates: Path


@dataclass(frozen=True)
class DatasetVerification:
    county_id: str
    dataset_type: str
    row_count: int
    parse_issue_count: int
    validation_error_count: int


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert real 2025 Harris and Fort Bend county exports into adapter-ready local files "
            "for PR1 live-ingestion validation."
        )
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path.home() / "county-data" / "2025" / "raw",
        help="Root directory containing the raw county export folders.",
    )
    parser.add_argument(
        "--ready-dir",
        type=Path,
        default=Path.home() / "county-data" / "2025" / "ready",
        help="Directory where adapter-ready outputs will be written.",
    )
    parser.add_argument(
        "--tax-year",
        type=int,
        default=2025,
        help="Tax year to convert. PR1 promotion validation expects 2025.",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip parse/validation verification with the existing county adapters.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    if args.tax_year != 2025:
        raise SystemExit("This converter is scoped to the 2025 real-source promotion workflow.")

    raw_root = args.raw_root.expanduser().resolve()
    ready_dir = args.ready_dir.expanduser().resolve()
    ready_dir.mkdir(parents=True, exist_ok=True)

    harris_paths = resolve_harris_paths(raw_root)
    fort_bend_paths = resolve_fort_bend_paths(raw_root)
    outputs = resolve_outputs(ready_dir)

    with tempfile.NamedTemporaryFile(
        prefix="dwellio-2025-real-source-conversion-",
        suffix=".sqlite3",
        delete=True,
    ) as handle:
        db_path = Path(handle.name)
        connection = _open_sqlite(db_path)
        try:
            harris_counts = convert_harris(
                connection=connection,
                tax_year=args.tax_year,
                raw_paths=harris_paths,
                property_roll_output=outputs.harris_property_roll,
                tax_rates_output=outputs.harris_tax_rates,
            )
            fort_bend_counts = convert_fort_bend(
                connection=connection,
                tax_year=args.tax_year,
                raw_paths=fort_bend_paths,
                property_roll_output=outputs.fort_bend_property_roll,
                tax_rates_output=outputs.fort_bend_tax_rates,
            )
        finally:
            connection.close()

    print("Generated adapter-ready outputs:")
    print(f"- Harris property_roll: {outputs.harris_property_roll} ({harris_counts['property_roll']} rows)")
    print(f"- Harris tax_rates: {outputs.harris_tax_rates} ({harris_counts['tax_rates']} rows)")
    print(
        "- Fort Bend property_roll: "
        f"{outputs.fort_bend_property_roll} ({fort_bend_counts['property_roll']} rows)"
    )
    print(
        f"- Fort Bend tax_rates: {outputs.fort_bend_tax_rates} ({fort_bend_counts['tax_rates']} rows)"
    )

    if args.skip_verify:
        return

    verification_results = verify_outputs(outputs=outputs, tax_year=args.tax_year)
    print("Verification results:")
    for result in verification_results:
        print(
            f"- {result.county_id}/{result.dataset_type}: "
            f"rows={result.row_count} parse_issues={result.parse_issue_count} "
            f"validation_errors={result.validation_error_count}"
        )

    failures = [
        result
        for result in verification_results
        if result.parse_issue_count > 0 or result.validation_error_count > 0
    ]
    if failures:
        raise SystemExit("One or more generated files failed adapter parse/validation checks.")


def resolve_harris_paths(raw_root: Path) -> HarrisRawPaths:
    return HarrisRawPaths(
        real_acct=raw_root / "2025 Harris_Real_acct_owner" / "real_acct.txt",
        owners=raw_root / "2025 Harris_Real_acct_owner" / "owners.txt",
        building_res=raw_root / "2025 Harris_Real_building_land" / "building_res.txt",
        land=raw_root / "2025 Harris_Real_building_land" / "land.txt",
        tax_rates=raw_root
        / "2025 Harris Roll Source_Real_jur_exempt"
        / "jur_tax_dist_exempt_value_rate.txt",
    )


def resolve_fort_bend_paths(raw_root: Path) -> FortBendRawPaths:
    base = raw_root / "2025 Fort Bend_Certified Export-EXTRACTED"
    return FortBendRawPaths(
        property_export=base / "2025_07_17_1800_PropertyExport.txt",
        owner_export=base / "2025_07_17_1800_OwnerExport.txt",
        exemption_export=base / "2025_07_17_1800_ExemptionExport.txt",
        residential_segments=raw_root / "WebsiteResidentialSegs-7-22.csv",
        tax_rates=raw_root / "2025 Fort Bend Tax Rate Source.csv",
    )


def resolve_outputs(ready_dir: Path) -> ConversionOutputs:
    return ConversionOutputs(
        harris_property_roll=ready_dir / HARRIS_PROPERTY_ROLL_FILENAME,
        harris_tax_rates=ready_dir / HARRIS_TAX_RATES_FILENAME,
        fort_bend_property_roll=ready_dir / FORT_BEND_PROPERTY_ROLL_FILENAME,
        fort_bend_tax_rates=ready_dir / FORT_BEND_TAX_RATES_FILENAME,
    )


def convert_harris(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    raw_paths: HarrisRawPaths,
    property_roll_output: Path,
    tax_rates_output: Path,
) -> dict[str, int]:
    _require_files(
        [
            raw_paths.real_acct,
            raw_paths.owners,
            raw_paths.building_res,
            raw_paths.land,
            raw_paths.tax_rates,
        ]
    )
    _prepare_harris_lookup_tables(connection, raw_paths)
    school_district_lookup = _build_harris_school_district_lookup(raw_paths.tax_rates)
    property_roll_count = _write_harris_property_roll(
        connection=connection,
        tax_year=tax_year,
        raw_path=raw_paths.real_acct,
        school_district_lookup=school_district_lookup,
        output_path=property_roll_output,
    )
    tax_rate_count = _write_harris_tax_rates(
        raw_path=raw_paths.tax_rates,
        output_path=tax_rates_output,
        tax_year=tax_year,
    )
    return {"property_roll": property_roll_count, "tax_rates": tax_rate_count}


def convert_fort_bend(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    raw_paths: FortBendRawPaths,
    property_roll_output: Path,
    tax_rates_output: Path,
) -> dict[str, int]:
    _require_files(
        [
            raw_paths.property_export,
            raw_paths.owner_export,
            raw_paths.exemption_export,
            raw_paths.residential_segments,
            raw_paths.tax_rates,
        ]
    )
    entity_lookup = _build_fort_bend_tax_entity_lookup(raw_paths.tax_rates)
    _prepare_fort_bend_lookup_tables(connection, raw_paths)
    property_roll_count = _write_fort_bend_property_roll(
        connection=connection,
        tax_year=tax_year,
        raw_path=raw_paths.property_export,
        entity_lookup=entity_lookup,
        output_path=property_roll_output,
    )
    tax_rate_count = _write_fort_bend_tax_rates(
        raw_path=raw_paths.tax_rates,
        output_path=tax_rates_output,
        tax_year=tax_year,
    )
    return {"property_roll": property_roll_count, "tax_rates": tax_rate_count}


def verify_outputs(*, outputs: ConversionOutputs, tax_year: int) -> list[DatasetVerification]:
    harris_config = load_county_adapter_config("harris")
    fort_bend_config = load_county_adapter_config("fort_bend")

    verifications: list[DatasetVerification] = []
    verifications.append(
        _verify_dataset(
            county_id="harris",
            dataset_type="property_roll",
            tax_year=tax_year,
            config=harris_config,
            source_system_code=harris_config.dataset_configs["property_roll"].source_system_code,
            media_type="application/json",
            source_path=outputs.harris_property_roll,
            parse_fn=parse_harris,
            validate_fn=validate_harris_property_roll,
        )
    )
    verifications.append(
        _verify_dataset(
            county_id="harris",
            dataset_type="tax_rates",
            tax_year=tax_year,
            config=harris_config,
            source_system_code=harris_config.dataset_configs["tax_rates"].source_system_code,
            media_type="application/json",
            source_path=outputs.harris_tax_rates,
            parse_fn=parse_harris,
            validate_fn=validate_harris_tax_rates,
        )
    )
    verifications.append(
        _verify_dataset(
            county_id="fort_bend",
            dataset_type="property_roll",
            tax_year=tax_year,
            config=fort_bend_config,
            source_system_code=fort_bend_config.dataset_configs["property_roll"].source_system_code,
            media_type="text/csv",
            source_path=outputs.fort_bend_property_roll,
            parse_fn=parse_fort_bend,
            validate_fn=validate_fb_property_roll,
        )
    )
    verifications.append(
        _verify_dataset(
            county_id="fort_bend",
            dataset_type="tax_rates",
            tax_year=tax_year,
            config=fort_bend_config,
            source_system_code=fort_bend_config.dataset_configs["tax_rates"].source_system_code,
            media_type="text/csv",
            source_path=outputs.fort_bend_tax_rates,
            parse_fn=parse_fort_bend,
            validate_fn=validate_fb_tax_rates,
        )
    )
    return verifications


def _verify_dataset(
    *,
    county_id: str,
    dataset_type: str,
    tax_year: int,
    config: Any,
    source_system_code: str,
    media_type: str,
    source_path: Path,
    parse_fn: Any,
    validate_fn: Any,
) -> DatasetVerification:
    acquired = AcquiredDataset(
        dataset_type=dataset_type,
        source_system_code=source_system_code,
        tax_year=tax_year,
        original_filename=source_path.name,
        content=source_path.read_bytes(),
        media_type=media_type,
    )
    parse_result = parse_fn(config=config, acquired=acquired)
    staging_rows = [row.raw_payload for row in parse_result.staging_rows]
    findings = validate_fn(
        config=config,
        job_id="convert_2025_real_sources",
        tax_year=tax_year,
        dataset_type=dataset_type,
        staging_rows=staging_rows,
    )
    validation_error_count = sum(1 for finding in findings if finding.severity == "error")
    return DatasetVerification(
        county_id=county_id,
        dataset_type=dataset_type,
        row_count=len(staging_rows),
        parse_issue_count=len(parse_result.issues),
        validation_error_count=validation_error_count,
    )


def _open_sqlite(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA synchronous=OFF;")
    connection.execute("PRAGMA temp_store=MEMORY;")
    return connection


def _open_raw_text(path: Path):
    return path.open("r", encoding="utf-8-sig", errors="replace", newline="")


def _prepare_harris_lookup_tables(connection: sqlite3.Connection, raw_paths: HarrisRawPaths) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS harris_owner_lookup;
        DROP TABLE IF EXISTS harris_building_lookup;
        DROP TABLE IF EXISTS harris_land_lookup;
        CREATE TABLE harris_owner_lookup (
            acct TEXT PRIMARY KEY,
            owner_name TEXT
        );
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
        CREATE TABLE harris_land_lookup (
            acct TEXT PRIMARY KEY,
            land_sf REAL NOT NULL DEFAULT 0,
            land_acres REAL NOT NULL DEFAULT 0,
            land_value INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    _index_harris_owners(connection, raw_paths.owners)
    _index_harris_buildings(connection, raw_paths.building_res)
    _index_harris_land(connection, raw_paths.land)
    connection.commit()


def _prepare_fort_bend_lookup_tables(connection: sqlite3.Connection, raw_paths: FortBendRawPaths) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS fort_bend_owner_lookup;
        DROP TABLE IF EXISTS fort_bend_residential_lookup;
        DROP TABLE IF EXISTS fort_bend_exemption_lookup;
        CREATE TABLE fort_bend_owner_lookup (
            property_quick_ref_id TEXT PRIMARY KEY,
            ownership_pct REAL NOT NULL DEFAULT 0,
            owner_name TEXT,
            land_market_value INTEGER,
            improvement_market_value INTEGER,
            market_value INTEGER,
            assessed_value INTEGER,
            hs_cap_adj INTEGER,
            cbl_cap_adj INTEGER
        );
        CREATE TABLE fort_bend_residential_lookup (
            property_quick_ref_id TEXT PRIMARY KEY,
            bldg_sqft INTEGER NOT NULL DEFAULT 0,
            yr_built INTEGER,
            eff_yr_built INTEGER,
            bed_cnt INTEGER,
            bath_half INTEGER,
            quality_grade TEXT,
            condition_grade TEXT
        );
        CREATE TABLE fort_bend_exemption_lookup (
            owner_quick_ref_id TEXT PRIMARY KEY,
            has_hs INTEGER NOT NULL DEFAULT 0,
            has_ov65 INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    _index_fort_bend_owners(connection, raw_paths.owner_export)
    _index_fort_bend_residential_segments(connection, raw_paths.residential_segments)
    _index_fort_bend_exemptions(connection, raw_paths.exemption_export)
    connection.commit()


def _index_harris_owners(connection: sqlite3.Connection, source_path: Path) -> None:
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows: list[tuple[str, str]] = []
        for row in reader:
            if _strip(row.get("ln_num")) != "1":
                continue
            acct = _strip(row.get("acct"))
            if not acct:
                continue
            rows.append((acct, _strip(row.get("name")) or ""))
            if len(rows) >= 5000:
                connection.executemany(
                    "INSERT OR REPLACE INTO harris_owner_lookup (acct, owner_name) VALUES (?, ?)",
                    rows,
                )
                rows.clear()
        if rows:
            connection.executemany(
                "INSERT OR REPLACE INTO harris_owner_lookup (acct, owner_name) VALUES (?, ?)",
                rows,
            )


def _index_harris_buildings(connection: sqlite3.Connection, source_path: Path) -> None:
    update_sql = """
        INSERT INTO harris_building_lookup (
            acct,
            primary_area,
            living_area_sf,
            year_built,
            effective_year_built,
            effective_age,
            quality_code,
            condition_code,
            property_use_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(acct) DO UPDATE SET
            primary_area = excluded.primary_area,
            living_area_sf = excluded.living_area_sf,
            year_built = excluded.year_built,
            effective_year_built = excluded.effective_year_built,
            effective_age = excluded.effective_age,
            quality_code = excluded.quality_code,
            condition_code = excluded.condition_code,
            property_use_code = excluded.property_use_code
        WHERE excluded.primary_area > harris_building_lookup.primary_area
    """
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows: list[tuple[Any, ...]] = []
        for row in reader:
            acct = _strip(row.get("acct"))
            if not acct:
                continue
            primary_area = max(
                _as_float(row.get("heat_ar")),
                _as_float(row.get("im_sq_ft")),
                _as_float(row.get("gross_ar")),
                _as_float(row.get("eff_ar")),
                _as_float(row.get("act_ar")),
            )
            living_area = _as_int(row.get("heat_ar")) or _as_int(row.get("im_sq_ft")) or _as_int(
                row.get("gross_ar")
            )
            eff_year = _as_int(row.get("eff"))
            rows.append(
                (
                    acct,
                    primary_area,
                    living_area,
                    _as_int(row.get("date_erected")),
                    eff_year,
                    max(2025 - eff_year, 0) if eff_year else None,
                    _strip(row.get("qa_cd")) or None,
                    _normalize_label(_strip(row.get("dscr"))),
                    _strip(row.get("property_use_cd")) or None,
                )
            )
            if len(rows) >= 5000:
                connection.executemany(update_sql, rows)
                rows.clear()
        if rows:
            connection.executemany(update_sql, rows)


def _index_harris_land(connection: sqlite3.Connection, source_path: Path) -> None:
    update_sql = """
        INSERT INTO harris_land_lookup (acct, land_sf, land_acres, land_value)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(acct) DO UPDATE SET
            land_sf = harris_land_lookup.land_sf + excluded.land_sf,
            land_acres = harris_land_lookup.land_acres + excluded.land_acres,
            land_value = harris_land_lookup.land_value + excluded.land_value
    """
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows: list[tuple[str, float, float, int]] = []
        for row in reader:
            acct = _strip(row.get("acct"))
            if not acct:
                continue
            unit_type = (_strip(row.get("tp")) or "").upper()
            units = _as_float(row.get("uts"))
            land_sf = units if unit_type == "SF" else 0.0
            land_acres = units if unit_type == "AC" else (units / 43_560 if unit_type == "SF" else 0.0)
            land_value = _as_int(row.get("ovr_val")) or _as_int(row.get("val"))
            rows.append((acct, land_sf, land_acres, land_value or 0))
            if len(rows) >= 5000:
                connection.executemany(update_sql, rows)
                rows.clear()
        if rows:
            connection.executemany(update_sql, rows)


def _index_fort_bend_owners(connection: sqlite3.Connection, source_path: Path) -> None:
    update_sql = """
        INSERT INTO fort_bend_owner_lookup (
            property_quick_ref_id,
            ownership_pct,
            owner_name,
            land_market_value,
            improvement_market_value,
            market_value,
            assessed_value,
            hs_cap_adj,
            cbl_cap_adj
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(property_quick_ref_id) DO UPDATE SET
            ownership_pct = excluded.ownership_pct,
            owner_name = excluded.owner_name,
            land_market_value = excluded.land_market_value,
            improvement_market_value = excluded.improvement_market_value,
            market_value = excluded.market_value,
            assessed_value = excluded.assessed_value,
            hs_cap_adj = excluded.hs_cap_adj,
            cbl_cap_adj = excluded.cbl_cap_adj
        WHERE excluded.ownership_pct > fort_bend_owner_lookup.ownership_pct
    """
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows: list[tuple[Any, ...]] = []
        for row in reader:
            quick_ref = _strip(row.get("PropertyQuickRefID"))
            if not quick_ref:
                continue
            owner_name = _strip(row.get("CurrentOwnerName")) or _strip(row.get("OwnerName"))
            land_market_value = _sum_ints(row.get("LandHSValue"), row.get("LandNHSValue"))
            improvement_market_value = _sum_ints(row.get("ImpHSValue"), row.get("ImpNHSValue"))
            market_value = _sum_ints(
                row.get("LandHSValue"),
                row.get("LandNHSValue"),
                row.get("ImpHSValue"),
                row.get("ImpNHSValue"),
                row.get("AgMktValue"),
                row.get("TimMktValue"),
            )
            hs_cap_adj = _as_int(row.get("HSCapAdj"))
            cbl_cap_adj = _as_int(row.get("CBLCapAdj"))
            assessed_value = max(market_value - (hs_cap_adj or 0) - (cbl_cap_adj or 0), 0)
            rows.append(
                (
                    quick_ref,
                    _as_float(row.get("OwnershipPercentage")),
                    owner_name,
                    land_market_value,
                    improvement_market_value,
                    market_value,
                    assessed_value,
                    hs_cap_adj,
                    cbl_cap_adj,
                )
            )
            if len(rows) >= 5000:
                connection.executemany(update_sql, rows)
                rows.clear()
        if rows:
            connection.executemany(update_sql, rows)


def _index_fort_bend_residential_segments(connection: sqlite3.Connection, source_path: Path) -> None:
    summary: dict[str, dict[str, Any]] = {}
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            quick_ref = _strip(row.get("QuickRefID"))
            if not quick_ref:
                continue
            bucket = summary.setdefault(
                quick_ref,
                {
                    "bldg_sqft": 0,
                    "yr_built": None,
                    "eff_yr_built": None,
                    "bed_cnt": 0,
                    "bath_half": 0,
                    "quality_grade": None,
                    "condition_grade": None,
                },
            )
            bucket["bldg_sqft"] += _as_int(row.get("vTSGRSeg_AdjArea")) or _as_int(row.get("fArea")) or 0
            yr_built = _as_int(row.get("fActYear"))
            eff_yr_built = _as_int(row.get("fEffYear"))
            if yr_built and (bucket["yr_built"] is None or yr_built < bucket["yr_built"]):
                bucket["yr_built"] = yr_built
            if eff_yr_built and (bucket["eff_yr_built"] is None or eff_yr_built > bucket["eff_yr_built"]):
                bucket["eff_yr_built"] = eff_yr_built
            bucket["bed_cnt"] = max(bucket["bed_cnt"], _as_int(row.get("fBedrooms")) or 0)
            bucket["bath_half"] = max(bucket["bath_half"], _as_int(row.get("fNumHalfBath")) or 0)
            if bucket["quality_grade"] is None:
                bucket["quality_grade"] = _strip(row.get("fCDU")) or _strip(row.get("fSegClass")) or None
            if bucket["condition_grade"] is None:
                bucket["condition_grade"] = _strip(row.get("fCondition")) or None

    rows = [
        (
            quick_ref,
            bucket["bldg_sqft"] or 0,
            bucket["yr_built"],
            bucket["eff_yr_built"],
            bucket["bed_cnt"] or None,
            bucket["bath_half"] or None,
            bucket["quality_grade"],
            bucket["condition_grade"],
        )
        for quick_ref, bucket in summary.items()
    ]
    connection.executemany(
        """
        INSERT OR REPLACE INTO fort_bend_residential_lookup (
            property_quick_ref_id,
            bldg_sqft,
            yr_built,
            eff_yr_built,
            bed_cnt,
            bath_half,
            quality_grade,
            condition_grade
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _index_fort_bend_exemptions(connection: sqlite3.Connection, source_path: Path) -> None:
    aggregate: dict[str, dict[str, int]] = {}
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            quick_ref = _strip(row.get("OwnerQuickRefID"))
            if not quick_ref:
                continue
            bucket = aggregate.setdefault(quick_ref, {"has_hs": 0, "has_ov65": 0})
            code = (_strip(row.get("ExemptionCode")) or "").upper()
            if code == "HS":
                bucket["has_hs"] = 1
            if code in {"OV65", "DP65", "OA65"}:
                bucket["has_ov65"] = 1
    rows = [(quick_ref, bucket["has_hs"], bucket["has_ov65"]) for quick_ref, bucket in aggregate.items()]
    connection.executemany(
        "INSERT OR REPLACE INTO fort_bend_exemption_lookup (owner_quick_ref_id, has_hs, has_ov65) VALUES (?, ?, ?)",
        rows,
    )


def _build_harris_school_district_lookup(source_path: Path) -> dict[str, str]:
    lookup: dict[str, str] = {}
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            code = (_strip(row.get("tax_dist")) or "").zfill(3)
            name = _strip(row.get("name"))
            if not code or not name or not _looks_like_school_name(name):
                continue
            lookup.setdefault(code, name)
    return lookup


def _build_fort_bend_tax_entity_lookup(source_path: Path) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    with _open_raw_text(source_path) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code = _strip(row.get("Code"))
            name = _strip(row.get("Taxing Unit"))
            unit_type = _strip(row.get("Type"))
            if code and name and unit_type:
                lookup[code] = {"unit_name": name, "unit_type": unit_type}
    return lookup


def _write_harris_property_roll(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    raw_path: Path,
    school_district_lookup: dict[str, str],
    output_path: Path,
) -> int:
    owner_cursor = connection.cursor()
    building_cursor = connection.cursor()
    land_cursor = connection.cursor()
    row_count = 0
    with _open_raw_text(raw_path) as handle, output_path.open(
        "w", encoding="utf-8"
    ) as output:
        reader = csv.DictReader(handle, delimiter="\t")
        output.write("[")
        for row in reader:
            normalized = _build_harris_property_roll_row(
                tax_year=tax_year,
                row=row,
                owner_cursor=owner_cursor,
                building_cursor=building_cursor,
                land_cursor=land_cursor,
                school_district_lookup=school_district_lookup,
            )
            if normalized is None:
                continue
            if row_count:
                output.write(",")
            json.dump(normalized, output, separators=(",", ":"), sort_keys=True)
            row_count += 1
        output.write("]\n")
    return row_count


def _build_harris_property_roll_row(
    *,
    tax_year: int,
    row: dict[str, str],
    owner_cursor: sqlite3.Cursor,
    building_cursor: sqlite3.Cursor,
    land_cursor: sqlite3.Cursor,
    school_district_lookup: dict[str, str],
) -> dict[str, Any] | None:
    account_number = _strip(row.get("acct"))
    if not account_number:
        return None

    owner_cursor.execute("SELECT owner_name FROM harris_owner_lookup WHERE acct = ?", (account_number,))
    owner_row = owner_cursor.fetchone()
    building_cursor.execute(
        """
        SELECT living_area_sf, year_built, effective_year_built, effective_age, quality_code, condition_code, property_use_code
        FROM harris_building_lookup
        WHERE acct = ?
        """,
        (account_number,),
    )
    building_row = building_cursor.fetchone()
    land_cursor.execute(
        "SELECT land_sf, land_acres, land_value FROM harris_land_lookup WHERE acct = ?",
        (account_number,),
    )
    land_row = land_cursor.fetchone()

    school_code = (_strip(row.get("school_dist")) or "").zfill(3)
    school_name = school_district_lookup.get(school_code) or _strip(row.get("school_dist")) or None
    land_sf = _as_int(row.get("land_ar")) or _as_int(land_row["land_sf"]) if land_row else None
    land_acres = _as_float(row.get("acreage")) or _as_float(land_row["land_acres"]) if land_row else None
    land_value = _as_int(row.get("land_val")) or (_as_int(land_row["land_value"]) if land_row else None)
    x_features_value = _as_int(row.get("x_features_val")) or 0
    improvement_value = _sum_ints(row.get("bld_val"), x_features_value)
    market_value = _as_int(row.get("tot_mkt_val")) or _as_int(row.get("tot_appr_val")) or _sum_ints(
        land_value,
        improvement_value,
    )
    assessed_value = _as_int(row.get("assessed_val")) or _as_int(row.get("tot_appr_val")) or market_value
    appraised_value = _as_int(row.get("tot_appr_val")) or market_value
    certified_value = assessed_value if _strip(row.get("certified_date")) else None
    year_built = _row_value(building_row, "year_built")
    effective_year_built = _row_value(building_row, "effective_year_built")
    effective_age = _row_value(building_row, "effective_age")

    normalized = {
        "account_number": account_number,
        "cad_property_id": f"HCAD-{account_number}",
        "situs_address": _strip(row.get("site_addr_1")),
        "situs_city": _strip(row.get("site_addr_2")),
        "situs_zip": _strip(row.get("site_addr_3")),
        "owner_name": _normalize_placeholder_owner(
            _row_value(owner_row, "owner_name") or _strip(row.get("mailto"))
        ),
        "property_type_code": infer_harris_property_type_code(_strip(row.get("state_class"))),
        "property_class_code": _strip(row.get("state_class")),
        "neighborhood_code": _strip(row.get("Neighborhood_Code")),
        "subdivision_name": _join_nonempty([_strip(row.get("lgl_1")), _strip(row.get("lgl_2"))], sep=", "),
        "school_district_name": school_name,
        "living_area_sf": _row_value(building_row, "living_area_sf"),
        "year_built": year_built,
        "effective_year_built": effective_year_built,
        "effective_age": effective_age if effective_age is not None else (
            max(tax_year - effective_year_built, 0) if effective_year_built else None
        ),
        "quality_code": _row_value(building_row, "quality_code"),
        "condition_code": _row_value(building_row, "condition_code"),
        "land_sf": land_sf,
        "land_acres": land_acres,
        "land_value": land_value,
        "improvement_value": improvement_value,
        "market_value": market_value,
        "assessed_value": assessed_value,
        "notice_value": appraised_value,
        "appraised_value": appraised_value,
        "certified_value": certified_value,
        "prior_year_market_value": _as_int(row.get("prior_tot_mkt_val")),
        "prior_year_assessed_value": _as_int(row.get("prior_tot_appr_val")),
        "exemptions": [],
    }
    if not normalized["situs_address"] or not normalized["situs_city"] or not normalized["situs_zip"]:
        return None
    if normalized["market_value"] is None:
        return None
    return _compact_record(normalized)


def _write_harris_tax_rates(*, raw_path: Path, output_path: Path, tax_year: int) -> int:
    rows_by_code: dict[str, dict[str, Any]] = {}
    exemption_codes_by_unit: dict[str, Counter[str]] = {}
    with _open_raw_text(raw_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            unit_code = (_strip(row.get("tax_dist")) or "").zfill(3)
            if not unit_code:
                continue
            name = _strip(row.get("name"))
            rate_per_100 = _as_float(row.get("curr"))
            if not name or not rate_per_100:
                continue
            rows_by_code.setdefault(
                unit_code,
                {
                    "unit_type_code": infer_harris_tax_unit_type(name),
                    "unit_code": f"HC-{unit_code}",
                    "unit_name": name,
                    "rate_component": "ad_valorem",
                    "rate_value": round(rate_per_100 / 100, 8),
                    "rate_per_100": round(rate_per_100, 6),
                    "effective_from": f"{tax_year}-01-01",
                    "effective_to": None,
                    "aliases": [name],
                    "assignment_hints": build_harris_assignment_hints(name=name),
                    "metadata_json": {
                        "source_family": "harris_2025_jur_tax_dist_export",
                        "raw_tax_dist": unit_code,
                    },
                },
            )
            exemption_codes_by_unit.setdefault(unit_code, Counter())[_strip(row.get("exempt_cd")) or ""] += 1

    ordered_rows = [rows_by_code[key] for key in sorted(rows_by_code)]
    for key, row in rows_by_code.items():
        row["metadata_json"]["exemption_code_count"] = sum(exemption_codes_by_unit.get(key, Counter()).values())

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(ordered_rows, handle, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
    return len(ordered_rows)


def _write_fort_bend_property_roll(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    raw_path: Path,
    entity_lookup: dict[str, dict[str, str]],
    output_path: Path,
) -> int:
    owner_cursor = connection.cursor()
    residential_cursor = connection.cursor()
    exemption_cursor = connection.cursor()
    row_count = 0
    with _open_raw_text(raw_path) as handle, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as output:
        reader = csv.DictReader(handle)
        writer = csv.DictWriter(output, fieldnames=FORT_BEND_PROPERTY_HEADERS)
        writer.writeheader()
        for row in reader:
            normalized = _build_fort_bend_property_roll_row(
                tax_year=tax_year,
                row=row,
                entity_lookup=entity_lookup,
                owner_cursor=owner_cursor,
                residential_cursor=residential_cursor,
                exemption_cursor=exemption_cursor,
            )
            if normalized is None:
                continue
            writer.writerow(normalized)
            row_count += 1
    return row_count


def _build_fort_bend_property_roll_row(
    *,
    tax_year: int,
    row: dict[str, str],
    entity_lookup: dict[str, dict[str, str]],
    owner_cursor: sqlite3.Cursor,
    residential_cursor: sqlite3.Cursor,
    exemption_cursor: sqlite3.Cursor,
) -> dict[str, str] | None:
    property_quick_ref_id = _strip(row.get("PropertyQuickRefID"))
    account_id = _strip(row.get("PropertyNumber"))
    if not property_quick_ref_id or not account_id:
        return None

    owner_cursor.execute(
        """
        SELECT owner_name, land_market_value, improvement_market_value, market_value, assessed_value
        FROM fort_bend_owner_lookup
        WHERE property_quick_ref_id = ?
        """,
        (property_quick_ref_id,),
    )
    owner_row = owner_cursor.fetchone()
    residential_cursor.execute(
        """
        SELECT bldg_sqft, yr_built, eff_yr_built, bed_cnt, bath_half, quality_grade, condition_grade
        FROM fort_bend_residential_lookup
        WHERE property_quick_ref_id = ?
        """,
        (property_quick_ref_id,),
    )
    residential_row = residential_cursor.fetchone()
    exemption_cursor.execute(
        "SELECT has_hs, has_ov65 FROM fort_bend_exemption_lookup WHERE owner_quick_ref_id = ?",
        (property_quick_ref_id,),
    )
    exemption_row = exemption_cursor.fetchone()

    acres = _as_float(row.get("Acres"))
    land_sqft = round(acres * 43_560) if acres else None
    school_district = _lookup_entity_name(
        entities=_split_codes(row.get("Entities")),
        entity_lookup=entity_lookup,
        wanted_types={"School"},
    )
    market_value = _row_value(owner_row, "market_value")
    assessed_value = _row_value(owner_row, "assessed_value") or market_value
    eff_yr_built = _row_value(residential_row, "eff_yr_built")

    normalized = {
        "account_id": account_id,
        "property_id": property_quick_ref_id,
        "site_address": _strip(row.get("SitusStreetAddress")),
        "site_city": _strip(row.get("SitusCity")),
        "site_zip": _strip(row.get("SitusZip")),
        "owner_name": _row_value(owner_row, "owner_name"),
        "class_cd": _strip(row.get("StateCodeImp")) or _strip(row.get("StateCodeLand")) or _strip(
            row.get("PropertyTypeCode")
        ),
        "neighborhood_cd": _strip(row.get("AbstractCode")),
        "subdivision": _strip(row.get("LegalDesc")),
        "school_district": school_district,
        "bldg_sqft": _stringify_optional(_row_value(residential_row, "bldg_sqft")),
        "yr_built": _stringify_optional(_row_value(residential_row, "yr_built")),
        "eff_yr_built": _stringify_optional(eff_yr_built),
        "eff_age": _stringify_optional(max(tax_year - eff_yr_built, 0) if eff_yr_built else None),
        "bed_cnt": _stringify_optional(_row_value(residential_row, "bed_cnt")),
        "bath_full": "",
        "bath_half": _stringify_optional(_row_value(residential_row, "bath_half")),
        "story_count": "",
        "quality_grade": _row_value(residential_row, "quality_grade") or "",
        "condition_grade": _row_value(residential_row, "condition_grade") or "",
        "garage_capacity": "",
        "pool_ind": "",
        "land_sqft": _stringify_optional(land_sqft),
        "land_acres": _stringify_optional_float(acres),
        "land_market_value": _stringify_optional(_row_value(owner_row, "land_market_value")),
        "improvement_market_value": _stringify_optional(_row_value(owner_row, "improvement_market_value")),
        "market_value": _stringify_optional(market_value),
        "assessed_value": _stringify_optional(assessed_value),
        "notice_value": _stringify_optional(market_value),
        "appraised_value": _stringify_optional(market_value),
        "certified_value": _stringify_optional(assessed_value),
        "prior_market_value": "",
        "prior_assessed_value": "",
        # ExemptionExport confirms presence but does not include numeric exemption amounts.
        "hs_amt": "",
        "ov65_amt": "",
    }
    if exemption_row and not school_district:
        normalized["school_district"] = ""
    if not normalized["site_address"] or not normalized["site_city"] or not normalized["site_zip"]:
        return None
    if not normalized["market_value"]:
        return None
    return normalized


def _write_fort_bend_tax_rates(*, raw_path: Path, output_path: Path, tax_year: int) -> int:
    rows: list[dict[str, str]] = []
    with _open_raw_text(raw_path) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            unit_code = _strip(row.get("Code"))
            unit_name = _strip(row.get("Taxing Unit"))
            source_type = _strip(row.get("Type"))
            rate_per_100 = _as_float(row.get("Total Rate"))
            if not unit_code or not unit_name or not source_type or not rate_per_100:
                continue
            unit_type_code = infer_fort_bend_tax_unit_type(source_type)
            rows.append(
                {
                    "unit_type_code": unit_type_code,
                    "unit_code": unit_code,
                    "unit_name": unit_name,
                    "rate_component": "ad_valorem",
                    "rate_value": _stringify_optional_float(round(rate_per_100 / 100, 8)),
                    "rate_per_100": _stringify_optional_float(round(rate_per_100, 7)),
                    "effective_from": f"{tax_year}-01-01",
                    "effective_to": "",
                    "cities": fort_bend_tax_rate_cities(unit_type_code=unit_type_code, unit_name=unit_name),
                    "school_district_names": unit_name if unit_type_code == "school" else "",
                    "subdivisions": "",
                    "zip_codes": "",
                    "account_numbers": "",
                    "aliases": unit_name,
                    "priority": str(fort_bend_priority(unit_type_code)),
                    "allow_multiple": "true" if unit_type_code in {"mud", "special"} else "false",
                }
            )

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FORT_BEND_TAX_RATE_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def infer_harris_property_type_code(state_class_code: str | None) -> str:
    code = (state_class_code or "").strip().upper()
    if code.startswith("A"):
        return "sfr"
    if code.startswith("B"):
        return "multifamily"
    if code.startswith("C"):
        return "vacant"
    if code.startswith(("J", "L", "O")):
        return "commercial"
    return "real_property"


def infer_harris_tax_unit_type(name: str) -> str:
    upper_name = name.upper()
    if "COUNTY" in upper_name and "HARRIS" in upper_name:
        return "county"
    if _looks_like_school_name(upper_name):
        return "school"
    if upper_name.startswith("CITY OF ") or " VILLAGE" in upper_name or upper_name in {
        "HOUSTON",
        "BUNKER HILL",
        "HILSHIRE VILLAGE",
        "HUNTERS CREEK VILLAGE",
        "JERSEY VILLAGE",
        "PINEY POINT VILLAGE",
        "SEABROOK",
        "SOUTH HOUSTON",
        "TAYLOR LAKE VILLAGE",
        "WEBSTER",
        "WEST UNIVERSITY PLACE",
    }:
        return "city"
    if any(token in upper_name for token in [" MUD", "WCID", "WATER DIST", "UTILITY DIST", " UD ", " PUD"]):
        return "mud"
    return "special"


def build_harris_assignment_hints(*, name: str) -> dict[str, Any]:
    unit_type_code = infer_harris_tax_unit_type(name)
    if unit_type_code == "county":
        return {"county_ids": ["harris"], "priority": 110}
    if unit_type_code == "school":
        return {"school_district_names": [name], "priority": 100}
    if unit_type_code == "city":
        city_name = name.removeprefix("CITY OF ").strip()
        return {"cities": [city_name], "priority": 90}
    return {"priority": 70}


def infer_fort_bend_tax_unit_type(source_type: str) -> str:
    normalized = source_type.strip().lower()
    if normalized == "county general":
        return "county"
    if normalized == "city":
        return "city"
    if normalized in {"school", "community college"}:
        return "school"
    if normalized in {"municipal utility", "fresh water", "levy improvement", "municipal"}:
        return "mud"
    return "special"


def fort_bend_tax_rate_cities(*, unit_type_code: str, unit_name: str) -> str:
    if unit_type_code == "county":
        return "fort_bend"
    if unit_type_code == "city":
        return unit_name.replace("City of ", "")
    return ""


def fort_bend_priority(unit_type_code: str) -> int:
    priorities = {
        "county": 110,
        "school": 100,
        "city": 90,
        "mud": 80,
        "special": 70,
    }
    return priorities[unit_type_code]


def _require_files(paths: Iterable[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required raw file(s): {', '.join(missing)}")


def _split_codes(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _lookup_entity_name(
    *,
    entities: list[str],
    entity_lookup: dict[str, dict[str, str]],
    wanted_types: set[str],
) -> str:
    for code in entities:
        entity = entity_lookup.get(code)
        if entity and entity["unit_type"] in wanted_types:
            return entity["unit_name"]
    return ""


def _looks_like_school_name(value: str) -> bool:
    upper_value = value.upper()
    return any(token in upper_value for token in [" ISD", " CISD", " USD", " SCHOOL DISTRICT"])


def _join_nonempty(values: Iterable[str | None], *, sep: str) -> str | None:
    cleaned = [value for value in values if value]
    return sep.join(cleaned) if cleaned else None


def _normalize_label(value: str | None) -> str | None:
    if not value:
        return None
    return value.upper().replace(" ", "_")


def _normalize_placeholder_owner(value: str | None) -> str | None:
    if not value:
        return None
    if value.upper() == "CURRENT OWNER":
        return None
    return value


def _compact_record(record: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    for key, value in record.items():
        if value is None:
            continue
        if value == "" and key != "situs_address":
            continue
        compacted[key] = value
    if "exemptions" not in compacted:
        compacted["exemptions"] = []
    return compacted


def _row_value(row: sqlite3.Row | None, key: str) -> Any:
    if row is None:
        return None
    return row[key]


def _stringify_optional(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _stringify_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text


def _strip(value: Any) -> str:
    return str(value or "").strip()


def _as_int(value: Any) -> int | None:
    raw = _strip(value)
    if raw == "":
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _as_float(value: Any) -> float | None:
    raw = _strip(value)
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _sum_ints(*values: Any) -> int:
    total = 0
    for value in values:
        coerced = value if isinstance(value, int) else _as_int(value)
        if coerced is not None:
            total += coerced
    return total


if __name__ == "__main__":
    main()
