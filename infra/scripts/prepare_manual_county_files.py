from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from app.county_adapters.common.base import AcquiredDataset
from app.county_adapters.common.config_loader import load_county_adapter_config
from app.county_adapters.fort_bend.parse import parse_raw_to_staging as parse_fort_bend
from app.county_adapters.fort_bend.validation import validate_property_roll as validate_fb_property_roll
from app.county_adapters.fort_bend.validation import validate_tax_rates as validate_fb_tax_rates
from app.county_adapters.harris.parse import parse_raw_to_staging as parse_harris
from app.county_adapters.harris.validation import validate_property_roll as validate_harris_property_roll
from app.county_adapters.harris.validation import validate_tax_rates as validate_harris_tax_rates

csv.field_size_limit(10_000_000)

COUNTY_CHOICES = ("harris", "fort_bend", "both")
DATASET_TYPE_CHOICES = ("property_roll", "tax_rates", "both")

HARRIS_PROPERTY_ROLL_FILENAME_TEMPLATE = "harris_property_roll_{tax_year}.json"
HARRIS_TAX_RATES_FILENAME_TEMPLATE = "harris_tax_rates_{tax_year}.json"
FORT_BEND_PROPERTY_ROLL_FILENAME_TEMPLATE = "fort_bend_property_roll_{tax_year}.csv"
FORT_BEND_TAX_RATES_FILENAME_TEMPLATE = "fort_bend_tax_rates_{tax_year}.csv"

HARRIS_RAW_OVERRIDE_KEYS = {
    "real_acct",
    "owners",
    "building_res",
    "land",
    "tax_rates",
}
FORT_BEND_RAW_OVERRIDE_KEYS = {
    "property_export",
    "owner_export",
    "exemption_export",
    "residential_segments",
    "tax_rates",
}
RAW_OVERRIDE_KEYS = {
    "harris": HARRIS_RAW_OVERRIDE_KEYS,
    "fort_bend": FORT_BEND_RAW_OVERRIDE_KEYS,
}

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


@dataclass(frozen=True)
class RawFileDetails:
    logical_name: str
    path: str
    size_bytes: int
    checksum_sha256: str


@dataclass(frozen=True)
class OutputFileDetails:
    path: str
    size_bytes: int
    checksum_sha256: str
    row_count: int


@dataclass(frozen=True)
class ManifestSummary:
    county_id: str
    tax_year: int
    dataset_type: str
    raw_files: list[RawFileDetails]
    output_files: list[OutputFileDetails]
    validation_status: str
    parse_issue_count: int
    validation_error_count: int


@dataclass(frozen=True)
class PreparedDataset:
    county_id: str
    tax_year: int
    dataset_type: str
    output_path: Path
    manifest_path: Path
    row_count: int
    verification: DatasetVerification | None
    raw_files: list[RawFileDetails]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare manually downloaded Harris and Fort Bend county raw files into adapter-ready "
            "property_roll and tax_rates files for historical backfill."
        )
    )
    parser.add_argument("--county-id", choices=COUNTY_CHOICES, required=True)
    parser.add_argument("--tax-year", type=int, required=True)
    parser.add_argument("--dataset-type", choices=DATASET_TYPE_CHOICES, required=True)
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=None,
        help=(
            "Year-scoped raw root. Canonical layout expects files under "
            "~/county-data/<tax_year>/raw/<county_id>/..."
        ),
    )
    parser.add_argument(
        "--ready-root",
        type=Path,
        default=None,
        help=(
            "Directory where adapter-ready outputs and manifests will be written. "
            "Defaults to ~/county-data/<tax_year>/ready."
        ),
    )
    parser.add_argument(
        "--raw-file-override",
        dest="raw_file_overrides",
        action="append",
        default=[],
        metavar="COUNTY.KEY=PATH",
        help=(
            "Override a raw source path when the downloaded filename differs from the canonical local "
            "contract. Example: --raw-file-override harris.real_acct=/tmp/hcad_real_acct.txt"
        ),
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip adapter parse/validation verification for generated adapter-ready outputs.",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    tax_year = args.tax_year
    raw_root = (
        args.raw_root.expanduser().resolve()
        if args.raw_root is not None
        else (Path.home() / "county-data" / str(tax_year) / "raw").resolve()
    )
    ready_root = (
        args.ready_root.expanduser().resolve()
        if args.ready_root is not None
        else (Path.home() / "county-data" / str(tax_year) / "ready").resolve()
    )
    ready_root.mkdir(parents=True, exist_ok=True)

    counties = expand_county_ids(args.county_id)
    dataset_types = expand_dataset_types(args.dataset_type)
    overrides = parse_raw_file_overrides(args.raw_file_overrides)
    results = prepare_manual_county_files(
        county_ids=counties,
        tax_year=tax_year,
        dataset_types=dataset_types,
        raw_root=raw_root,
        ready_root=ready_root,
        raw_file_overrides=overrides,
        skip_verify=args.skip_verify,
    )

    print("Prepared adapter-ready files:")
    for result in results:
        print(
            f"- {result.county_id}/{result.dataset_type}: {result.output_path} "
            f"(rows={result.row_count}, manifest={result.manifest_path})"
        )

    failures = [
        result
        for result in results
        if result.verification is not None
        and (result.verification.parse_issue_count > 0 or result.verification.validation_error_count > 0)
    ]
    if failures:
        raise SystemExit("One or more generated files failed adapter parse/validation checks.")


def expand_county_ids(value: str) -> list[str]:
    if value == "both":
        return ["harris", "fort_bend"]
    return [value]


def expand_dataset_types(value: str) -> list[str]:
    if value == "both":
        return ["property_roll", "tax_rates"]
    return [value]


def parse_raw_file_overrides(values: Sequence[str]) -> dict[str, dict[str, Path]]:
    overrides: dict[str, dict[str, Path]] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(
                "Invalid --raw-file-override value. Expected COUNTY.KEY=PATH, "
                f"received: {value}"
            )
        scoped_key, raw_path = value.split("=", 1)
        if "." not in scoped_key:
            raise SystemExit(
                "Invalid --raw-file-override key. Expected COUNTY.KEY so overrides stay "
                f"unambiguous across counties, received: {scoped_key}"
            )
        county_id, logical_name = scoped_key.split(".", 1)
        if county_id not in RAW_OVERRIDE_KEYS:
            raise SystemExit(
                f"Unsupported override county '{county_id}'. Expected one of: {', '.join(sorted(RAW_OVERRIDE_KEYS))}."
            )
        if logical_name not in RAW_OVERRIDE_KEYS[county_id]:
            supported_keys = ", ".join(sorted(f"{county_id}.{key}" for key in RAW_OVERRIDE_KEYS[county_id]))
            raise SystemExit(
                f"Unsupported override key '{scoped_key}'. Supported keys: {supported_keys}."
            )
        overrides.setdefault(county_id, {})[logical_name] = Path(raw_path).expanduser().resolve()
    return overrides


def prepare_manual_county_files(
    *,
    county_ids: Sequence[str],
    tax_year: int,
    dataset_types: Sequence[str],
    raw_root: Path,
    ready_root: Path,
    raw_file_overrides: dict[str, dict[str, Path]] | None = None,
    skip_verify: bool = False,
) -> list[PreparedDataset]:
    raw_file_overrides = raw_file_overrides or {}
    ready_root.mkdir(parents=True, exist_ok=True)
    _validate_requested_tax_years(county_ids=county_ids, dataset_types=dataset_types, tax_year=tax_year)

    outputs = resolve_outputs(ready_root=ready_root, tax_year=tax_year)
    prepared: list[PreparedDataset] = []
    with tempfile.NamedTemporaryFile(
        prefix=f"dwellio-manual-prep-{tax_year}-",
        suffix=".sqlite3",
        delete=True,
    ) as handle:
        connection = _open_sqlite(Path(handle.name))
        try:
            for county_id in county_ids:
                if county_id == "harris":
                    prepared.extend(
                        prepare_harris(
                            connection=connection,
                            tax_year=tax_year,
                            dataset_types=dataset_types,
                            raw_root=raw_root,
                            ready_root=ready_root,
                            outputs=outputs,
                            raw_file_overrides=raw_file_overrides.get("harris", {}),
                            skip_verify=skip_verify,
                        )
                    )
                    continue
                if county_id == "fort_bend":
                    prepared.extend(
                        prepare_fort_bend(
                            connection=connection,
                            tax_year=tax_year,
                            dataset_types=dataset_types,
                            raw_root=raw_root,
                            ready_root=ready_root,
                            outputs=outputs,
                            raw_file_overrides=raw_file_overrides.get("fort_bend", {}),
                            skip_verify=skip_verify,
                        )
                    )
                    continue
                raise ValueError(f"Unsupported county_id={county_id}.")
        finally:
            connection.close()
    return prepared


def prepare_harris(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    dataset_types: Sequence[str],
    raw_root: Path,
    ready_root: Path,
    outputs: ConversionOutputs,
    raw_file_overrides: dict[str, Path],
    skip_verify: bool,
) -> list[PreparedDataset]:
    raw_paths = resolve_harris_paths(raw_root=raw_root, tax_year=tax_year, overrides=raw_file_overrides)
    results: list[PreparedDataset] = []

    if "property_roll" in dataset_types:
        property_inputs = [
            ("real_acct", raw_paths.real_acct),
            ("owners", raw_paths.owners),
            ("building_res", raw_paths.building_res),
            ("land", raw_paths.land),
            ("tax_rates", raw_paths.tax_rates),
        ]
        _require_named_files("harris", "property_roll", property_inputs)
        _prepare_harris_lookup_tables(connection, raw_paths=raw_paths, tax_year=tax_year)
        school_lookup = _build_harris_school_district_lookup(raw_paths.tax_rates)
        row_count = _write_harris_property_roll(
            connection=connection,
            tax_year=tax_year,
            raw_path=raw_paths.real_acct,
            school_district_lookup=school_lookup,
            output_path=outputs.harris_property_roll,
        )
        verification = None
        if not skip_verify:
            verification = verify_dataset_output(
                county_id="harris",
                dataset_type="property_roll",
                tax_year=tax_year,
                source_path=outputs.harris_property_roll,
            )
        manifest_path = write_manifest(
            ready_root=ready_root,
            county_id="harris",
            dataset_type="property_roll",
            tax_year=tax_year,
            raw_inputs=property_inputs,
            output_path=outputs.harris_property_roll,
            row_count=row_count,
            verification=verification,
        )
        results.append(
            PreparedDataset(
                county_id="harris",
                tax_year=tax_year,
                dataset_type="property_roll",
                output_path=outputs.harris_property_roll,
                manifest_path=manifest_path,
                row_count=row_count,
                verification=verification,
                raw_files=[build_raw_file_details(logical_name=name, path=path) for name, path in property_inputs],
            )
        )

    if "tax_rates" in dataset_types:
        tax_inputs = [("tax_rates", raw_paths.tax_rates)]
        _require_named_files("harris", "tax_rates", tax_inputs)
        row_count = _write_harris_tax_rates(
            raw_path=raw_paths.tax_rates,
            output_path=outputs.harris_tax_rates,
            tax_year=tax_year,
        )
        verification = None
        if not skip_verify:
            verification = verify_dataset_output(
                county_id="harris",
                dataset_type="tax_rates",
                tax_year=tax_year,
                source_path=outputs.harris_tax_rates,
            )
        manifest_path = write_manifest(
            ready_root=ready_root,
            county_id="harris",
            dataset_type="tax_rates",
            tax_year=tax_year,
            raw_inputs=tax_inputs,
            output_path=outputs.harris_tax_rates,
            row_count=row_count,
            verification=verification,
        )
        results.append(
            PreparedDataset(
                county_id="harris",
                tax_year=tax_year,
                dataset_type="tax_rates",
                output_path=outputs.harris_tax_rates,
                manifest_path=manifest_path,
                row_count=row_count,
                verification=verification,
                raw_files=[build_raw_file_details(logical_name=name, path=path) for name, path in tax_inputs],
            )
        )

    return results


def prepare_fort_bend(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    dataset_types: Sequence[str],
    raw_root: Path,
    ready_root: Path,
    outputs: ConversionOutputs,
    raw_file_overrides: dict[str, Path],
    skip_verify: bool,
) -> list[PreparedDataset]:
    raw_paths = resolve_fort_bend_paths(raw_root=raw_root, tax_year=tax_year, overrides=raw_file_overrides)
    results: list[PreparedDataset] = []

    if "property_roll" in dataset_types:
        property_inputs = [
            ("property_export", raw_paths.property_export),
            ("owner_export", raw_paths.owner_export),
            ("exemption_export", raw_paths.exemption_export),
            ("residential_segments", raw_paths.residential_segments),
            ("tax_rates", raw_paths.tax_rates),
        ]
        _require_named_files("fort_bend", "property_roll", property_inputs)
        entity_lookup = _build_fort_bend_tax_entity_lookup(raw_paths.tax_rates)
        _prepare_fort_bend_lookup_tables(connection, raw_paths)
        row_count = _write_fort_bend_property_roll(
            connection=connection,
            tax_year=tax_year,
            raw_path=raw_paths.property_export,
            entity_lookup=entity_lookup,
            output_path=outputs.fort_bend_property_roll,
        )
        verification = None
        if not skip_verify:
            verification = verify_dataset_output(
                county_id="fort_bend",
                dataset_type="property_roll",
                tax_year=tax_year,
                source_path=outputs.fort_bend_property_roll,
            )
        manifest_path = write_manifest(
            ready_root=ready_root,
            county_id="fort_bend",
            dataset_type="property_roll",
            tax_year=tax_year,
            raw_inputs=property_inputs,
            output_path=outputs.fort_bend_property_roll,
            row_count=row_count,
            verification=verification,
        )
        results.append(
            PreparedDataset(
                county_id="fort_bend",
                tax_year=tax_year,
                dataset_type="property_roll",
                output_path=outputs.fort_bend_property_roll,
                manifest_path=manifest_path,
                row_count=row_count,
                verification=verification,
                raw_files=[build_raw_file_details(logical_name=name, path=path) for name, path in property_inputs],
            )
        )

    if "tax_rates" in dataset_types:
        tax_inputs = [("tax_rates", raw_paths.tax_rates)]
        _require_named_files("fort_bend", "tax_rates", tax_inputs)
        row_count = _write_fort_bend_tax_rates(
            raw_path=raw_paths.tax_rates,
            output_path=outputs.fort_bend_tax_rates,
            tax_year=tax_year,
        )
        verification = None
        if not skip_verify:
            verification = verify_dataset_output(
                county_id="fort_bend",
                dataset_type="tax_rates",
                tax_year=tax_year,
                source_path=outputs.fort_bend_tax_rates,
            )
        manifest_path = write_manifest(
            ready_root=ready_root,
            county_id="fort_bend",
            dataset_type="tax_rates",
            tax_year=tax_year,
            raw_inputs=tax_inputs,
            output_path=outputs.fort_bend_tax_rates,
            row_count=row_count,
            verification=verification,
        )
        results.append(
            PreparedDataset(
                county_id="fort_bend",
                tax_year=tax_year,
                dataset_type="tax_rates",
                output_path=outputs.fort_bend_tax_rates,
                manifest_path=manifest_path,
                row_count=row_count,
                verification=verification,
                raw_files=[build_raw_file_details(logical_name=name, path=path) for name, path in tax_inputs],
            )
        )

    return results


def _validate_requested_tax_years(
    *,
    county_ids: Sequence[str],
    dataset_types: Sequence[str],
    tax_year: int,
) -> None:
    unsupported: list[str] = []
    for county_id in county_ids:
        config = load_county_adapter_config(county_id)
        for dataset_type in dataset_types:
            supported_years = config.dataset_configs[dataset_type].supported_years
            if tax_year not in supported_years:
                unsupported.append(
                    f"{county_id}/{dataset_type} supports {supported_years}, not {tax_year}"
                )
    if unsupported:
        raise SystemExit(
            "Unsupported tax year for manual prep: " + "; ".join(unsupported)
        )


def resolve_harris_paths(
    raw_root: Path,
    tax_year: int,
    overrides: dict[str, Path] | None = None,
) -> HarrisRawPaths:
    overrides = overrides or {}
    return HarrisRawPaths(
        real_acct=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="harris",
            logical_name="real_acct",
            canonical_filename="real_acct.txt",
            tax_year=tax_year,
            override=overrides.get("real_acct"),
            legacy_relative_paths=[
                Path(f"{tax_year} Harris_Real_acct_owner") / "real_acct.txt",
            ],
        ),
        owners=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="harris",
            logical_name="owners",
            canonical_filename="owners.txt",
            tax_year=tax_year,
            override=overrides.get("owners"),
            legacy_relative_paths=[
                Path(f"{tax_year} Harris_Real_acct_owner") / "owners.txt",
            ],
        ),
        building_res=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="harris",
            logical_name="building_res",
            canonical_filename="building_res.txt",
            tax_year=tax_year,
            override=overrides.get("building_res"),
            legacy_relative_paths=[
                Path(f"{tax_year} Harris_Real_building_land") / "building_res.txt",
            ],
        ),
        land=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="harris",
            logical_name="land",
            canonical_filename="land.txt",
            tax_year=tax_year,
            override=overrides.get("land"),
            legacy_relative_paths=[
                Path(f"{tax_year} Harris_Real_building_land") / "land.txt",
            ],
        ),
        tax_rates=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="harris",
            logical_name="tax_rates",
            canonical_filename="jur_tax_dist_exempt_value_rate.txt",
            tax_year=tax_year,
            override=overrides.get("tax_rates"),
            legacy_relative_paths=[
                Path(f"{tax_year} Harris Roll Source_Real_jur_exempt")
                / "jur_tax_dist_exempt_value_rate.txt",
            ],
        ),
    )


def resolve_fort_bend_paths(
    raw_root: Path,
    tax_year: int,
    overrides: dict[str, Path] | None = None,
) -> FortBendRawPaths:
    overrides = overrides or {}
    return FortBendRawPaths(
        property_export=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="fort_bend",
            logical_name="property_export",
            canonical_filename="PropertyExport.txt",
            tax_year=tax_year,
            override=overrides.get("property_export"),
            legacy_relative_paths=[
                Path(f"{tax_year} Fort Bend_Certified Export-EXTRACTED")
                / _legacy_fort_bend_export_filename(tax_year=tax_year, suffix="PropertyExport.txt"),
            ],
        ),
        owner_export=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="fort_bend",
            logical_name="owner_export",
            canonical_filename="OwnerExport.txt",
            tax_year=tax_year,
            override=overrides.get("owner_export"),
            legacy_relative_paths=[
                Path(f"{tax_year} Fort Bend_Certified Export-EXTRACTED")
                / _legacy_fort_bend_export_filename(tax_year=tax_year, suffix="OwnerExport.txt"),
            ],
        ),
        exemption_export=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="fort_bend",
            logical_name="exemption_export",
            canonical_filename="ExemptionExport.txt",
            tax_year=tax_year,
            override=overrides.get("exemption_export"),
            legacy_relative_paths=[
                Path(f"{tax_year} Fort Bend_Certified Export-EXTRACTED")
                / _legacy_fort_bend_export_filename(tax_year=tax_year, suffix="ExemptionExport.txt"),
            ],
        ),
        residential_segments=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="fort_bend",
            logical_name="residential_segments",
            canonical_filename="WebsiteResidentialSegs.csv",
            tax_year=tax_year,
            override=overrides.get("residential_segments"),
            legacy_relative_paths=[
                Path("WebsiteResidentialSegs-7-22.csv"),
            ],
        ),
        tax_rates=_resolve_raw_file_path(
            raw_root=raw_root,
            county_id="fort_bend",
            logical_name="tax_rates",
            canonical_filename="Fort Bend Tax Rate Source.csv",
            tax_year=tax_year,
            override=overrides.get("tax_rates"),
            legacy_relative_paths=[
                Path(f"{tax_year} Fort Bend Tax Rate Source.csv"),
            ],
        ),
    )


def _resolve_raw_file_path(
    *,
    raw_root: Path,
    county_id: str,
    logical_name: str,
    canonical_filename: str,
    tax_year: int,
    override: Path | None,
    legacy_relative_paths: Sequence[Path],
) -> Path:
    if override is not None:
        return override.expanduser().resolve()

    county_root = raw_root / county_id
    candidates = [
        county_root / canonical_filename,
        raw_root / canonical_filename,
    ]
    candidates.extend(raw_root / relative_path for relative_path in legacy_relative_paths)
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def _legacy_fort_bend_export_filename(*, tax_year: int, suffix: str) -> str:
    return f"{tax_year}_07_17_1800_{suffix}"


def resolve_outputs(ready_root: Path, tax_year: int = 2025) -> ConversionOutputs:
    return ConversionOutputs(
        harris_property_roll=ready_root / HARRIS_PROPERTY_ROLL_FILENAME_TEMPLATE.format(tax_year=tax_year),
        harris_tax_rates=ready_root / HARRIS_TAX_RATES_FILENAME_TEMPLATE.format(tax_year=tax_year),
        fort_bend_property_roll=ready_root / FORT_BEND_PROPERTY_ROLL_FILENAME_TEMPLATE.format(tax_year=tax_year),
        fort_bend_tax_rates=ready_root / FORT_BEND_TAX_RATES_FILENAME_TEMPLATE.format(tax_year=tax_year),
    )


def convert_harris(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    raw_paths: HarrisRawPaths,
    property_roll_output: Path,
    tax_rates_output: Path,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    property_inputs = [
        ("real_acct", raw_paths.real_acct),
        ("owners", raw_paths.owners),
        ("building_res", raw_paths.building_res),
        ("land", raw_paths.land),
        ("tax_rates", raw_paths.tax_rates),
    ]
    _require_named_files("harris", "property_roll", property_inputs)
    _prepare_harris_lookup_tables(connection, raw_paths=raw_paths, tax_year=tax_year)
    school_district_lookup = _build_harris_school_district_lookup(raw_paths.tax_rates)
    counts["property_roll"] = _write_harris_property_roll(
        connection=connection,
        tax_year=tax_year,
        raw_path=raw_paths.real_acct,
        school_district_lookup=school_district_lookup,
        output_path=property_roll_output,
    )
    _require_named_files("harris", "tax_rates", [("tax_rates", raw_paths.tax_rates)])
    counts["tax_rates"] = _write_harris_tax_rates(
        raw_path=raw_paths.tax_rates,
        output_path=tax_rates_output,
        tax_year=tax_year,
    )
    return counts


def convert_fort_bend(
    *,
    connection: sqlite3.Connection,
    tax_year: int,
    raw_paths: FortBendRawPaths,
    property_roll_output: Path,
    tax_rates_output: Path,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    property_inputs = [
        ("property_export", raw_paths.property_export),
        ("owner_export", raw_paths.owner_export),
        ("exemption_export", raw_paths.exemption_export),
        ("residential_segments", raw_paths.residential_segments),
        ("tax_rates", raw_paths.tax_rates),
    ]
    _require_named_files("fort_bend", "property_roll", property_inputs)
    entity_lookup = _build_fort_bend_tax_entity_lookup(raw_paths.tax_rates)
    _prepare_fort_bend_lookup_tables(connection, raw_paths)
    counts["property_roll"] = _write_fort_bend_property_roll(
        connection=connection,
        tax_year=tax_year,
        raw_path=raw_paths.property_export,
        entity_lookup=entity_lookup,
        output_path=property_roll_output,
    )
    _require_named_files("fort_bend", "tax_rates", [("tax_rates", raw_paths.tax_rates)])
    counts["tax_rates"] = _write_fort_bend_tax_rates(
        raw_path=raw_paths.tax_rates,
        output_path=tax_rates_output,
        tax_year=tax_year,
    )
    return counts


def verify_outputs(
    *,
    outputs: ConversionOutputs,
    tax_year: int,
    county_ids: Sequence[str] | None = None,
    dataset_types: Sequence[str] | None = None,
) -> list[DatasetVerification]:
    requested_counties = set(county_ids or ["harris", "fort_bend"])
    requested_datasets = set(dataset_types or ["property_roll", "tax_rates"])
    verifications: list[DatasetVerification] = []

    if "harris" in requested_counties and "property_roll" in requested_datasets:
        verifications.append(
            verify_dataset_output(
                county_id="harris",
                dataset_type="property_roll",
                tax_year=tax_year,
                source_path=outputs.harris_property_roll,
            )
        )
    if "harris" in requested_counties and "tax_rates" in requested_datasets:
        verifications.append(
            verify_dataset_output(
                county_id="harris",
                dataset_type="tax_rates",
                tax_year=tax_year,
                source_path=outputs.harris_tax_rates,
            )
        )
    if "fort_bend" in requested_counties and "property_roll" in requested_datasets:
        verifications.append(
            verify_dataset_output(
                county_id="fort_bend",
                dataset_type="property_roll",
                tax_year=tax_year,
                source_path=outputs.fort_bend_property_roll,
            )
        )
    if "fort_bend" in requested_counties and "tax_rates" in requested_datasets:
        verifications.append(
            verify_dataset_output(
                county_id="fort_bend",
                dataset_type="tax_rates",
                tax_year=tax_year,
                source_path=outputs.fort_bend_tax_rates,
            )
        )
    return verifications


def verify_dataset_output(
    *,
    county_id: str,
    dataset_type: str,
    tax_year: int,
    source_path: Path,
) -> DatasetVerification:
    config = load_county_adapter_config(county_id)
    if county_id == "harris":
        media_type = "application/json"
        parse_fn = parse_harris
        validate_fn = (
            validate_harris_property_roll if dataset_type == "property_roll" else validate_harris_tax_rates
        )
    elif county_id == "fort_bend":
        media_type = "text/csv"
        parse_fn = parse_fort_bend
        validate_fn = validate_fb_property_roll if dataset_type == "property_roll" else validate_fb_tax_rates
    else:
        raise ValueError(f"Unsupported county_id={county_id}.")

    return _verify_dataset(
        county_id=county_id,
        dataset_type=dataset_type,
        tax_year=tax_year,
        config=config,
        source_system_code=config.dataset_configs[dataset_type].source_system_code,
        media_type=media_type,
        source_path=source_path,
        parse_fn=parse_fn,
        validate_fn=validate_fn,
    )


def write_manifest(
    *,
    ready_root: Path,
    county_id: str,
    dataset_type: str,
    tax_year: int,
    raw_inputs: Sequence[tuple[str, Path]],
    output_path: Path,
    row_count: int,
    verification: DatasetVerification | None,
) -> Path:
    manifest_path = ready_root / f"{county_id}_{dataset_type}_{tax_year}.manifest.json"
    raw_files = [build_raw_file_details(logical_name=name, path=path) for name, path in raw_inputs]
    output_files = [build_output_file_details(path=output_path, row_count=row_count)]
    validation_status = "skipped"
    parse_issue_count = 0
    validation_error_count = 0
    if verification is not None:
        parse_issue_count = verification.parse_issue_count
        validation_error_count = verification.validation_error_count
        validation_status = (
            "passed"
            if parse_issue_count == 0 and validation_error_count == 0
            else "failed"
        )

    payload = {
        "schema_version": 1,
        "county_id": county_id,
        "tax_year": tax_year,
        "dataset_type": dataset_type,
        "raw_files": [asdict(item) for item in raw_files],
        "output_files": [asdict(item) for item in output_files],
        "row_count": row_count,
        "validation": {
            "status": validation_status,
            "parse_issue_count": parse_issue_count,
            "validation_error_count": validation_error_count,
        },
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest_path


def build_raw_file_details(*, logical_name: str, path: Path) -> RawFileDetails:
    return RawFileDetails(
        logical_name=logical_name,
        path=str(path),
        size_bytes=path.stat().st_size,
        checksum_sha256=_sha256_file(path),
    )


def build_output_file_details(*, path: Path, row_count: int) -> OutputFileDetails:
    return OutputFileDetails(
        path=str(path),
        size_bytes=path.stat().st_size,
        checksum_sha256=_sha256_file(path),
        row_count=row_count,
    )


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
        job_id="prepare_manual_county_files",
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


def _prepare_harris_lookup_tables(
    connection: sqlite3.Connection,
    *,
    raw_paths: HarrisRawPaths,
    tax_year: int,
) -> None:
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
    _index_harris_buildings(connection, source_path=raw_paths.building_res, tax_year=tax_year)
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


def _index_harris_buildings(
    connection: sqlite3.Connection,
    *,
    source_path: Path,
    tax_year: int,
) -> None:
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
                    max(tax_year - eff_year, 0) if eff_year else None,
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
    with _open_raw_text(raw_path) as handle, output_path.open("w", encoding="utf-8") as output:
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
                        "source_family": "harris_manual_jur_tax_dist_export",
                        "raw_tax_dist": unit_code,
                        "source_tax_year": tax_year,
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
    with _open_raw_text(raw_path) as handle, output_path.open("w", encoding="utf-8", newline="") as output:
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


def _require_named_files(
    county_id: str,
    dataset_type: str,
    paths: Sequence[tuple[str, Path]],
) -> None:
    missing = [f"{logical_name}={path}" for logical_name, path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing required raw file(s) for {county_id}/{dataset_type}: {', '.join(missing)}"
        )


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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    main()
