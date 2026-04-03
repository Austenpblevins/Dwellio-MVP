from __future__ import annotations

import argparse
from pathlib import Path

from infra.scripts.prepare_manual_county_files import (
    ConversionOutputs,
    DatasetVerification,
    FortBendRawPaths,
    HarrisRawPaths,
    _open_sqlite,
    convert_fort_bend,
    convert_harris,
    resolve_fort_bend_paths,
    resolve_harris_paths,
    resolve_outputs,
    verify_outputs,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility wrapper for the original 2025 real-source converter. "
            "Use infra.scripts.prepare_manual_county_files for the reusable year-parameterized workflow."
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
        help="Tax year to convert. This compatibility wrapper remains scoped to 2025.",
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
        raise SystemExit(
            "convert_2025_real_sources.py remains 2025-only. "
            "Use python3 -m infra.scripts.prepare_manual_county_files for reusable year-parameterized prep."
        )

    raw_root = args.raw_root.expanduser().resolve()
    ready_dir = args.ready_dir.expanduser().resolve()
    ready_dir.mkdir(parents=True, exist_ok=True)

    harris_paths = resolve_harris_paths(raw_root=raw_root, tax_year=args.tax_year)
    fort_bend_paths = resolve_fort_bend_paths(raw_root=raw_root, tax_year=args.tax_year)
    outputs = resolve_outputs(ready_root=ready_dir, tax_year=args.tax_year)

    connection = _open_sqlite(ready_dir / ".convert_2025_real_sources.sqlite3")
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
        sqlite_path = ready_dir / ".convert_2025_real_sources.sqlite3"
        if sqlite_path.exists():
            sqlite_path.unlink()

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


__all__ = [
    "ConversionOutputs",
    "DatasetVerification",
    "FortBendRawPaths",
    "HarrisRawPaths",
    "_open_sqlite",
    "build_arg_parser",
    "convert_fort_bend",
    "convert_harris",
    "main",
    "resolve_outputs",
    "verify_outputs",
]


if __name__ == "__main__":
    main()
