from __future__ import annotations

import argparse
import csv
import json
import os
from decimal import Decimal
from pathlib import Path

import psycopg


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply the Fort Bend living-area correction from PropertyDataExport SquareFootage."
    )
    parser.add_argument(
        "--property-summary-export",
        required=True,
        type=Path,
        help="Path to the Fort Bend PropertyDataExport property file containing QuickRefID and SquareFootage.",
    )
    parser.add_argument("--tax-year", type=int, default=2026)
    parser.add_argument(
        "--database-url",
        default=os.getenv("DWELLIO_DATABASE_URL"),
        help="Database URL. Defaults to DWELLIO_DATABASE_URL.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="Optional path to write the correction report as JSON.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute correction counts without updating parcel_improvements.",
    )
    return parser


def _load_square_footage_rows(path: Path) -> list[tuple[str, int]]:
    rows: list[tuple[str, int]] = []
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            cad_property_id = str(row.get("QuickRefID") or "").strip()
            raw_sqft = str(row.get("SquareFootage") or "").strip()
            if not cad_property_id or not raw_sqft:
                continue
            try:
                living_area_sf = int(round(float(raw_sqft)))
            except ValueError:
                continue
            if living_area_sf <= 0:
                continue
            rows.append((cad_property_id, living_area_sf))
    return rows


def _prepare_temp_table(connection: psycopg.Connection, rows: list[tuple[str, int]]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE fort_bend_property_square_footage (
                cad_property_id text PRIMARY KEY,
                living_area_sf integer NOT NULL
            ) ON COMMIT DROP
            """
        )
        for start in range(0, len(rows), 5000):
            cursor.executemany(
                """
                INSERT INTO fort_bend_property_square_footage (cad_property_id, living_area_sf)
                VALUES (%s, %s)
                ON CONFLICT (cad_property_id) DO UPDATE
                SET living_area_sf = EXCLUDED.living_area_sf
                """,
                rows[start : start + 5000],
            )


def _build_report(connection: psycopg.Connection, *, tax_year: int) -> dict[str, object]:
    with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
        cursor.execute(
            """
            WITH candidates AS (
                SELECT
                    p.parcel_id,
                    p.cad_property_id,
                    pi.living_area_sf AS current_living_area_sf,
                    src.living_area_sf AS corrected_living_area_sf
                FROM parcels AS p
                JOIN parcel_improvements AS pi
                  ON pi.parcel_id = p.parcel_id
                 AND pi.tax_year = %s
                JOIN fort_bend_property_square_footage AS src
                  ON src.cad_property_id = p.cad_property_id
                WHERE p.county_id = 'fort_bend'
            )
            SELECT
                COUNT(*) AS matched_rows,
                COUNT(*) FILTER (
                    WHERE current_living_area_sf IS DISTINCT FROM corrected_living_area_sf
                ) AS changed_rows,
                COUNT(*) FILTER (
                    WHERE COALESCE(current_living_area_sf, 0) <= 0
                ) AS current_missing_rows,
                ROUND(AVG(current_living_area_sf - corrected_living_area_sf)::numeric, 2) AS mean_sqft_delta,
                ROUND(
                    percentile_cont(0.5) WITHIN GROUP (
                        ORDER BY current_living_area_sf - corrected_living_area_sf
                    )::numeric,
                    2
                ) AS median_sqft_delta
            FROM candidates
            """,
            (tax_year,),
        )
        summary = dict(cursor.fetchone() or {})

        cursor.execute(
            """
            WITH candidates AS (
                SELECT
                    p.cad_property_id,
                    pi.living_area_sf AS current_living_area_sf,
                    src.living_area_sf AS corrected_living_area_sf
                FROM parcels AS p
                JOIN parcel_improvements AS pi
                  ON pi.parcel_id = p.parcel_id
                 AND pi.tax_year = %s
                JOIN fort_bend_property_square_footage AS src
                  ON src.cad_property_id = p.cad_property_id
                WHERE p.county_id = 'fort_bend'
                  AND pi.living_area_sf IS DISTINCT FROM src.living_area_sf
                ORDER BY ABS(COALESCE(pi.living_area_sf, 0) - src.living_area_sf) DESC, p.cad_property_id
                LIMIT 5
            )
            SELECT json_agg(candidates) AS largest_changes
            FROM candidates
            """,
            (tax_year,),
        )
        largest_changes = cursor.fetchone() or {}

    summary["largest_changes"] = largest_changes.get("largest_changes") or []
    return summary


def _apply_update(connection: psycopg.Connection, *, tax_year: int) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH candidates AS (
                SELECT
                    pi.parcel_improvement_id,
                    src.living_area_sf AS corrected_living_area_sf
                FROM parcels AS p
                JOIN parcel_improvements AS pi
                  ON pi.parcel_id = p.parcel_id
                 AND pi.tax_year = %s
                JOIN fort_bend_property_square_footage AS src
                  ON src.cad_property_id = p.cad_property_id
                WHERE p.county_id = 'fort_bend'
                  AND pi.living_area_sf IS DISTINCT FROM src.living_area_sf
            )
            UPDATE parcel_improvements AS pi
            SET living_area_sf = candidates.corrected_living_area_sf
            FROM candidates
            WHERE pi.parcel_improvement_id = candidates.parcel_improvement_id
            """,
            (tax_year,),
        )
        return cursor.rowcount


def main() -> None:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("A database URL is required via --database-url or DWELLIO_DATABASE_URL.")

    rows = _load_square_footage_rows(args.property_summary_export.expanduser().resolve())
    if not rows:
        raise SystemExit("No Fort Bend SquareFootage rows were loaded from the property summary export.")

    with psycopg.connect(args.database_url) as connection:
        _prepare_temp_table(connection, rows)
        report = _json_safe({
            "tax_year": args.tax_year,
            "loaded_square_footage_rows": len(rows),
            **_build_report(connection, tax_year=args.tax_year),
            "dry_run": bool(args.dry_run),
        })
        if not args.dry_run:
            report["updated_rows"] = _apply_update(connection, tax_year=args.tax_year)
            connection.commit()
        else:
            connection.rollback()

    if args.report_json is not None:
        args.report_json.expanduser().resolve().write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
