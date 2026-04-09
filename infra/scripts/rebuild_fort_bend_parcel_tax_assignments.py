from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from app.db.connection import get_connection
from app.ingestion.repository import IngestionRepository
from psycopg.types.json import Jsonb

MATCH_ENTITY_ASSIGNMENT_REASON = "match_entity_code"
MATCH_ENTITY_ASSIGNMENT_CONFIDENCE = 0.9999
MATCH_SCHOOL_ASSIGNMENT_REASON = "match_school_district_name"
MATCH_SCHOOL_ASSIGNMENT_CONFIDENCE = 0.96
FORT_BEND_COUNTY_ID = "fort_bend"


@dataclass(frozen=True)
class FortBendParcelScopeRow:
    parcel_id: str
    account_number: str
    cad_property_id: str


@dataclass(frozen=True)
class FortBendTaxUnitRow:
    taxing_unit_id: str
    unit_code: str
    unit_name: str
    unit_type_code: str


@dataclass(frozen=True)
class CorrectionSummary:
    county_id: str
    tax_year: int
    entity_export_paths: list[str]
    parcel_scope_row_count: int
    entity_code_row_count: int
    parcel_with_entity_rows_count: int
    matched_assignment_row_count: int
    parcel_with_matched_assignments_count: int
    school_name_assignment_row_count: int
    unmatched_entity_code_count: int
    unmatched_entity_code_rows: list[dict[str, Any]]
    prior_parcel_tax_assignment_count: int
    prior_effective_tax_rate_count: int
    resulting_parcel_tax_assignment_count: int
    resulting_effective_tax_rate_count: int


def _normalize_lookup_name(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().upper().split())


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild Fort Bend parcel_taxing_units from official EntityExport rows against the "
            "existing canonical taxing_units/tax_rates universe, then refresh effective_tax_rates."
        )
    )
    parser.add_argument("--tax-year", type=int, required=True)
    parser.add_argument("--entity-export", type=Path, action="append", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def load_entity_code_sets(*, entity_export_paths: Iterable[Path]) -> dict[str, set[str]]:
    code_sets: dict[str, set[str]] = defaultdict(set)
    for entity_export_path in entity_export_paths:
        with entity_export_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                cad_property_id = str(row.get("OwnerQuickRefID") or "").strip()
                entity_code = str(row.get("EntityCode") or "").strip()
                if not cad_property_id or not entity_code:
                    continue
                code_sets[cad_property_id].add(entity_code)
    return dict(code_sets)


def build_assignment_rows(
    *,
    entity_code_sets: dict[str, set[str]],
    parcel_scope: Iterable[FortBendParcelScopeRow],
    tax_units_by_code: dict[str, FortBendTaxUnitRow],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    parcel_by_quick_ref = {row.cad_property_id: row for row in parcel_scope}
    assignments: list[dict[str, Any]] = []
    unmatched_code_counts: Counter[str] = Counter()

    for cad_property_id, entity_codes in entity_code_sets.items():
        parcel = parcel_by_quick_ref.get(cad_property_id)
        if parcel is None:
            continue
        for entity_code in sorted(entity_codes):
            tax_unit = tax_units_by_code.get(entity_code)
            if tax_unit is None:
                unmatched_code_counts[entity_code] += 1
                continue
            assignments.append(
                {
                    "parcel_id": parcel.parcel_id,
                    "cad_property_id": cad_property_id,
                    "account_number": parcel.account_number,
                    "entity_code": entity_code,
                    "taxing_unit_id": tax_unit.taxing_unit_id,
                    "unit_code": tax_unit.unit_code,
                    "unit_name": tax_unit.unit_name,
                    "unit_type_code": tax_unit.unit_type_code,
                }
            )
    return assignments, unmatched_code_counts


def fetch_fort_bend_parcel_scope(*, repository: IngestionRepository, tax_year: int) -> list[FortBendParcelScopeRow]:
    rows = repository._fetch_rows(
        """
        SELECT
          p.parcel_id,
          p.account_number,
          p.cad_property_id
        FROM parcels p
        JOIN parcel_year_snapshots pys
          ON pys.parcel_id = p.parcel_id
        WHERE p.county_id = %s
          AND pys.tax_year = %s
          AND pys.is_current = true
          AND p.cad_property_id IS NOT NULL
        ORDER BY p.account_number ASC
        """,
        (FORT_BEND_COUNTY_ID, tax_year),
    )
    return [
        FortBendParcelScopeRow(
            parcel_id=str(row["parcel_id"]),
            account_number=row["account_number"],
            cad_property_id=row["cad_property_id"],
        )
        for row in rows
    ]


def fetch_rate_bearing_tax_units(
    *,
    repository: IngestionRepository,
    tax_year: int,
) -> dict[str, FortBendTaxUnitRow]:
    rows = repository._fetch_rows(
        """
        SELECT DISTINCT
          tu.taxing_unit_id,
          tu.unit_code,
          tu.unit_name,
          tu.unit_type_code
        FROM taxing_units tu
        JOIN tax_rates tr
          ON tr.taxing_unit_id = tu.taxing_unit_id
         AND tr.tax_year = tu.tax_year
         AND tr.is_current = true
        WHERE tu.county_id = %s
          AND tu.tax_year = %s
          AND tu.active_flag = true
        ORDER BY tu.unit_code ASC
        """,
        (FORT_BEND_COUNTY_ID, tax_year),
    )
    return {
        row["unit_code"]: FortBendTaxUnitRow(
            taxing_unit_id=str(row["taxing_unit_id"]),
            unit_code=row["unit_code"],
            unit_name=row["unit_name"],
            unit_type_code=row["unit_type_code"],
        )
        for row in rows
    }


def build_preferred_school_unit_name_map(
    *, tax_units_by_code: dict[str, FortBendTaxUnitRow]
) -> dict[str, FortBendTaxUnitRow]:
    school_units: dict[str, FortBendTaxUnitRow] = {}
    for tax_unit in tax_units_by_code.values():
        if tax_unit.unit_type_code != "school":
            continue
        if not tax_unit.unit_code.startswith("S"):
            continue
        key = _normalize_lookup_name(tax_unit.unit_name)
        if not key:
            continue
        existing = school_units.get(key)
        if existing is None or tax_unit.unit_code < existing.unit_code:
            school_units[key] = tax_unit
    return school_units


def count_existing_state(*, repository: IngestionRepository, tax_year: int) -> tuple[int, int]:
    assignment_row = repository._fetch_optional_row(
        """
        SELECT count(*) AS count
        FROM parcel_taxing_units ptu
        JOIN parcels p
          ON p.parcel_id = ptu.parcel_id
        WHERE p.county_id = %s
          AND ptu.tax_year = %s
        """,
        (FORT_BEND_COUNTY_ID, tax_year),
    )
    etr_row = repository._fetch_optional_row(
        """
        SELECT count(*) AS count
        FROM effective_tax_rates etr
        JOIN parcels p
          ON p.parcel_id = etr.parcel_id
        WHERE p.county_id = %s
          AND etr.tax_year = %s
        """,
        (FORT_BEND_COUNTY_ID, tax_year),
    )
    return int(assignment_row["count"] or 0), int(etr_row["count"] or 0)


def replace_assignments_from_entity_codes(
    *,
    repository: IngestionRepository,
    tax_year: int,
    property_import_batch_id: str,
    property_source_system_id: str,
    job_run_id: str,
    entity_export_paths: Iterable[Path],
    assignment_rows: list[dict[str, Any]],
) -> None:
    if not assignment_rows:
        return

    matched_parcel_ids = sorted({str(row["parcel_id"]) for row in assignment_rows})
    source_files = [str(path) for path in entity_export_paths]

    with repository.connection.cursor() as cursor:
        cursor.execute("CREATE TEMP TABLE tmp_fort_bend_entity_target_parcels (parcel_id uuid PRIMARY KEY) ON COMMIT DROP")
        cursor.executemany(
            "INSERT INTO tmp_fort_bend_entity_target_parcels (parcel_id) VALUES (%s) ON CONFLICT (parcel_id) DO NOTHING",
            [(parcel_id,) for parcel_id in matched_parcel_ids],
        )
        cursor.execute(
            """
            DELETE FROM parcel_taxing_units ptu
            USING parcels p, tmp_fort_bend_entity_target_parcels target
            WHERE ptu.parcel_id = p.parcel_id
              AND target.parcel_id = ptu.parcel_id
              AND p.county_id = %s
              AND ptu.tax_year = %s
              AND ptu.assignment_method <> 'manual'
            """,
            (FORT_BEND_COUNTY_ID, tax_year),
        )
        cursor.executemany(
            """
            INSERT INTO parcel_taxing_units (
              parcel_id,
              tax_year,
              taxing_unit_id,
              assignment_method,
              assignment_confidence,
              is_primary,
              source_system_id,
              import_batch_id,
              job_run_id,
              assignment_reason_code,
              match_basis_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (parcel_id, tax_year, taxing_unit_id)
            DO UPDATE SET
              assignment_method = EXCLUDED.assignment_method,
              assignment_confidence = EXCLUDED.assignment_confidence,
              is_primary = EXCLUDED.is_primary,
              source_system_id = EXCLUDED.source_system_id,
              import_batch_id = EXCLUDED.import_batch_id,
              job_run_id = EXCLUDED.job_run_id,
              assignment_reason_code = EXCLUDED.assignment_reason_code,
              match_basis_json = EXCLUDED.match_basis_json,
              updated_at = now()
            """,
            [
                (
                    assignment["parcel_id"],
                    tax_year,
                    assignment["taxing_unit_id"],
                    "source_direct",
                    MATCH_ENTITY_ASSIGNMENT_CONFIDENCE,
                    False,
                    property_source_system_id,
                    property_import_batch_id,
                    job_run_id,
                    MATCH_ENTITY_ASSIGNMENT_REASON,
                    Jsonb(
                        {
                            "matched_field": "entity_code",
                            "entity_code": assignment["entity_code"],
                            "cad_property_id": assignment["cad_property_id"],
                            "account_number": assignment["account_number"],
                            "source_files": source_files,
                        }
                    ),
                )
                for assignment in assignment_rows
            ],
        )

        cursor.execute(
            """
            WITH ranked AS (
              SELECT
                ptu.parcel_taxing_unit_id,
                ROW_NUMBER() OVER (
                  PARTITION BY ptu.parcel_id, tu.unit_type_code
                  ORDER BY tu.unit_code ASC, ptu.parcel_taxing_unit_id ASC
                ) AS unit_rank
              FROM parcel_taxing_units ptu
              JOIN taxing_units tu
                ON tu.taxing_unit_id = ptu.taxing_unit_id
              JOIN parcels p
                ON p.parcel_id = ptu.parcel_id
              JOIN tmp_fort_bend_entity_target_parcels target
                ON target.parcel_id = ptu.parcel_id
              WHERE p.county_id = %s
                AND ptu.tax_year = %s
                AND ptu.assignment_method <> 'manual'
            )
            UPDATE parcel_taxing_units ptu
            SET
              is_primary = ranked.unit_rank = 1,
              updated_at = now()
            FROM ranked
            WHERE ranked.parcel_taxing_unit_id = ptu.parcel_taxing_unit_id
            """,
            (FORT_BEND_COUNTY_ID, tax_year),
        )


def supplement_missing_school_assignments(
    *,
    repository: IngestionRepository,
    tax_year: int,
    property_import_batch_id: str,
    property_source_system_id: str,
    job_run_id: str,
    school_units_by_name: dict[str, FortBendTaxUnitRow],
) -> int:
    if not school_units_by_name:
        return 0

    parcel_rows = repository._fetch_rows(
        """
        SELECT
          pys.parcel_id,
          p.account_number,
          COALESCE(pc.school_district_name, p.school_district_name) AS school_district_name
        FROM parcel_year_snapshots pys
        JOIN parcels p
          ON p.parcel_id = pys.parcel_id
        LEFT JOIN property_characteristics pc
          ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
        WHERE p.county_id = %s
          AND pys.tax_year = %s
          AND pys.is_current = true
        ORDER BY p.account_number ASC
        """,
        (FORT_BEND_COUNTY_ID, tax_year),
    )
    existing_school_rows = repository._fetch_rows(
        """
        SELECT DISTINCT ptu.parcel_id
        FROM parcel_taxing_units ptu
        JOIN taxing_units tu
          ON tu.taxing_unit_id = ptu.taxing_unit_id
        JOIN parcels p
          ON p.parcel_id = ptu.parcel_id
        WHERE p.county_id = %s
          AND ptu.tax_year = %s
          AND tu.unit_type_code = 'school'
        """,
        (FORT_BEND_COUNTY_ID, tax_year),
    )
    existing_school_parcel_ids = {
        str(row["parcel_id"])
        for row in existing_school_rows
        if row.get("parcel_id") is not None
    }

    assignment_rows: list[tuple[Any, ...]] = []
    for row in parcel_rows:
        parcel_id = str(row["parcel_id"])
        if parcel_id in existing_school_parcel_ids:
            continue
        school_name = _normalize_lookup_name(row.get("school_district_name"))
        school_unit = school_units_by_name.get(school_name)
        if school_unit is None:
            continue
        assignment_rows.append(
            (
                parcel_id,
                tax_year,
                school_unit.taxing_unit_id,
                "source_inferred",
                MATCH_SCHOOL_ASSIGNMENT_CONFIDENCE,
                True,
                property_source_system_id,
                property_import_batch_id,
                job_run_id,
                MATCH_SCHOOL_ASSIGNMENT_REASON,
                Jsonb(
                    {
                        "matched_field": "school_district_name",
                        "matched_value": row.get("school_district_name"),
                        "unit_code": school_unit.unit_code,
                        "unit_name": school_unit.unit_name,
                        "account_number": row.get("account_number"),
                    }
                ),
            )
        )

    if not assignment_rows:
        return 0

    with repository.connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO parcel_taxing_units (
              parcel_id,
              tax_year,
              taxing_unit_id,
              assignment_method,
              assignment_confidence,
              is_primary,
              source_system_id,
              import_batch_id,
              job_run_id,
              assignment_reason_code,
              match_basis_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (parcel_id, tax_year, taxing_unit_id)
            DO UPDATE SET
              assignment_method = EXCLUDED.assignment_method,
              assignment_confidence = EXCLUDED.assignment_confidence,
              is_primary = EXCLUDED.is_primary,
              source_system_id = EXCLUDED.source_system_id,
              import_batch_id = EXCLUDED.import_batch_id,
              job_run_id = EXCLUDED.job_run_id,
              assignment_reason_code = EXCLUDED.assignment_reason_code,
              match_basis_json = EXCLUDED.match_basis_json,
              updated_at = now()
            """,
            assignment_rows,
        )
    return len(assignment_rows)


def rebuild_fort_bend_parcel_tax_assignments(
    *,
    tax_year: int,
    entity_export_paths: Iterable[Path],
    dry_run: bool = False,
) -> CorrectionSummary:
    resolved_entity_export_paths = [path.expanduser().resolve() for path in entity_export_paths]
    entity_code_sets = load_entity_code_sets(entity_export_paths=resolved_entity_export_paths)

    with get_connection() as connection:
        repository = IngestionRepository(connection)
        parcel_scope = fetch_fort_bend_parcel_scope(repository=repository, tax_year=tax_year)
        tax_units_by_code = fetch_rate_bearing_tax_units(repository=repository, tax_year=tax_year)
        school_units_by_name = build_preferred_school_unit_name_map(
            tax_units_by_code=tax_units_by_code
        )
        assignment_rows, unmatched_code_counts = build_assignment_rows(
            entity_code_sets=entity_code_sets,
            parcel_scope=parcel_scope,
            tax_units_by_code=tax_units_by_code,
        )
        prior_assignment_count, prior_etr_count = count_existing_state(
            repository=repository,
            tax_year=tax_year,
        )

        property_batch = repository.find_import_batch(
            county_id=FORT_BEND_COUNTY_ID,
            tax_year=tax_year,
            dataset_type="property_roll",
            import_batch_id=None,
        )

        summary = CorrectionSummary(
            county_id=FORT_BEND_COUNTY_ID,
            tax_year=tax_year,
            entity_export_paths=[str(path) for path in resolved_entity_export_paths],
            parcel_scope_row_count=len(parcel_scope),
            entity_code_row_count=sum(len(codes) for codes in entity_code_sets.values()),
            parcel_with_entity_rows_count=len(entity_code_sets),
            matched_assignment_row_count=len(assignment_rows),
            parcel_with_matched_assignments_count=len({row["parcel_id"] for row in assignment_rows}),
            school_name_assignment_row_count=0,
            unmatched_entity_code_count=len(unmatched_code_counts),
            unmatched_entity_code_rows=[
                {"entity_code": code, "parcel_row_count": count}
                for code, count in unmatched_code_counts.most_common()
            ],
            prior_parcel_tax_assignment_count=prior_assignment_count,
            prior_effective_tax_rate_count=prior_etr_count,
            resulting_parcel_tax_assignment_count=prior_assignment_count,
            resulting_effective_tax_rate_count=prior_etr_count,
        )
        if dry_run:
            return summary

        job_run_id = repository.create_job_run(
            county_id=FORT_BEND_COUNTY_ID,
            tax_year=tax_year,
            job_name="job_rebuild_fort_bend_parcel_tax_assignments",
            job_stage="normalize",
            import_batch_id=property_batch.import_batch_id,
            raw_file_id=property_batch.raw_file_id,
            dry_run_flag=False,
            metadata_json={
                "entity_export_paths": [str(path) for path in resolved_entity_export_paths],
                "matched_assignment_row_count": len(assignment_rows),
                "school_name_assignment_row_count": 0,
                "unmatched_entity_code_count": len(unmatched_code_counts),
            },
        )
        replace_assignments_from_entity_codes(
            repository=repository,
            tax_year=tax_year,
            property_import_batch_id=property_batch.import_batch_id,
            property_source_system_id=property_batch.source_system_id,
            job_run_id=job_run_id,
            entity_export_paths=resolved_entity_export_paths,
            assignment_rows=assignment_rows,
        )
        school_name_assignment_row_count = supplement_missing_school_assignments(
            repository=repository,
            tax_year=tax_year,
            property_import_batch_id=property_batch.import_batch_id,
            property_source_system_id=property_batch.source_system_id,
            job_run_id=job_run_id,
            school_units_by_name=school_units_by_name,
        )
        repository.refresh_effective_tax_rates(county_id=FORT_BEND_COUNTY_ID, tax_year=tax_year)
        resulting_assignment_count, resulting_etr_count = count_existing_state(
            repository=repository,
            tax_year=tax_year,
        )
        repository.complete_job_run(
            job_run_id,
            status="succeeded",
            row_count=len(assignment_rows),
            metadata_json={
                "entity_export_paths": [str(path) for path in resolved_entity_export_paths],
                "matched_assignment_row_count": len(assignment_rows),
                "school_name_assignment_row_count": school_name_assignment_row_count,
                "parcel_with_matched_assignments_count": len({row["parcel_id"] for row in assignment_rows}),
                "unmatched_entity_code_rows": summary.unmatched_entity_code_rows[:100],
                "prior_parcel_tax_assignment_count": prior_assignment_count,
                "prior_effective_tax_rate_count": prior_etr_count,
                "resulting_parcel_tax_assignment_count": resulting_assignment_count,
                "resulting_effective_tax_rate_count": resulting_etr_count,
            },
        )
        connection.commit()

        return CorrectionSummary(
            **{
                **asdict(summary),
                "school_name_assignment_row_count": school_name_assignment_row_count,
                "resulting_parcel_tax_assignment_count": resulting_assignment_count,
                "resulting_effective_tax_rate_count": resulting_etr_count,
            }
        )


def main() -> None:
    args = build_arg_parser().parse_args()
    summary = rebuild_fort_bend_parcel_tax_assignments(
        tax_year=args.tax_year,
        entity_export_paths=args.entity_export,
        dry_run=args.dry_run,
    )
    print(json.dumps(asdict(summary), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
