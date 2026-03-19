from __future__ import annotations

import argparse
import os
import uuid
from typing import Iterable

import psycopg

from infra.scripts.run_migrations import (
    apply_migration,
    discover_migrations,
    ensure_schema_migrations_table,
    fetch_applied_versions,
)

REQUIRED_TABLES = (
    "counties",
    "appraisal_districts",
    "source_systems",
    "import_batches",
    "raw_files",
    "job_runs",
    "validation_results",
    "lineage_records",
    "parcels",
    "parcel_year_snapshots",
    "property_characteristics",
    "improvements",
    "land_segments",
    "value_components",
    "exemption_types",
    "parcel_exemptions",
    "taxing_unit_types",
    "taxing_units",
    "tax_rates",
    "parcel_taxing_units",
    "parcel_geometries",
    "taxing_unit_boundaries",
    "deed_records",
    "deed_parties",
    "parcel_owner_periods",
    "current_owner_rollups",
    "search_documents",
    "valuation_runs",
    "protest_cases",
    "evidence_packets",
    "case_outcomes",
    "manual_overrides",
)

REQUIRED_VIEWS = (
    "v_search_read_model",
    "v_quote_read_model",
)

REQUIRED_INDEXES = (
    "idx_parcels_county_year_account",
    "idx_parcel_addresses_normalized_trgm",
    "idx_validation_results_scope_severity",
    "idx_taxing_units_lookup",
    "idx_parcel_geometries_geom",
    "idx_taxing_unit_boundaries_geom",
    "idx_search_documents_search_text_trgm",
    "idx_manual_overrides_target",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply and verify the Stage 1 canonical schema.")
    parser.add_argument("--database-url", default=None, help="Override DWELLIO_DATABASE_URL.")
    parser.add_argument(
        "--skip-migrate",
        action="store_true",
        help="Verify the schema without applying pending migrations first.",
    )
    return parser


def default_database_url() -> str:
    return os.getenv(
        "DWELLIO_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:54322/postgres",
    )


def apply_pending_migrations(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    ensure_schema_migrations_table(connection)
    applied_versions = fetch_applied_versions(connection)
    pending = [migration for migration in discover_migrations() if migration.version not in applied_versions]
    if not pending:
        print("[verify] no pending migrations")
        return

    for migration in pending:
        apply_migration(connection, migration, dry_run=False)
    connection.commit()


def assert_relation_exists(
    connection: psycopg.Connection[tuple[object, ...]],
    relation_name: str,
    relation_kind: str,
) -> None:
    with connection.cursor() as cursor:
        if relation_kind == "table":
            cursor.execute("SELECT to_regclass(%s)", (relation_name,))
            row = cursor.fetchone()
            if row is None or row[0] is None:
                raise AssertionError(f"Missing required table: {relation_name}")
            return

        cursor.execute(
            """
            SELECT 1
            FROM pg_views
            WHERE schemaname = current_schema()
              AND viewname = %s
            """,
            (relation_name,),
        )
        if cursor.fetchone() is None:
            raise AssertionError(f"Missing required view: {relation_name}")


def assert_index_exists(
    connection: psycopg.Connection[tuple[object, ...]],
    index_name: str,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", (index_name,))
        row = cursor.fetchone()
    if row is None or row[0] is None:
        raise AssertionError(f"Missing required index: {index_name}")


def fetch_required_ids(
    connection: psycopg.Connection[tuple[object, ...]],
    county_id: str,
) -> tuple[str, str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT appraisal_district_id
            FROM appraisal_districts
            WHERE county_id = %s
            ORDER BY district_name
            LIMIT 1
            """,
            (county_id,),
        )
        appraisal_row = cursor.fetchone()
        cursor.execute(
            """
            SELECT source_system_id
            FROM source_systems
            WHERE source_system_code = 'MANUAL_UPLOAD'
            LIMIT 1
            """
        )
        source_row = cursor.fetchone()
    if appraisal_row is None or source_row is None:
        raise AssertionError("Expected Stage 1 seed rows for appraisal districts and MANUAL_UPLOAD.")
    return appraisal_row[0], source_row[0]


def assert_seed_counts(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    checks: tuple[tuple[str, str, tuple[object, ...]], ...] = (
        ("counties", "SELECT count(*) FROM counties WHERE county_id IN ('harris', 'fort_bend')", ()),
        ("appraisal_districts", "SELECT count(*) FROM appraisal_districts WHERE county_id IN ('harris', 'fort_bend')", ()),
        ("tax_years", "SELECT count(*) FROM tax_years WHERE tax_year = 2026", ()),
        ("exemption_types", "SELECT count(*) FROM exemption_types", ()),
        ("taxing_unit_types", "SELECT count(*) FROM taxing_unit_types", ()),
    )
    with connection.cursor() as cursor:
        for label, query, params in checks:
            cursor.execute(query, params)
            row = cursor.fetchone()
            if row is None or row[0] == 0:
                raise AssertionError(f"Expected seed rows in {label}.")


def run_smoke_insert(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    county_id = "harris"
    tax_year = 2026
    appraisal_district_id, source_system_id = fetch_required_ids(connection, county_id)
    token = uuid.uuid4().hex[:8]
    account_number = f"SMOKE-{token}"

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO import_batches (
              source_system_id,
              county_id,
              tax_year,
              source_filename,
              source_checksum,
              status,
              row_count,
              error_count
            )
            VALUES (%s, %s, %s, %s, %s, 'created', 1, 0)
            RETURNING import_batch_id
            """,
            (source_system_id, county_id, tax_year, f"{token}.csv", token),
        )
        import_batch_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO raw_files (
              import_batch_id,
              source_system_id,
              county_id,
              tax_year,
              storage_path,
              original_filename,
              checksum,
              mime_type,
              size_bytes,
              file_kind
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'text/csv', 128, 'parcel')
            RETURNING raw_file_id
            """,
            (
                import_batch_id,
                source_system_id,
                county_id,
                tax_year,
                f"smoke/{token}.csv",
                f"{token}.csv",
                token,
            ),
        )
        raw_file_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO job_runs (
              county_id,
              tax_year,
              job_name,
              status,
              import_batch_id,
              raw_file_id,
              job_stage
            )
            VALUES (%s, %s, 'stage1_smoke', 'running', %s, %s, 'verify')
            RETURNING job_run_id
            """,
            (county_id, tax_year, import_batch_id, raw_file_id),
        )
        job_run_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO parcels (
              county_id,
              appraisal_district_id,
              tax_year,
              account_number,
              cad_property_id,
              situs_address,
              situs_city,
              situs_state,
              situs_zip,
              owner_name,
              property_type_code,
              property_class_code,
              neighborhood_code,
              source_system_id,
              source_record_hash
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, 'Houston', 'TX', '77002', 'Smoke Owner',
              'sfr', 'A1', 'NBHD-1', %s, %s
            )
            RETURNING parcel_id
            """,
            (
                county_id,
                appraisal_district_id,
                tax_year,
                account_number,
                f"CAD-{token}",
                f"{token} Smoke Test Ln",
                source_system_id,
                token,
            ),
        )
        parcel_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO parcel_addresses (
              parcel_id,
              situs_address,
              situs_city,
              situs_state,
              situs_zip,
              normalized_address,
              is_current,
              source_system_id,
              source_record_hash
            )
            VALUES (%s, %s, 'Houston', 'TX', '77002', %s, true, %s, %s)
            """,
            (parcel_id, f"{token} Smoke Test Ln", f"{token} SMOKE TEST LN", source_system_id, token),
        )

        cursor.execute(
            """
            INSERT INTO parcel_assessments (
              parcel_id,
              tax_year,
              market_value,
              assessed_value,
              notice_value,
              source_system_id,
              source_record_hash
            )
            VALUES (%s, %s, 350000, 340000, 360000, %s, %s)
            """,
            (parcel_id, tax_year, source_system_id, token),
        )

        cursor.execute(
            """
            INSERT INTO parcel_exemptions (
              parcel_id,
              tax_year,
              exemption_type_code,
              exemption_amount,
              source_system_id,
              source_record_hash
            )
            VALUES (%s, %s, 'homestead', 100000, %s, %s)
            """,
            (parcel_id, tax_year, source_system_id, token),
        )

        cursor.execute(
            """
            INSERT INTO parcel_year_snapshots (
              parcel_id,
              county_id,
              appraisal_district_id,
              tax_year,
              account_number,
              source_system_id,
              import_batch_id,
              job_run_id,
              source_record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING parcel_year_snapshot_id
            """,
            (
                parcel_id,
                county_id,
                appraisal_district_id,
                tax_year,
                account_number,
                source_system_id,
                import_batch_id,
                job_run_id,
                token,
            ),
        )
        parcel_year_snapshot_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO property_characteristics (
              parcel_year_snapshot_id,
              property_type_code,
              property_class_code,
              neighborhood_code,
              homestead_flag
            )
            VALUES (%s, 'sfr', 'A1', 'NBHD-1', true)
            """,
            (parcel_year_snapshot_id,),
        )

        cursor.execute(
            """
            INSERT INTO improvements (
              parcel_year_snapshot_id,
              building_label,
              living_area_sf,
              year_built,
              bedrooms,
              full_baths,
              stories,
              source_system_id,
              source_record_hash
            )
            VALUES (%s, 'Main', 2100, 2005, 4, 2, 2, %s, %s)
            """,
            (parcel_year_snapshot_id, source_system_id, token),
        )

        cursor.execute(
            """
            INSERT INTO land_segments (
              parcel_year_snapshot_id,
              segment_num,
              land_sf,
              land_acres,
              source_system_id,
              source_record_hash
            )
            VALUES (%s, 1, 7200, 0.1653, %s, %s)
            """,
            (parcel_year_snapshot_id, source_system_id, token),
        )

        cursor.execute(
            """
            INSERT INTO value_components (
              parcel_year_snapshot_id,
              component_code,
              component_label,
              component_category,
              market_value,
              assessed_value,
              taxable_value,
              source_system_id,
              source_record_hash
            )
            VALUES (%s, 'market_total', 'Market Total', 'market', 350000, 340000, 260000, %s, %s)
            """,
            (parcel_year_snapshot_id, source_system_id, token),
        )

        cursor.execute(
            """
            INSERT INTO taxing_units (
              county_id,
              tax_year,
              appraisal_district_id,
              unit_type_code,
              unit_code,
              unit_name,
              source_system_id,
              import_batch_id,
              source_record_hash
            )
            VALUES (%s, %s, %s, 'county', %s, 'Smoke County Unit', %s, %s, %s)
            RETURNING taxing_unit_id
            """,
            (county_id, tax_year, appraisal_district_id, f"COUNTY-{token}", source_system_id, import_batch_id, token),
        )
        taxing_unit_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO tax_rates (
              taxing_unit_id,
              county_id,
              tax_year,
              rate_component,
              rate_value,
              rate_per_100,
              source_system_id,
              import_batch_id,
              source_record_hash
            )
            VALUES (%s, %s, %s, 'ad_valorem', 0.02150000, 2.150000, %s, %s, %s)
            """,
            (taxing_unit_id, county_id, tax_year, source_system_id, import_batch_id, token),
        )

        cursor.execute(
            """
            INSERT INTO effective_tax_rates (
              parcel_id,
              tax_year,
              effective_tax_rate,
              source_method,
              calculation_basis_json
            )
            VALUES (%s, %s, 0.02150000, 'stage1_smoke', '{"components": 1}'::jsonb)
            """,
            (parcel_id, tax_year),
        )

        cursor.execute(
            """
            INSERT INTO parcel_taxing_units (
              parcel_id,
              tax_year,
              taxing_unit_id,
              assignment_method,
              assignment_confidence,
              source_system_id,
              import_batch_id,
              job_run_id,
              source_record_hash
            )
            VALUES (%s, %s, %s, 'manual', 1.0, %s, %s, %s, %s)
            RETURNING parcel_taxing_unit_id
            """,
            (parcel_id, tax_year, taxing_unit_id, source_system_id, import_batch_id, job_run_id, token),
        )
        parcel_taxing_unit_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO parcel_geometries (
              parcel_id,
              tax_year,
              geometry_role,
              geom,
              centroid,
              source_system_id,
              import_batch_id,
              source_record_hash
            )
            VALUES (
              %s,
              %s,
              'parcel_centroid',
              ST_GeomFromText('POINT(-95.3698 29.7604)', 4326),
              ST_GeomFromText('POINT(-95.3698 29.7604)', 4326),
              %s,
              %s,
              %s
            )
            """,
            (parcel_id, tax_year, source_system_id, import_batch_id, token),
        )

        cursor.execute(
            """
            INSERT INTO taxing_unit_boundaries (
              taxing_unit_id,
              tax_year,
              boundary_scope,
              boundary_name,
              geom,
              source_system_id,
              import_batch_id,
              source_record_hash
            )
            VALUES (
              %s,
              %s,
              'service_area',
              'Smoke Boundary',
              ST_GeomFromText('MULTIPOLYGON(((-95.37 29.76,-95.37 29.77,-95.36 29.77,-95.36 29.76,-95.37 29.76)))', 4326),
              %s,
              %s,
              %s
            )
            """,
            (taxing_unit_id, tax_year, source_system_id, import_batch_id, token),
        )

        cursor.execute(
            """
            INSERT INTO validation_results (
              job_run_id,
              import_batch_id,
              raw_file_id,
              county_id,
              tax_year,
              validation_scope,
              severity,
              entity_table,
              validation_code,
              message
            )
            VALUES (%s, %s, %s, %s, %s, 'canonical_publish', 'info', 'parcels', 'SMOKE_OK', 'Smoke validation row.')
            """,
            (job_run_id, import_batch_id, raw_file_id, county_id, tax_year),
        )

        cursor.execute(
            """
            INSERT INTO lineage_records (
              job_run_id,
              import_batch_id,
              raw_file_id,
              relation_type,
              source_table,
              source_id,
              target_table,
              target_id,
              source_record_hash
            )
            VALUES (%s, %s, %s, 'raw_to_staging', 'raw_files', %s, 'parcel_year_snapshots', %s, %s)
            """,
            (job_run_id, import_batch_id, raw_file_id, raw_file_id, parcel_year_snapshot_id, token),
        )

        cursor.execute(
            """
            INSERT INTO deed_records (
              county_id,
              parcel_id,
              tax_year,
              source_system_id,
              import_batch_id,
              job_run_id,
              instrument_number,
              recording_date,
              consideration_amount,
              source_record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, 325000, %s)
            RETURNING deed_record_id
            """,
            (county_id, parcel_id, tax_year, source_system_id, import_batch_id, job_run_id, f"INSTR-{token}", token),
        )
        deed_record_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO deed_parties (
              deed_record_id,
              party_role,
              party_name,
              normalized_name,
              party_order
            )
            VALUES (%s, 'grantee', 'Smoke Owner', 'SMOKE OWNER', 1)
            """,
            (deed_record_id,),
        )

        cursor.execute(
            """
            INSERT INTO parcel_owner_periods (
              parcel_id,
              county_id,
              owner_name,
              owner_name_normalized,
              start_date,
              source_basis,
              deed_record_id,
              source_system_id,
              confidence_score,
              is_current
            )
            VALUES (%s, %s, 'Smoke Owner', 'SMOKE OWNER', CURRENT_DATE, 'smoke_test', %s, %s, 1.0, true)
            RETURNING parcel_owner_period_id
            """,
            (parcel_id, county_id, deed_record_id, source_system_id),
        )
        owner_period_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO current_owner_rollups (
              parcel_id,
              county_id,
              tax_year,
              owner_name,
              owner_name_normalized,
              source_basis,
              source_system_id,
              owner_period_id,
              confidence_score
            )
            VALUES (%s, %s, %s, 'Smoke Owner', 'SMOKE OWNER', 'smoke_test', %s, %s, 1.0)
            """,
            (parcel_id, county_id, tax_year, source_system_id, owner_period_id),
        )

        cursor.execute(
            """
            INSERT INTO search_documents (
              parcel_id,
              county_id,
              tax_year,
              account_number,
              normalized_address,
              normalized_owner_name,
              display_address,
              search_text
            )
            VALUES (%s, %s, %s, %s, %s, 'SMOKE OWNER', %s, %s)
            """,
            (
                parcel_id,
                county_id,
                tax_year,
                account_number,
                f"{token} SMOKE TEST LN HOUSTON TX 77002",
                f"{token} Smoke Test Ln, Houston, TX 77002",
                f"{account_number} {token} Smoke Test Ln Smoke Owner",
            ),
        )

        cursor.execute(
            """
            INSERT INTO valuation_runs (
              parcel_id,
              county_id,
              tax_year,
              run_status,
              market_value_point,
              equity_value_point,
              defensible_value_point,
              confidence_score
            )
            VALUES (%s, %s, %s, 'completed', 355000, 345000, 345000, 0.80)
            RETURNING valuation_run_id
            """,
            (parcel_id, county_id, tax_year),
        )
        valuation_run_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO protest_cases (
              parcel_id,
              tax_year,
              valuation_run_id,
              appraisal_district_id,
              workflow_status_code,
              case_status
            )
            VALUES (%s, %s, %s, %s, 'active', 'active')
            RETURNING protest_case_id
            """,
            (parcel_id, tax_year, valuation_run_id, appraisal_district_id),
        )
        protest_case_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO evidence_packets (
              protest_case_id,
              parcel_id,
              tax_year,
              valuation_run_id,
              packet_status
            )
            VALUES (%s, %s, %s, %s, 'draft')
            """,
            (protest_case_id, parcel_id, tax_year, valuation_run_id),
        )

        cursor.execute(
            """
            INSERT INTO case_outcomes (
              protest_case_id,
              outcome_code,
              final_value,
              reduction_amount,
              savings_amount,
              hearing_type_code
            )
            VALUES (%s, 'pending', 345000, 15000, 322.50, 'informal')
            """,
            (protest_case_id,),
        )

        cursor.execute(
            """
            INSERT INTO manual_overrides (
              county_id,
              tax_year,
              target_table,
              target_record_id,
              override_scope,
              override_payload,
              reason,
              status
            )
            VALUES (
              %s,
              %s,
              'parcel_taxing_units',
              %s,
              'assignment',
              '{"action": "keep"}'::jsonb,
              'Schema smoke test.',
              'approved'
            )
            """,
            (county_id, tax_year, parcel_taxing_unit_id),
        )

    connection.rollback()


def verify_schema(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    for table_name in REQUIRED_TABLES:
        assert_relation_exists(connection, table_name, relation_kind="table")
    for view_name in REQUIRED_VIEWS:
        assert_relation_exists(connection, view_name, relation_kind="view")
    for index_name in REQUIRED_INDEXES:
        assert_index_exists(connection, index_name)
    assert_seed_counts(connection)
    run_smoke_insert(connection)


def print_summary(items: Iterable[str], label: str) -> None:
    joined = ", ".join(items)
    print(f"[verify] {label}: {joined}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    database_url = args.database_url or default_database_url()
    with psycopg.connect(database_url, autocommit=False) as connection:
        if not args.skip_migrate:
            apply_pending_migrations(connection)
        verify_schema(connection)
        connection.rollback()

    print_summary(REQUIRED_TABLES[:6], "checked core tables")
    print_summary(REQUIRED_INDEXES, "checked indexes")
    print("[verify] Stage 1 schema verification passed")


if __name__ == "__main__":
    main()
