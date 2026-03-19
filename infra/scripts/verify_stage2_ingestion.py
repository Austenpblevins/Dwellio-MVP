from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import psycopg

from app.core.config import get_settings
from app.ingestion.service import IngestionLifecycleService
from infra.scripts.run_migrations import (
    apply_migration,
    discover_migrations,
    ensure_schema_migrations_table,
    fetch_applied_versions,
)

FIXTURE_ACCOUNT_NUMBERS = ("1001001001001", "1001001001002")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Stage 2 ingestion smoke test.")
    parser.add_argument("--database-url", default=None, help="Override DWELLIO_DATABASE_URL.")
    parser.add_argument("--county-id", default="harris")
    parser.add_argument("--tax-year", default=2026, type=int)
    parser.add_argument("--dataset-type", default="property_roll")
    parser.add_argument("--skip-migrate", action="store_true")
    return parser


def apply_pending_migrations(database_url: str) -> None:
    with psycopg.connect(database_url, autocommit=False) as connection:
        ensure_schema_migrations_table(connection)
        applied_versions = fetch_applied_versions(connection)
        pending = [migration for migration in discover_migrations() if migration.version not in applied_versions]
        for migration in pending:
            apply_migration(connection, migration, dry_run=False)
        connection.commit()


def capture_state(*, database_url: str, county_id: str, tax_year: int) -> dict[str, int]:
    with psycopg.connect(database_url, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*) AS count
                FROM import_batches
                WHERE county_id = %s
                  AND tax_year = %s
                """,
                (county_id, tax_year),
            )
            import_batches = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM raw_files
                WHERE county_id = %s
                  AND tax_year = %s
                """,
                (county_id, tax_year),
            )
            raw_files = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM job_runs
                WHERE county_id = %s
                  AND tax_year = %s
                """,
                (county_id, tax_year),
            )
            job_runs = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM stg_county_property_raw
                WHERE county_id = %s
                  AND import_batch_id IN (
                    SELECT import_batch_id
                    FROM import_batches
                    WHERE county_id = %s
                      AND tax_year = %s
                  )
                """,
                (county_id, county_id, tax_year),
            )
            staging_rows = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM validation_results
                WHERE county_id = %s
                  AND tax_year = %s
                """,
                (county_id, tax_year),
            )
            validation_results = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM lineage_records
                WHERE import_batch_id IN (
                  SELECT import_batch_id
                  FROM import_batches
                  WHERE county_id = %s
                    AND tax_year = %s
                )
                """,
                (county_id, tax_year),
            )
            lineage_records = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcels
                WHERE county_id = %s
                  AND account_number = ANY(%s)
                """,
                (county_id, list(FIXTURE_ACCOUNT_NUMBERS)),
            )
            parcels = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcel_year_snapshots
                WHERE county_id = %s
                  AND tax_year = %s
                  AND account_number = ANY(%s)
                """,
                (county_id, tax_year, list(FIXTURE_ACCOUNT_NUMBERS)),
            )
            parcel_year_snapshots = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT count(*) AS count
                FROM parcel_assessments pa
                JOIN parcels p ON p.parcel_id = pa.parcel_id
                WHERE p.county_id = %s
                  AND pa.tax_year = %s
                  AND p.account_number = ANY(%s)
                """,
                (county_id, tax_year, list(FIXTURE_ACCOUNT_NUMBERS)),
            )
            parcel_assessments = cursor.fetchone()[0]

    return {
        "import_batches": import_batches,
        "raw_files": raw_files,
        "job_runs": job_runs,
        "staging_rows": staging_rows,
        "validation_results": validation_results,
        "lineage_records": lineage_records,
        "parcels": parcels,
        "parcel_year_snapshots": parcel_year_snapshots,
        "parcel_assessments": parcel_assessments,
    }


def assert_state_unchanged(before: dict[str, int], after: dict[str, int], *, label: str) -> None:
    if before != after:
        raise AssertionError(f"{label} mutated persisted state: before={before} after={after}")


def assert_batch_state(
    *,
    database_url: str,
    import_batch_id: str,
    raw_file_id: str,
    county_id: str,
    tax_year: int,
    dataset_type: str,
) -> None:
    with psycopg.connect(database_url, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, publish_state, publish_version
                FROM import_batches
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            batch_row = cursor.fetchone()
            if batch_row is None:
                raise AssertionError("Missing import batch from Stage 2 smoke test.")
            if batch_row[0] != "normalized" or batch_row[1] != "published":
                raise AssertionError(f"Unexpected import batch state: {batch_row}")

            cursor.execute(
                """
                SELECT count(*)
                FROM raw_files
                WHERE raw_file_id = %s
                """,
                (raw_file_id,),
            )
            if cursor.fetchone()[0] != 1:
                raise AssertionError("Expected one raw_files row for the Stage 2 smoke test.")

            cursor.execute(
                """
                SELECT count(*)
                FROM stg_county_property_raw
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            staged_count = cursor.fetchone()[0]
            if staged_count == 0:
                raise AssertionError("Expected staging rows for the Stage 2 smoke test.")

            cursor.execute(
                """
                SELECT count(*)
                FROM validation_results
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            validation_count = cursor.fetchone()[0]
            if validation_count == 0:
                raise AssertionError("Expected validation_results rows for the Stage 2 smoke test.")

            cursor.execute(
                """
                SELECT count(*)
                FROM lineage_records
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            lineage_count = cursor.fetchone()[0]
            if lineage_count == 0:
                raise AssertionError("Expected lineage_records rows for the Stage 2 smoke test.")

            cursor.execute(
                """
                SELECT count(*)
                FROM parcels
                WHERE county_id = %s
                  AND account_number = ANY(%s)
                """,
                (county_id, list(FIXTURE_ACCOUNT_NUMBERS)),
            )
            parcel_count = cursor.fetchone()[0]
            if parcel_count < 2:
                raise AssertionError("Expected Harris fixture parcels in canonical storage.")

            cursor.execute(
                """
                SELECT count(*)
                FROM parcel_year_snapshots
                WHERE county_id = %s
                  AND tax_year = %s
                  AND account_number = ANY(%s)
                """,
                (county_id, tax_year, list(FIXTURE_ACCOUNT_NUMBERS)),
            )
            snapshot_count = cursor.fetchone()[0]
            if snapshot_count < 2:
                raise AssertionError("Expected parcel_year_snapshots rows from normalization.")

            cursor.execute(
                """
                SELECT count(*)
                FROM job_runs
                WHERE county_id = %s
                  AND tax_year = %s
                  AND metadata_json ->> 'dataset_type' = %s
                """,
                (county_id, tax_year, dataset_type),
            )
            if cursor.fetchone()[0] < 3:
                raise AssertionError("Expected fetch, staging, and normalize job_runs rows.")


def assert_rollback_state(
    *,
    database_url: str,
    import_batch_id: str,
    county_id: str,
    tax_year: int,
    dataset_type: str,
) -> None:
    with psycopg.connect(database_url, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, publish_state
                FROM import_batches
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            batch_row = cursor.fetchone()
            if batch_row is None:
                raise AssertionError("Missing import batch after rollback.")
            if batch_row[0] != "rolled_back" or batch_row[1] != "rolled_back":
                raise AssertionError(f"Unexpected rolled back batch state: {batch_row}")

            cursor.execute(
                """
                SELECT count(*)
                FROM parcel_year_snapshots
                WHERE import_batch_id = %s
                """,
                (import_batch_id,),
            )
            if cursor.fetchone()[0] != 0:
                raise AssertionError("Rollback should remove current parcel_year_snapshot publish rows.")

            cursor.execute(
                """
                SELECT count(*)
                FROM parcels
                WHERE county_id = %s
                  AND account_number = ANY(%s)
                """,
                (county_id, list(FIXTURE_ACCOUNT_NUMBERS)),
            )
            if cursor.fetchone()[0] != 0:
                raise AssertionError("Rollback should remove Stage 2 Harris fixture parcels on a clean verifier database.")

            cursor.execute(
                """
                SELECT count(*)
                FROM job_runs
                WHERE county_id = %s
                  AND tax_year = %s
                  AND job_stage = 'rollback'
                  AND metadata_json ->> 'dataset_type' = %s
                """,
                (county_id, tax_year, dataset_type),
            )
            if cursor.fetchone()[0] != 1:
                raise AssertionError("Expected one rollback job_run row.")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    database_url = args.database_url or os.getenv(
        "DWELLIO_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:54322/postgres",
    )

    if not args.skip_migrate:
        apply_pending_migrations(database_url)

    with tempfile.TemporaryDirectory(prefix="dwellio-stage2-archive-") as archive_root:
        os.environ["DWELLIO_DATABASE_URL"] = database_url
        os.environ["DWELLIO_RAW_ARCHIVE_ROOT"] = archive_root
        get_settings.cache_clear()

        service = IngestionLifecycleService()

        baseline = capture_state(database_url=database_url, county_id=args.county_id, tax_year=args.tax_year)
        dry_fetch = service.fetch_sources(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
            dry_run=True,
        )
        if not dry_fetch:
            raise AssertionError("Dry-run fetch should preview at least one dataset.")
        after_dry_fetch = capture_state(database_url=database_url, county_id=args.county_id, tax_year=args.tax_year)
        assert_state_unchanged(baseline, after_dry_fetch, label="dry-run fetch")
        if any(Path(archive_root).rglob("*")):
            raise AssertionError("Dry-run fetch should not persist archive files.")

        fetch_results = service.fetch_sources(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
        )
        if not fetch_results:
            raise AssertionError("Stage 2 smoke test did not fetch any datasets.")
        fetched = fetch_results[0]

        before_dry_staging = capture_state(database_url=database_url, county_id=args.county_id, tax_year=args.tax_year)
        service.load_staging(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
            import_batch_id=fetched.import_batch_id,
            dry_run=True,
        )
        after_dry_staging = capture_state(database_url=database_url, county_id=args.county_id, tax_year=args.tax_year)
        assert_state_unchanged(before_dry_staging, after_dry_staging, label="dry-run staging")

        staged = service.load_staging(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
            import_batch_id=fetched.import_batch_id,
        )

        before_dry_normalize = capture_state(database_url=database_url, county_id=args.county_id, tax_year=args.tax_year)
        service.normalize(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
            import_batch_id=fetched.import_batch_id,
            dry_run=True,
        )
        after_dry_normalize = capture_state(database_url=database_url, county_id=args.county_id, tax_year=args.tax_year)
        assert_state_unchanged(before_dry_normalize, after_dry_normalize, label="dry-run normalize")

        normalized = service.normalize(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
            import_batch_id=fetched.import_batch_id,
        )

        assert_batch_state(
            database_url=database_url,
            import_batch_id=fetched.import_batch_id,
            raw_file_id=fetched.raw_file_id or staged.raw_file_id or normalized.raw_file_id or "",
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
        )

        service.rollback_publish(
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
            import_batch_id=fetched.import_batch_id,
        )
        assert_rollback_state(
            database_url=database_url,
            import_batch_id=fetched.import_batch_id,
            county_id=args.county_id,
            tax_year=args.tax_year,
            dataset_type=args.dataset_type,
        )

        print(
            f"[verify] Stage 2 ingestion passed for {args.county_id} {args.tax_year} "
            f"{args.dataset_type} batch={fetched.import_batch_id} publish={normalized.publish_version} "
            "with dry-run and rollback checks"
        )


if __name__ == "__main__":
    main()
