from __future__ import annotations

import argparse
import os

import psycopg

from app.county_adapters.common.config_loader import load_county_adapter_config
from app.county_adapters.common.field_mapping import canonical_field_codes
from app.county_adapters.harris.adapter import HarrisCountyAdapter
from infra.scripts.run_migrations import (
    apply_migration,
    discover_migrations,
    ensure_schema_migrations_table,
    fetch_applied_versions,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Stage 3 county config and field dictionary verifier.")
    parser.add_argument("--database-url", default=None, help="Override DWELLIO_DATABASE_URL.")
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


def assert_dictionary_rows(database_url: str, field_codes: list[str]) -> None:
    with psycopg.connect(database_url, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM canonical_field_dictionary")
            dictionary_count = cursor.fetchone()[0]
            if dictionary_count < 20:
                raise AssertionError("Expected seeded canonical field dictionary rows for Stage 3.")

            cursor.execute(
                """
                SELECT canonical_field_code
                FROM canonical_field_dictionary
                WHERE canonical_field_code = ANY(%s)
                ORDER BY canonical_field_code
                """,
                (field_codes,),
            )
            matched_codes = [row[0] for row in cursor.fetchall()]

    missing_codes = sorted(set(field_codes) - set(matched_codes))
    if missing_codes:
        raise AssertionError(f"Missing canonical field dictionary rows for mapping codes: {missing_codes}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    database_url = args.database_url or os.getenv(
        "DWELLIO_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:54322/postgres",
    )

    if not args.skip_migrate:
        apply_pending_migrations(database_url)

    harris_config = load_county_adapter_config("harris")
    fort_bend_config = load_county_adapter_config("fort_bend")

    if list(harris_config.dataset_configs) != ["property_roll"]:
        raise AssertionError("Expected Harris Stage 3 dataset config to expose property_roll explicitly.")
    if list(fort_bend_config.dataset_configs) != ["property_roll"]:
        raise AssertionError("Expected Fort Bend Stage 3 dataset config to expose property_roll explicitly.")
    if fort_bend_config.dataset_configs["property_roll"].ingestion_ready:
        raise AssertionError("Fort Bend should remain scaffold-only in Stage 3.")

    field_codes = canonical_field_codes(config=harris_config, dataset_type="property_roll")
    if not field_codes:
        raise AssertionError("Expected Harris property_roll mapping codes in Stage 3 config.")
    assert_dictionary_rows(database_url, field_codes)

    adapter = HarrisCountyAdapter()
    acquired = adapter.acquire_dataset("property_roll", 2026)
    staging_rows = adapter.parse_raw_to_staging(acquired)
    normalized = adapter.normalize_staging_to_canonical(
        "property_roll",
        [row.raw_payload for row in staging_rows],
    )
    property_roll = normalized["property_roll"]
    if property_roll[0]["parcel"]["account_number"] != "1001001001001":
        raise AssertionError("Expected Harris config-driven normalization to preserve account numbers.")
    if property_roll[0]["characteristics"]["homestead_flag"] is not True:
        raise AssertionError("Expected Harris config-driven normalization to derive homestead_flag.")
    if property_roll[0]["value_components"][2]["taxable_value"] != 230000:
        raise AssertionError("Expected Harris config-driven normalization to derive taxable market total.")

    print(
        "[verify] Stage 3 county configs passed: Harris config-driven property_roll mapping is wired, "
        "Fort Bend remains scaffold-only, and canonical field dictionary rows are seeded."
    )


if __name__ == "__main__":
    main()
