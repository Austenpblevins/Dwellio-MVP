from __future__ import annotations

import os
from uuid import uuid4

import psycopg

from app.core.config import Settings
from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def _isolated_stage21_database_url(monkeypatch) -> str:
    if not os.getenv("DWELLIO_DATABASE_URL"):
        monkeypatch.setenv(
            "DWELLIO_DATABASE_URL",
            "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev",
        )
    if not os.getenv("DWELLIO_ENV"):
        monkeypatch.setenv("DWELLIO_ENV", "stage21_dev")
    return Settings().database_url


def test_unequal_roll_mvp_foundation_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0067" in versions


def test_unequal_roll_mvp_foundation_migration_creates_required_tables() -> None:
    migration_path = MIGRATIONS_DIR / "0067_unequal_roll_mvp_foundation.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS unequal_roll_runs" in sql
    assert "unequal_roll_run_id uuid PRIMARY KEY" in sql
    assert "subject_snapshot_status" in sql
    assert "summary_json jsonb" in sql
    assert "CREATE TABLE IF NOT EXISTS unequal_roll_subject_snapshots" in sql
    assert "valuation_bathroom_features_json jsonb" in sql
    assert "snapshot_json jsonb" in sql
    assert "source_provenance_json jsonb" in sql
    assert "UNIQUE (unequal_roll_run_id)" in sql
    assert "supported_with_review" in sql
    assert "manual_review_required" in sql
    assert "canonical_snapshot_with_missing_additive_bathroom_metadata" in sql


def test_unequal_roll_mvp_foundation_migration_uses_env_driven_stage21_db(monkeypatch) -> None:
    database_url = _isolated_stage21_database_url(monkeypatch)

    assert "54322" not in database_url
    assert "55442" in database_url


def test_unequal_roll_mvp_foundation_migration_applies_in_isolated_schema(monkeypatch) -> None:
    migration_path = MIGRATIONS_DIR / "0067_unequal_roll_mvp_foundation.sql"
    sql = migration_path.read_text(encoding="utf-8")
    schema_name = f"tmp_unequal_roll_{uuid4().hex[:8]}"
    database_url = _isolated_stage21_database_url(monkeypatch)

    with psycopg.connect(database_url, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema_name}"')
            cursor.execute(f'SET search_path TO "{schema_name}", public')
            cursor.execute(
                """
                CREATE TABLE counties (county_id text PRIMARY KEY);
                CREATE TABLE tax_years (tax_year integer PRIMARY KEY);
                CREATE TABLE parcels (parcel_id uuid PRIMARY KEY);
                """
            )
            cursor.execute(sql)
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = 'unequal_roll_runs'
                ORDER BY ordinal_position
                """,
                (schema_name,),
            )
            run_columns = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = 'unequal_roll_subject_snapshots'
                ORDER BY ordinal_position
                """,
                (schema_name,),
            )
            snapshot_columns = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = %s::regclass
                ORDER BY conname
                """,
                (f'{schema_name}.unequal_roll_runs',),
            )
            constraint_defs = [row[0] for row in cursor.fetchall()]

        connection.rollback()

    assert "support_status" in run_columns
    assert "source_coverage_status" in run_columns
    assert "valuation_bathroom_features_json" in snapshot_columns
    assert "snapshot_json" in snapshot_columns
    assert any("supported_with_review" in definition for definition in constraint_defs)
    assert any(
        "canonical_snapshot_with_missing_additive_bathroom_metadata" in definition
        for definition in constraint_defs
    )
