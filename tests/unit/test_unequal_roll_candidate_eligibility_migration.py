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


def test_unequal_roll_candidate_eligibility_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0069" in versions


def test_unequal_roll_candidate_eligibility_migration_adds_required_columns() -> None:
    migration_path = MIGRATIONS_DIR / "0069_unequal_roll_candidate_eligibility_scaffolding.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS eligibility_status" in sql
    assert "ADD COLUMN IF NOT EXISTS eligibility_reason_code" in sql
    assert "unequal_roll_candidates_eligibility_status_check" in sql
    assert "eligible', 'review', 'excluded" in sql


def test_unequal_roll_candidate_eligibility_migration_uses_env_driven_stage21_db(
    monkeypatch,
) -> None:
    database_url = _isolated_stage21_database_url(monkeypatch)

    assert "54322" not in database_url
    assert "55442" in database_url


def test_unequal_roll_candidate_eligibility_migration_applies_in_isolated_schema(
    monkeypatch,
) -> None:
    migration_0067 = (MIGRATIONS_DIR / "0067_unequal_roll_mvp_foundation.sql").read_text(
        encoding="utf-8"
    )
    migration_0068 = (MIGRATIONS_DIR / "0068_unequal_roll_candidate_discovery.sql").read_text(
        encoding="utf-8"
    )
    migration_0069 = (
        MIGRATIONS_DIR / "0069_unequal_roll_candidate_eligibility_scaffolding.sql"
    ).read_text(encoding="utf-8")
    schema_name = f"tmp_unequal_roll_candidate_eligibility_{uuid4().hex[:8]}"
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
            cursor.execute(migration_0067)
            cursor.execute(migration_0068)
            cursor.execute(migration_0069)
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = 'unequal_roll_candidates'
                ORDER BY ordinal_position
                """,
                (schema_name,),
            )
            candidate_columns = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = %s::regclass
                ORDER BY conname
                """,
                (f'{schema_name}.unequal_roll_candidates',),
            )
            constraint_defs = [row[0] for row in cursor.fetchall()]

        connection.rollback()

    assert "eligibility_status" in candidate_columns
    assert "eligibility_reason_code" in candidate_columns
    assert any("eligible" in definition for definition in constraint_defs)
    assert any("review" in definition for definition in constraint_defs)
    assert any("excluded" in definition for definition in constraint_defs)
