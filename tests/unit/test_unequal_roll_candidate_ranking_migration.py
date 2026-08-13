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


def test_unequal_roll_candidate_ranking_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0072" in versions


def test_unequal_roll_candidate_ranking_migration_adds_required_columns() -> None:
    migration_path = MIGRATIONS_DIR / "0072_unequal_roll_candidate_ranking_support.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS ranking_position" in sql
    assert "ADD COLUMN IF NOT EXISTS ranking_status" in sql
    assert "ADD COLUMN IF NOT EXISTS ranking_version" in sql
    assert "ADD COLUMN IF NOT EXISTS ranking_config_version" in sql
    assert "ADD COLUMN IF NOT EXISTS ranking_detail_json" in sql
    assert "unequal_roll_candidates_ranking_status_check" in sql
    assert "pg_constraint" in sql


def test_unequal_roll_candidate_ranking_migration_uses_env_driven_stage21_db(
    monkeypatch,
) -> None:
    database_url = _isolated_stage21_database_url(monkeypatch)

    assert "54322" not in database_url
    assert "55442" in database_url


def test_unequal_roll_candidate_ranking_migration_applies_in_isolated_schema(
    monkeypatch,
) -> None:
    migrations = [
        "0067_unequal_roll_mvp_foundation.sql",
        "0068_unequal_roll_candidate_discovery.sql",
        "0069_unequal_roll_candidate_eligibility_scaffolding.sql",
        "0070_unequal_roll_candidate_eligibility_detail.sql",
        "0071_unequal_roll_candidate_similarity_scoring.sql",
        "0072_unequal_roll_candidate_ranking_support.sql",
    ]
    schema_name = f"tmp_unequal_roll_candidate_ranking_{uuid4().hex[:8]}"
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
            for migration in migrations:
                cursor.execute((MIGRATIONS_DIR / migration).read_text(encoding="utf-8"))
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
            columns = [row[0] for row in cursor.fetchall()]
        connection.rollback()

    assert "ranking_position" in columns
    assert "ranking_status" in columns
    assert "ranking_version" in columns
    assert "ranking_config_version" in columns
    assert "ranking_detail_json" in columns


def test_unequal_roll_candidate_ranking_migration_is_idempotent_for_constraint(
    monkeypatch,
) -> None:
    migrations = [
        "0067_unequal_roll_mvp_foundation.sql",
        "0068_unequal_roll_candidate_discovery.sql",
        "0069_unequal_roll_candidate_eligibility_scaffolding.sql",
        "0070_unequal_roll_candidate_eligibility_detail.sql",
        "0071_unequal_roll_candidate_similarity_scoring.sql",
        "0072_unequal_roll_candidate_ranking_support.sql",
    ]
    schema_name = f"tmp_unequal_roll_candidate_ranking_idempotent_{uuid4().hex[:8]}"
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
            for migration in migrations:
                cursor.execute((MIGRATIONS_DIR / migration).read_text(encoding="utf-8"))
            cursor.execute(
                (MIGRATIONS_DIR / "0072_unequal_roll_candidate_ranking_support.sql").read_text(
                    encoding="utf-8"
                )
            )
        connection.rollback()
