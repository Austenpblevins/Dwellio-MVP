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


def test_unequal_roll_final_value_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0079" in versions


def test_unequal_roll_final_value_migration_adds_required_structures() -> None:
    migration_path = MIGRATIONS_DIR / "0079_unequal_roll_final_value_logic.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS final_value_status" in sql
    assert "ADD COLUMN IF NOT EXISTS requested_roll_value" in sql
    assert "ADD COLUMN IF NOT EXISTS final_value_detail_json" in sql
    assert "unequal_roll_runs_final_value_status_check" in sql
    assert "unequal_roll_candidates_final_value_status_check" in sql
    assert "pg_constraint" in sql


def test_unequal_roll_final_value_migration_uses_env_driven_stage21_db(
    monkeypatch,
) -> None:
    database_url = _isolated_stage21_database_url(monkeypatch)

    assert "54322" not in database_url
    assert "55442" in database_url


def test_unequal_roll_final_value_migration_applies_in_isolated_schema(
    monkeypatch,
) -> None:
    migrations = [
        "0067_unequal_roll_mvp_foundation.sql",
        "0068_unequal_roll_candidate_discovery.sql",
        "0069_unequal_roll_candidate_eligibility_scaffolding.sql",
        "0070_unequal_roll_candidate_eligibility_detail.sql",
        "0071_unequal_roll_candidate_similarity_scoring.sql",
        "0072_unequal_roll_candidate_ranking_support.sql",
        "0073_unequal_roll_candidate_shortlist_support.sql",
        "0074_unequal_roll_candidate_final_selection_support.sql",
        "0075_unequal_roll_candidate_chosen_comp_semantics.sql",
        "0076_unequal_roll_selection_governance.sql",
        "0077_unequal_roll_candidate_adjustment_support.sql",
        "0078_unequal_roll_adjustment_math_scaffolding.sql",
        "0079_unequal_roll_final_value_logic.sql",
    ]
    schema_name = f"tmp_unequal_roll_final_value_{uuid4().hex[:8]}"
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
                  AND table_name = 'unequal_roll_candidates'
                ORDER BY ordinal_position
                """,
                (schema_name,),
            )
            candidate_columns = [row[0] for row in cursor.fetchall()]
        connection.rollback()

    assert "final_value_status" in run_columns
    assert "requested_roll_value" in run_columns
    assert "final_value_detail_json" in run_columns
    assert "final_value_position" in candidate_columns
    assert "final_value_status" in candidate_columns
    assert "final_value_detail_json" in candidate_columns


def test_unequal_roll_final_value_migration_is_idempotent_for_constraints(
    monkeypatch,
) -> None:
    migrations = [
        "0067_unequal_roll_mvp_foundation.sql",
        "0068_unequal_roll_candidate_discovery.sql",
        "0069_unequal_roll_candidate_eligibility_scaffolding.sql",
        "0070_unequal_roll_candidate_eligibility_detail.sql",
        "0071_unequal_roll_candidate_similarity_scoring.sql",
        "0072_unequal_roll_candidate_ranking_support.sql",
        "0073_unequal_roll_candidate_shortlist_support.sql",
        "0074_unequal_roll_candidate_final_selection_support.sql",
        "0075_unequal_roll_candidate_chosen_comp_semantics.sql",
        "0076_unequal_roll_selection_governance.sql",
        "0077_unequal_roll_candidate_adjustment_support.sql",
        "0078_unequal_roll_adjustment_math_scaffolding.sql",
        "0079_unequal_roll_final_value_logic.sql",
    ]
    schema_name = f"tmp_unequal_roll_final_value_idempotent_{uuid4().hex[:8]}"
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
                (MIGRATIONS_DIR / "0079_unequal_roll_final_value_logic.sql").read_text(
                    encoding="utf-8"
                )
            )
        connection.rollback()
