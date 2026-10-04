from __future__ import annotations

from uuid import uuid4

import psycopg

from app.core.config import Settings
from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_forward_repair_restores_constraints_with_cross_schema_name_collisions(monkeypatch):
    monkeypatch.setenv(
        "DWELLIO_DATABASE_URL",
        "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev",
    )
    monkeypatch.setenv("DWELLIO_ENV", "stage21_dev")
    repair = (MIGRATIONS_DIR / "0080_unequal_roll_schema_scoped_constraint_repair.sql").read_text()
    migrations = [m for m in discover_migrations() if "0067" <= m.version <= "0079"]
    schemas = [f"tmp_constraint_repair_{uuid4().hex[:8]}" for _ in range(2)]
    with psycopg.connect(Settings().database_url) as connection:
        with connection.cursor() as cursor:
            definitions = []
            for schema in schemas:
                cursor.execute(f'CREATE SCHEMA "{schema}"')
                cursor.execute(f'SET search_path TO "{schema}", public')
                cursor.execute(
                    "CREATE TABLE counties (county_id text PRIMARY KEY); CREATE TABLE tax_years (tax_year integer PRIMARY KEY); CREATE TABLE parcels (parcel_id uuid PRIMARY KEY);"
                )
                for migration in migrations:
                    cursor.execute(migration.path.read_text())
                cursor.execute(repair)
                cursor.execute(
                    """
                    SELECT c.conname, pg_get_constraintdef(c.oid)
                    FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
                    WHERE n.nspname = %s AND c.conname IN (
                      'unequal_roll_candidates_eligibility_status_check',
                      'unequal_roll_candidates_ranking_status_check',
                      'unequal_roll_candidates_shortlist_status_check',
                      'unequal_roll_candidates_final_selection_support_status_check',
                      'unequal_roll_candidates_chosen_comp_status_check',
                      'unequal_roll_runs_final_comp_count_status_check',
                      'unequal_roll_runs_selection_governance_status_check',
                      'unequal_roll_candidates_adjustment_support_status_check',
                      'unequal_roll_adjustments_adjustment_reliability_flag_check',
                      'unequal_roll_candidates_adjustment_math_status_check',
                      'unequal_roll_runs_final_value_status_check',
                      'unequal_roll_candidates_final_value_status_check'
                    ) ORDER BY c.conname
                """,
                    (schema,),
                )
                before = cursor.fetchall()
                assert len(before) == 12
                cursor.execute(repair)
                cursor.execute(
                    "SELECT c.conname, pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = %s ORDER BY c.conname",
                    (schema,),
                )
                after = dict(cursor.fetchall())
                assert all(after[name] == definition for name, definition in before)
                definitions.append(before)
            assert definitions[0] == definitions[1]
        connection.rollback()
