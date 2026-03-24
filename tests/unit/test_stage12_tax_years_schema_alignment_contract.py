from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_tax_years_alignment_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0037" in versions


def test_tax_years_alignment_migration_is_additive_and_backfills_valuation_date() -> None:
    migration_path = MIGRATIONS_DIR / "0037_tax_years_valuation_date_alignment.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS valuation_date date" in sql
    assert "ADD COLUMN IF NOT EXISTS certified_roll_date date" in sql
    assert "SET valuation_date = make_date(tax_year, 1, 1)" in sql
    assert "WHERE valuation_date IS NULL" in sql


def test_tax_years_alignment_migration_does_not_repurpose_existing_period_columns() -> None:
    migration_path = MIGRATIONS_DIR / "0037_tax_years_valuation_date_alignment.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "DROP COLUMN starts_on" not in sql
    assert "DROP COLUMN ends_on" not in sql
    assert "RENAME COLUMN starts_on" not in sql
    assert "RENAME COLUMN ends_on" not in sql
