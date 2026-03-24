from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_multi_year_tax_year_bootstrap_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0038" in versions


def test_multi_year_tax_year_bootstrap_seeds_historical_years() -> None:
    sql = (MIGRATIONS_DIR / "0038_multi_year_tax_year_bootstrap.sql").read_text(encoding="utf-8")

    for year in ("2022", "2023", "2024", "2025"):
        assert f"({year}, DATE '{year}-01-01', DATE '{year}-12-31'" in sql
    assert "valuation_date" in sql
    assert "ON CONFLICT (tax_year) DO UPDATE" in sql
