from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage24_instant_quote_county_tax_capability_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0059" in versions


def test_stage24_instant_quote_county_tax_capability_migration_adds_matrix_table() -> None:
    migration_path = MIGRATIONS_DIR / "0059_stage24_instant_quote_county_tax_capability.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_county_tax_capability" in sql
    assert "exemption_normalization_confidence" in sql
    assert "over65_reliability" in sql
    assert "disabled_reliability" in sql
    assert "disabled_veteran_reliability" in sql
    assert "freeze_reliability" in sql
    assert "tax_unit_assignment_reliability" in sql
    assert "tax_rate_reliability" in sql
    assert "profile_support_level" in sql
