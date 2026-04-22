from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage24_assessment_basis_contract_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0057" in versions


def test_stage24_assessment_basis_contract_migration_adds_typed_basis_columns() -> None:
    migration_path = MIGRATIONS_DIR / "0057_stage24_instant_quote_assessment_basis_contract.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ALTER TABLE instant_quote_subject_cache" in sql
    assert "assessment_basis_source_value_type" in sql
    assert "assessment_basis_source_year" in sql
    assert "assessment_basis_source_reason" in sql
    assert "assessment_basis_quality_code" in sql
