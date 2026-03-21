from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage8_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0033" in versions


def test_stage8_migration_contains_rollup_view_and_raw_code_support() -> None:
    migration_path = MIGRATIONS_DIR / "0033_stage8_exemption_normalization_rollup.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "raw_exemption_codes" in sql
    assert "parcel_exemption_rollup_view" in sql
    assert "qa_issue_codes" in sql
