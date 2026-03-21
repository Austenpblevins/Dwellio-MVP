from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage7_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0031" in versions


def test_stage7_gis_migration_contains_core_helper_functions() -> None:
    migration_path = MIGRATIONS_DIR / "0031_stage7_gis_support.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "dwellio_normalize_geometry" in sql
    assert "dwellio_geometry_validation_issues" in sql
    assert "dwellio_spatial_assignment_candidates" in sql
