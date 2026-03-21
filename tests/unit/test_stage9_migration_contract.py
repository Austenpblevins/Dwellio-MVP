from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage9_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0034" in versions


def test_stage9_migration_contains_owner_reconciliation_views_and_cad_snapshot_fields() -> None:
    migration_path = MIGRATIONS_DIR / "0034_stage9_ownership_reconciliation.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "cad_owner_name" in sql
    assert "cad_owner_name_normalized" in sql
    assert "metadata_json" in sql
    assert "v_owner_reconciliation_evidence" in sql
    assert "v_owner_reconciliation_qa" in sql
