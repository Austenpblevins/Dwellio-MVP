from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage14_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0041" in versions


def test_stage14_migration_uses_canonical_case_and_packet_tables() -> None:
    migration_path = MIGRATIONS_DIR / "0041_stage14_case_ops_foundation.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS client_parcels" in sql
    assert "CREATE TABLE IF NOT EXISTS case_assignments" in sql
    assert "CREATE TABLE IF NOT EXISTS hearings" in sql
    assert "CREATE TABLE IF NOT EXISTS case_status_history" in sql
    assert "CREATE TABLE IF NOT EXISTS evidence_packet_items" in sql
    assert "CREATE TABLE IF NOT EXISTS evidence_comp_sets" in sql
    assert "CREATE TABLE IF NOT EXISTS evidence_comp_set_items" in sql
    assert "evidence_packet_sections" not in sql
    assert "evidence_packet_comps" not in sql
