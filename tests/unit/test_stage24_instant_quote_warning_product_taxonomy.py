from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage24_warning_product_taxonomy_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0058" in versions


def test_stage24_warning_product_taxonomy_migration_adds_internal_request_log_fields() -> None:
    migration_path = MIGRATIONS_DIR / "0058_stage24_instant_quote_warning_product_taxonomy.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ALTER TABLE instant_quote_request_logs" in sql
    assert "warning_action_classes" in sql
    assert "dominant_warning_action_class" in sql
    assert "warning_taxonomy_json" in sql
    assert "opportunity_vs_savings_state" in sql
    assert "product_state_reason_code" in sql
