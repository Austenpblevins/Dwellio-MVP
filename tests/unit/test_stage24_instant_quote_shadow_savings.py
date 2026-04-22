from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage24_instant_quote_shadow_savings_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0061" in versions


def test_stage24_instant_quote_shadow_savings_migration_adds_request_log_columns() -> None:
    migration_path = MIGRATIONS_DIR / "0061_stage24_instant_quote_shadow_savings.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ALTER TABLE instant_quote_request_logs" in sql
    assert "shadow_profile_version" in sql
    assert "shadow_savings_estimate_raw" in sql
    assert "shadow_savings_delta_raw" in sql
    assert "shadow_tax_profile_status" in sql
    assert "shadow_tax_profile_quality_score" in sql
    assert "shadow_marginal_model_type" in sql
    assert "shadow_marginal_tax_rate_total" in sql
    assert "shadow_opportunity_vs_savings_state" in sql
    assert "shadow_limiting_reason_codes" in sql
    assert "shadow_fallback_tax_profile_used_flag" in sql
