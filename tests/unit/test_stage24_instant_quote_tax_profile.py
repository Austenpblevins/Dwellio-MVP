from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage24_instant_quote_tax_profile_migration_present() -> None:
    versions = [migration.version for migration in discover_migrations()]

    assert "0060" in versions


def test_stage24_instant_quote_tax_profile_migration_adds_required_v51_columns() -> None:
    migration_path = MIGRATIONS_DIR / "0060_stage24_instant_quote_tax_profile.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_tax_profile" in sql
    assert "assessment_basis_source_value_type" in sql
    assert "assessment_basis_source_year" in sql
    assert "assessment_basis_source_reason" in sql
    assert "raw_exemption_codes" in sql
    assert "normalized_exemption_codes" in sql
    assert "exemption_profile_quality_score" in sql
    assert "tax_unit_assignment_complete_flag" in sql
    assert "tax_rate_complete_flag" in sql
    assert "tax_profile_status" in sql
    assert "tax_profile_quality_score" in sql
    assert "total_exemption_flag" in sql
    assert "near_total_exemption_flag" in sql
    assert "marginal_model_type" in sql
    assert "marginal_tax_rate_total" in sql
    assert "marginal_tax_rate_school" in sql
    assert "marginal_tax_rate_non_school" in sql
    assert "marginal_rate_basis" in sql
    assert "savings_limited_by_codes" in sql
    assert "affected_unit_mask" in sql
    assert "opportunity_vs_savings_state" in sql
    assert "profile_warning_codes" in sql
    assert "fallback_tax_profile_used_flag" in sql
