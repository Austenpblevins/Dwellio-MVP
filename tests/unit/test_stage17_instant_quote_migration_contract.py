from __future__ import annotations

from infra.scripts.run_migrations import MIGRATIONS_DIR, discover_migrations


def test_stage17_instant_quote_migration_version_present() -> None:
    versions = [migration.version for migration in discover_migrations()]
    assert "0044" in versions
    assert "0045" in versions
    assert "0046" in versions
    assert "0047" in versions
    assert "0048" in versions
    assert "0049" in versions
    assert "0050" in versions
    assert "0051" in versions


def test_stage17_instant_quote_migration_contains_required_tables_and_view() -> None:
    migration_path = MIGRATIONS_DIR / "0044_stage17_instant_quote_service.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_neighborhood_stats" in sql
    assert "CREATE TABLE IF NOT EXISTS instant_quote_segment_stats" in sql
    assert "CREATE TABLE IF NOT EXISTS instant_quote_request_logs" in sql
    assert "CREATE VIEW instant_quote_subject_view AS" in sql
    assert "assessment_basis_value" in sql
    assert "support_blocker_code" in sql
    assert "now() AS last_refresh_at" not in sql


def test_stage17_instant_quote_serving_cache_migration_contains_required_table_and_indexes() -> None:
    migration_path = MIGRATIONS_DIR / "0045_stage17_instant_quote_serving_cache.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_subject_cache" in sql
    assert "PRIMARY KEY (parcel_id, tax_year)" in sql
    assert "idx_instant_quote_subject_cache_lookup" in sql
    assert "idx_instant_quote_subject_cache_neighborhood" in sql
    assert "idx_instant_quote_subject_cache_segment" in sql


def test_stage17_instant_quote_refresh_runs_migration_contains_required_table_and_indexes() -> None:
    migration_path = MIGRATIONS_DIR / "0046_stage17_instant_quote_refresh_runs.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_refresh_runs" in sql
    assert "refresh_status text NOT NULL DEFAULT 'running'" in sql
    assert "validation_report jsonb" in sql
    assert "idx_instant_quote_refresh_runs_lookup" in sql


def test_stage17_instant_quote_subject_cache_scope_index_migration_present() -> None:
    migration_path = MIGRATIONS_DIR / "0047_stage17_instant_quote_subject_cache_scope_index.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "idx_instant_quote_subject_cache_scope" in sql
    assert "ON instant_quote_subject_cache(county_id, tax_year, parcel_id)" in sql


def test_stage17_county_year_readiness_indexes_migration_present() -> None:
    migration_path = MIGRATIONS_DIR / "0048_stage17_county_year_readiness_indexes.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "idx_parcel_year_snapshots_current_scope" in sql
    assert "idx_search_documents_scope" in sql


def test_stage17_dynamic_tax_rate_basis_migration_present() -> None:
    migration_path = MIGRATIONS_DIR / "0049_stage17_dynamic_tax_rate_basis.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ALTER TABLE instant_quote_subject_cache" in sql
    assert "effective_tax_rate_basis_year" in sql
    assert "effective_tax_rate_basis_reason" in sql
    assert "effective_tax_rate_basis_fallback_applied" in sql
    assert "ALTER TABLE instant_quote_refresh_runs" in sql
    assert "tax_rate_basis_year" in sql
    assert "tax_rate_basis_reason" in sql
    assert "tax_rate_basis_fallback_applied" in sql
    assert "requested_tax_rate_supportable_subject_row_count" in sql
    assert "tax_rate_basis_supportable_subject_row_count" in sql

def test_stage17_tax_rate_basis_hardening_migration_present() -> None:
    migration_path = MIGRATIONS_DIR / "0050_stage17_tax_rate_basis_hardening.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "ALTER TABLE instant_quote_refresh_runs" in sql
    assert "tax_rate_quoteable_subject_row_count" in sql
    assert "requested_tax_rate_effective_tax_rate_coverage_ratio" in sql
    assert "requested_tax_rate_assignment_coverage_ratio" in sql
    assert "tax_rate_basis_effective_tax_rate_coverage_ratio" in sql
    assert "tax_rate_basis_assignment_coverage_ratio" in sql
    assert "tax_rate_basis_continuity_parcel_match_row_count" in sql
    assert "tax_rate_basis_continuity_parcel_gap_row_count" in sql
    assert "tax_rate_basis_continuity_parcel_match_ratio" in sql
    assert "tax_rate_basis_continuity_account_number_match_row_count" in sql
    assert "tax_rate_basis_warning_codes" in sql


def test_stage17_tax_rate_adoption_status_admin_truth_migration_present() -> None:
    migration_path = MIGRATIONS_DIR / "0051_stage17_tax_rate_adoption_status_admin_truth.sql"
    sql = migration_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS instant_quote_tax_rate_adoption_statuses" in sql
    assert "current_year_unofficial_or_proposed_rates" in sql
    assert "current_year_final_adopted_rates" in sql
    assert "effective_tax_rate_basis_status" in sql
    assert "effective_tax_rate_basis_status_reason" in sql
    assert "tax_rate_basis_status" in sql
    assert "tax_rate_basis_status_reason" in sql


def test_stage17_tax_rate_migration_order_is_integrated_and_non_colliding() -> None:
    migrations = {migration.version: migration.name for migration in discover_migrations()}

    assert migrations["0050"] == "stage17_tax_rate_basis_hardening"
    assert migrations["0051"] == "stage17_tax_rate_adoption_status_admin_truth"
