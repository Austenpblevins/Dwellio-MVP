from pathlib import Path


def test_stage24_savings_translation_rollout_migration_adds_rollout_columns() -> None:
    migration = Path("app/db/migrations/0063_stage24_instant_quote_savings_translation_rollout.sql")

    assert migration.exists()
    sql = migration.read_text()

    assert "savings_translation_mode" in sql
    assert "savings_translation_reason_code" in sql
    assert "savings_translation_applied_flag" in sql
    assert "selected_public_savings_estimate_raw" in sql
