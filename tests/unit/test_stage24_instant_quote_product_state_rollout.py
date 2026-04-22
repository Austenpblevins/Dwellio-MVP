from pathlib import Path


def test_stage24_product_state_rollout_migration_adds_public_rollout_columns() -> None:
    migration = Path("app/db/migrations/0062_stage24_instant_quote_product_state_rollout.sql")

    assert migration.exists()
    sql = migration.read_text()

    assert "public_rollout_state" in sql
    assert "public_rollout_reason_code" in sql
    assert "public_rollout_applied_flag" in sql
