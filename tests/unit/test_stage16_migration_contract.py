from __future__ import annotations

from pathlib import Path


def test_stage16_lead_funnel_migration_is_additive() -> None:
    sql = Path("app/db/migrations/0042_stage16_lead_funnel_backend_contracts.sql").read_text(
        encoding="utf-8"
    )

    assert "ALTER TABLE leads" in sql
    assert "ADD COLUMN IF NOT EXISTS account_number text" in sql
    assert "ADD COLUMN IF NOT EXISTS owner_name text" in sql
    assert "ADD COLUMN IF NOT EXISTS consent_to_contact boolean NOT NULL DEFAULT false" in sql

