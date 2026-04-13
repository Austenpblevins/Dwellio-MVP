from __future__ import annotations

from app.services.stage17_tax_rate_migration_reconciliation import (
    Stage17TaxRateMigrationInspection,
    build_stage17_tax_rate_migration_plan,
)


def test_reconciliation_plan_reports_healthy_integrated_chain() -> None:
    plan = build_stage17_tax_rate_migration_plan(
        Stage17TaxRateMigrationInspection(
            schema_migrations_present=True,
            applied_versions={
                "0050": "stage17_tax_rate_basis_hardening",
                "0051": "stage17_tax_rate_adoption_status_admin_truth",
            },
            missing_hardening_columns=(),
            missing_adoption_tables=(),
            missing_adoption_columns=(),
        )
    )

    assert plan.environment_shape == "integrated_expected"
    assert plan.safe_to_apply_repair is False
    assert plan.repair_actions == ()


def test_reconciliation_plan_flags_legacy_old_0050_collision() -> None:
    plan = build_stage17_tax_rate_migration_plan(
        Stage17TaxRateMigrationInspection(
            schema_migrations_present=True,
            applied_versions={"0050": "stage17_tax_rate_adoption_status_admin_truth"},
            missing_hardening_columns=(
                "instant_quote_refresh_runs.tax_rate_quoteable_subject_row_count",
            ),
            missing_adoption_tables=(),
            missing_adoption_columns=(),
        )
    )

    assert plan.environment_shape == "legacy_old_0050_adoption_collision"
    assert plan.safe_to_apply_repair is True
    assert plan.repair_actions == (
        "record_0050_hardening_metadata",
        "record_0051_adoption_metadata",
        "apply_hardening_sql",
    )


def test_reconciliation_plan_recommends_normal_migrations_for_fresh_environment() -> None:
    plan = build_stage17_tax_rate_migration_plan(
        Stage17TaxRateMigrationInspection(
            schema_migrations_present=False,
            applied_versions={},
            missing_hardening_columns=(
                "instant_quote_refresh_runs.tax_rate_quoteable_subject_row_count",
            ),
            missing_adoption_tables=("instant_quote_tax_rate_adoption_statuses",),
            missing_adoption_columns=(
                "instant_quote_subject_cache.effective_tax_rate_basis_status",
            ),
        )
    )

    assert plan.environment_shape == "pending_normal_migrations"
    assert plan.safe_to_apply_repair is False
    assert "run_migrations" in plan.recommended_operator_steps[0]
