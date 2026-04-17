from __future__ import annotations

from app.ingestion.source_registry import CountyCapabilityEntry
from app.services.county_onboarding import CountyOnboardingService
from app.services.data_readiness import (
    CountyTaxYearReadiness,
    DatasetYearReadiness,
    TaxYearDerivedReadiness,
)
from app.services.historical_validation import HistoricalValidationService


def _dataset(
    dataset_type: str,
    *,
    tax_year: int,
    raw_file_count: int,
    canonical_published: bool,
    latest_import_batch_id: str | None = None,
    latest_import_status: str | None = None,
    latest_publish_state: str | None = None,
) -> DatasetYearReadiness:
    return DatasetYearReadiness(
        county_id="harris",
        tax_year=tax_year,
        dataset_type=dataset_type,
        source_system_code="source",
        access_method="manual_upload",
        availability_status="manual_upload_required",
        tax_year_known=True,
        raw_file_count=raw_file_count,
        latest_import_batch_id=latest_import_batch_id,
        latest_import_status=latest_import_status,
        latest_publish_state=latest_publish_state,
        staged=latest_import_status in {"staged", "normalized"},
        canonical_published=canonical_published,
    )


class StubDataReadinessService:
    def build_tax_year_readiness(self, *, county_id: str, tax_year: int) -> CountyTaxYearReadiness:
        assert county_id == "harris"
        if tax_year == 2026:
            return CountyTaxYearReadiness(
                county_id=county_id,
                tax_year=tax_year,
                tax_year_known=True,
                datasets=[
                    _dataset("property_roll", tax_year=tax_year, raw_file_count=0, canonical_published=False),
                    _dataset("tax_rates", tax_year=tax_year, raw_file_count=0, canonical_published=False),
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=False,
                    parcel_year_trend_ready=False,
                    neighborhood_stats_ready=False,
                    neighborhood_year_trend_ready=False,
                    search_support_ready=False,
                    feature_ready=False,
                    comp_ready=False,
                    quote_ready=False,
                ),
            )
        return CountyTaxYearReadiness(
            county_id=county_id,
            tax_year=tax_year,
            tax_year_known=True,
            datasets=[
                _dataset(
                    "property_roll",
                    tax_year=tax_year,
                    raw_file_count=1,
                    canonical_published=True,
                    latest_import_batch_id="batch-property",
                    latest_import_status="normalized",
                    latest_publish_state="published",
                ),
                _dataset(
                    "tax_rates",
                    tax_year=tax_year,
                    raw_file_count=1,
                    canonical_published=True,
                    latest_import_batch_id="batch-tax",
                    latest_import_status="normalized",
                    latest_publish_state="published",
                ),
            ],
            derived=TaxYearDerivedReadiness(
                parcel_summary_ready=True,
                parcel_year_trend_ready=True,
                neighborhood_stats_ready=True,
                neighborhood_year_trend_ready=True,
                search_support_ready=True,
                feature_ready=True,
                comp_ready=False,
                quote_ready=False,
            ),
        )


def test_build_contract_selects_repeatable_validation_year(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.county_onboarding.list_county_capability_entries",
        lambda county_id: [
            CountyCapabilityEntry(
                county_id=county_id,
                capability_code="parcel_level_homestead",
                label="Parcel-level Homestead",
                status="supported",
                source_datasets=["property_roll"],
            ),
            CountyCapabilityEntry(
                county_id=county_id,
                capability_code="parcel_level_freeze_signal",
                label="Freeze signal",
                status="limited",
                source_datasets=["property_roll"],
            ),
        ],
    )

    service = CountyOnboardingService(
        data_readiness_service=StubDataReadinessService(),  # type: ignore[arg-type]
        historical_validation_service=HistoricalValidationService(),
    )
    contract = service.build_contract(
        county_id="harris",
        tax_years=[2026, 2025],
        current_tax_year=2026,
    )

    assert contract.validation_tax_year == 2025
    assert contract.validation_recommended is True
    assert contract.current_year_snapshot is not None
    assert contract.current_year_snapshot.tax_year == 2026
    assert contract.validation_year_snapshot is not None
    assert contract.validation_year_snapshot.tax_year == 2025

    phases = {phase.phase_code: phase for phase in contract.phases}
    assert phases["capability_review"].status == "done"
    assert phases["validation_year_selection"].status == "done"
    assert phases["dataset_prep_contract"].status == "done"
    assert phases["canonical_publish_validation"].status == "done"
    assert phases["searchable_validation"].status == "done"
    assert phases["quote_supportability_validation"].status == "pending"
    assert "capability_limit:parcel_level_freeze_signal" in phases["quote_supportability_validation"].details
    assert contract.onboarding_summary.overall_status == "partial"
    assert contract.onboarding_summary.done_phase_count == 5
    assert contract.onboarding_summary.pending_phase_count == 1
    assert contract.onboarding_summary.next_phase_code == "quote_supportability_validation"
    assert contract.baseline_comparison.comparable is True
    assert contract.baseline_comparison.current_year_lagging is True
    assert "parcel_summary_ready" in contract.baseline_comparison.lagging_signals
    assert "property_roll:canonical_publish" in contract.baseline_comparison.lagging_signals
    action_codes = [action.action_code for action in contract.recommended_actions]
    assert "review_quote_supportability" in action_codes


def test_build_contract_flags_missing_manual_prep(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.county_onboarding.list_county_capability_entries",
        lambda county_id: [
            CountyCapabilityEntry(
                county_id=county_id,
                capability_code="parcel_level_homestead",
                label="Parcel-level Homestead",
                status="supported",
                source_datasets=["property_roll"],
            )
        ],
    )

    class SparseReadinessService:
        def build_tax_year_readiness(self, *, county_id: str, tax_year: int) -> CountyTaxYearReadiness:
            return CountyTaxYearReadiness(
                county_id=county_id,
                tax_year=tax_year,
                tax_year_known=True,
                datasets=[
                    _dataset("property_roll", tax_year=tax_year, raw_file_count=0, canonical_published=False),
                    _dataset("tax_rates", tax_year=tax_year, raw_file_count=0, canonical_published=False),
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=False,
                    parcel_year_trend_ready=False,
                    neighborhood_stats_ready=False,
                    neighborhood_year_trend_ready=False,
                    search_support_ready=False,
                    feature_ready=False,
                    comp_ready=False,
                    quote_ready=False,
                ),
            )

    service = CountyOnboardingService(
        data_readiness_service=SparseReadinessService(),  # type: ignore[arg-type]
        historical_validation_service=HistoricalValidationService(),
    )
    contract = service.build_contract(
        county_id="harris",
        tax_years=[2026],
        current_tax_year=2026,
    )

    phases = {phase.phase_code: phase for phase in contract.phases}
    assert phases["validation_year_selection"].status == "pending"
    assert phases["dataset_prep_contract"].status == "pending"
    assert phases["dataset_prep_contract"].blocking is True
    assert "property_roll:manual_prep_required" in phases["dataset_prep_contract"].details
    assert contract.onboarding_summary.overall_status == "blocked"
    assert contract.onboarding_summary.next_blocking_phase_code == "validation_year_selection"
    assert contract.baseline_comparison.comparable is True
    assert contract.baseline_comparison.current_year_lagging is False
    assert contract.baseline_comparison.notes == ["current_year_matches_or_exceeds_validation_baseline"]
    actions = {action.action_code: action for action in contract.recommended_actions}
    assert "review_validation_year_ranking" in actions
    assert "prepare_adapter_ready_files" in actions
    assert actions["prepare_adapter_ready_files"].blocking is True
    assert "--tax-year 2026" in (actions["prepare_adapter_ready_files"].command_hint or "")
