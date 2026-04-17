from __future__ import annotations

from app.ingestion.source_registry import CountyCapabilityEntry
from app.services.county_onboarding import (
    OnboardingAction,
    CountyOnboardingContract,
    OnboardingPhase,
    OnboardingReadinessSnapshot,
    OnboardingDatasetSnapshot,
    OnboardingValidationYear,
)
from infra.scripts.report_county_onboarding import build_payload


def test_report_county_onboarding_builds_machine_readable_contract(monkeypatch) -> None:
    class StubCountyOnboardingService:
        def build_contract(self, *, county_id: str, tax_years: list[int], current_tax_year: int | None):
            assert county_id == "harris"
            assert tax_years == [2026, 2025]
            assert current_tax_year == 2026
            return CountyOnboardingContract(
                county_id=county_id,
                current_tax_year=2026,
                validation_tax_year=2025,
                validation_recommended=True,
                capabilities=[
                    CountyCapabilityEntry(
                        county_id=county_id,
                        capability_code="parcel_level_homestead",
                        label="Parcel-level Homestead",
                        status="supported",
                        source_datasets=["property_roll"],
                    )
                ],
                validation_candidates=[
                    OnboardingValidationYear(
                        tax_year=2025,
                        readiness_score=48,
                        recommended_for_qa=True,
                        caveats=["comp_generation_not_ready"],
                        validation_capabilities={"parcel_summary_validation_ready": True},
                    )
                ],
                current_year_snapshot=OnboardingReadinessSnapshot(
                    tax_year=2026,
                    datasets=[
                        OnboardingDatasetSnapshot(
                            dataset_type="property_roll",
                            access_method="manual_upload",
                            availability_status="manual_upload_required",
                            raw_file_count=0,
                            latest_import_batch_id=None,
                            latest_import_status=None,
                            latest_publish_state=None,
                            canonical_published=False,
                        )
                    ],
                    parcel_summary_ready=False,
                    search_support_ready=False,
                    feature_ready=False,
                    comp_ready=False,
                    quote_ready=False,
                ),
                validation_year_snapshot=OnboardingReadinessSnapshot(
                    tax_year=2025,
                    datasets=[
                        OnboardingDatasetSnapshot(
                            dataset_type="property_roll",
                            access_method="manual_upload",
                            availability_status="manual_upload_required",
                            raw_file_count=1,
                            latest_import_batch_id="batch-property",
                            latest_import_status="normalized",
                            latest_publish_state="published",
                            canonical_published=True,
                        )
                    ],
                    parcel_summary_ready=True,
                    search_support_ready=True,
                    feature_ready=True,
                    comp_ready=False,
                    quote_ready=False,
                ),
                phases=[
                    OnboardingPhase(
                        phase_code="validation_year_selection",
                        label="Validation year selection",
                        status="done",
                        blocking=False,
                        summary="Use tax year 2025 as the repeatable onboarding QA baseline.",
                        details=["comp_generation_not_ready"],
                    )
                ],
                recommended_actions=[
                    OnboardingAction(
                        action_code="review_quote_supportability",
                        phase_code="quote_supportability_validation",
                        blocking=False,
                        summary="Review quote-supportability gaps against the capability matrix.",
                        command_hint="python3 -m infra.scripts.report_readiness_metrics --county-id harris --tax-years 2025",
                    )
                ],
            )

    monkeypatch.setattr(
        "infra.scripts.report_county_onboarding.CountyOnboardingService",
        StubCountyOnboardingService,
    )

    payload = build_payload(
        county_id="harris",
        tax_years=[2026, 2025],
        current_tax_year=2026,
    )

    assert payload["validation_tax_year"] == 2025
    assert payload["validation_recommended"] is True
    assert payload["capabilities"][0]["capability_code"] == "parcel_level_homestead"
    assert payload["validation_candidates"][0]["readiness_score"] == 48
    assert payload["current_year_snapshot"]["tax_year"] == 2026
    assert payload["validation_year_snapshot"]["tax_year"] == 2025
    assert payload["phases"][0]["phase_code"] == "validation_year_selection"
    assert payload["recommended_actions"][0]["action_code"] == "review_quote_supportability"
