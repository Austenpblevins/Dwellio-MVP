from __future__ import annotations

from dataclasses import dataclass, field

from app.ingestion.source_registry import CountyCapabilityEntry, list_county_capability_entries
from app.services.data_readiness import CountyTaxYearReadiness, DataReadinessService
from app.services.historical_validation import HistoricalValidationCandidate, HistoricalValidationService

REQUIRED_ONBOARDING_DATASETS = ("property_roll", "tax_rates")


@dataclass(frozen=True)
class OnboardingDatasetSnapshot:
    dataset_type: str
    access_method: str
    availability_status: str
    raw_file_count: int
    latest_import_batch_id: str | None
    latest_import_status: str | None
    latest_publish_state: str | None
    canonical_published: bool


@dataclass(frozen=True)
class OnboardingReadinessSnapshot:
    tax_year: int
    datasets: list[OnboardingDatasetSnapshot]
    parcel_summary_ready: bool
    search_support_ready: bool
    feature_ready: bool
    comp_ready: bool
    quote_ready: bool


@dataclass(frozen=True)
class OnboardingValidationYear:
    tax_year: int
    readiness_score: int
    recommended_for_qa: bool
    caveats: list[str] = field(default_factory=list)
    validation_capabilities: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class OnboardingPhase:
    phase_code: str
    label: str
    status: str
    blocking: bool
    summary: str
    details: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OnboardingAction:
    action_code: str
    phase_code: str
    blocking: bool
    summary: str
    command_hint: str | None = None


@dataclass(frozen=True)
class OnboardingSummary:
    overall_status: str
    done_phase_count: int
    pending_phase_count: int
    blocked_phase_count: int
    blocking_phase_codes: list[str] = field(default_factory=list)
    next_phase_code: str | None = None
    next_blocking_phase_code: str | None = None


@dataclass(frozen=True)
class CountyOnboardingContract:
    county_id: str
    current_tax_year: int
    validation_tax_year: int | None
    validation_recommended: bool
    capabilities: list[CountyCapabilityEntry]
    validation_candidates: list[OnboardingValidationYear]
    current_year_snapshot: OnboardingReadinessSnapshot | None
    validation_year_snapshot: OnboardingReadinessSnapshot | None
    onboarding_summary: OnboardingSummary
    phases: list[OnboardingPhase]
    recommended_actions: list[OnboardingAction]


class CountyOnboardingService:
    def __init__(
        self,
        *,
        data_readiness_service: DataReadinessService | None = None,
        historical_validation_service: HistoricalValidationService | None = None,
    ) -> None:
        self.data_readiness_service = data_readiness_service or DataReadinessService()
        self.historical_validation_service = (
            historical_validation_service or HistoricalValidationService()
        )

    def build_contract(
        self,
        *,
        county_id: str,
        tax_years: list[int],
        current_tax_year: int | None = None,
    ) -> CountyOnboardingContract:
        if not tax_years:
            raise ValueError("tax_years must not be empty.")

        current_year = current_tax_year or max(tax_years)
        readiness_items = [
            self.data_readiness_service.build_tax_year_readiness(
                county_id=county_id,
                tax_year=tax_year,
            )
            for tax_year in tax_years
        ]
        readiness_by_year = {item.tax_year: item for item in readiness_items}
        ranked_candidates = self.historical_validation_service.rank_validation_years(
            readiness_items,
            current_tax_year=current_year,
        )
        validation_candidate = next(
            (candidate for candidate in ranked_candidates if candidate.recommended_for_qa),
            ranked_candidates[0] if ranked_candidates else None,
        )
        capabilities = list_county_capability_entries(county_id)
        current_snapshot = self._build_snapshot(readiness_by_year.get(current_year))
        validation_snapshot = self._build_snapshot(
            None
            if validation_candidate is None
            else readiness_by_year.get(validation_candidate.tax_year)
        )
        phases = self._build_phases(
            capabilities=capabilities,
            validation_candidate=validation_candidate,
            validation_readiness=(
                None
                if validation_candidate is None
                else readiness_by_year.get(validation_candidate.tax_year)
            ),
        )
        recommended_actions = self._build_recommended_actions(
            county_id=county_id,
            tax_years=tax_years,
            current_tax_year=current_year,
            validation_tax_year=(
                None if validation_candidate is None else validation_candidate.tax_year
            ),
            phases=phases,
        )
        onboarding_summary = self._build_onboarding_summary(phases)

        return CountyOnboardingContract(
            county_id=county_id,
            current_tax_year=current_year,
            validation_tax_year=None if validation_candidate is None else validation_candidate.tax_year,
            validation_recommended=(
                False if validation_candidate is None else validation_candidate.recommended_for_qa
            ),
            capabilities=capabilities,
            validation_candidates=[
                OnboardingValidationYear(
                    tax_year=candidate.tax_year,
                    readiness_score=candidate.readiness_score,
                    recommended_for_qa=candidate.recommended_for_qa,
                    caveats=list(candidate.caveats),
                    validation_capabilities=self._candidate_capabilities(candidate),
                )
                for candidate in ranked_candidates
            ],
            current_year_snapshot=current_snapshot,
            validation_year_snapshot=validation_snapshot,
            onboarding_summary=onboarding_summary,
            phases=phases,
            recommended_actions=recommended_actions,
        )

    def _build_snapshot(
        self,
        readiness: CountyTaxYearReadiness | None,
    ) -> OnboardingReadinessSnapshot | None:
        if readiness is None:
            return None
        return OnboardingReadinessSnapshot(
            tax_year=readiness.tax_year,
            datasets=[
                OnboardingDatasetSnapshot(
                    dataset_type=dataset.dataset_type,
                    access_method=dataset.access_method,
                    availability_status=dataset.availability_status,
                    raw_file_count=dataset.raw_file_count,
                    latest_import_batch_id=dataset.latest_import_batch_id,
                    latest_import_status=dataset.latest_import_status,
                    latest_publish_state=dataset.latest_publish_state,
                    canonical_published=dataset.canonical_published,
                )
                for dataset in readiness.datasets
            ],
            parcel_summary_ready=readiness.derived.parcel_summary_ready,
            search_support_ready=readiness.derived.search_support_ready,
            feature_ready=readiness.derived.feature_ready,
            comp_ready=readiness.derived.comp_ready,
            quote_ready=readiness.derived.quote_ready,
        )

    def _build_phases(
        self,
        *,
        capabilities: list[CountyCapabilityEntry],
        validation_candidate: HistoricalValidationCandidate | None,
        validation_readiness: CountyTaxYearReadiness | None,
    ) -> list[OnboardingPhase]:
        return [
            self._build_capability_phase(capabilities),
            self._build_validation_year_phase(validation_candidate),
            self._build_prep_phase(validation_readiness),
            self._build_publish_phase(validation_readiness),
            self._build_search_phase(validation_readiness),
            self._build_quote_phase(validation_readiness, capabilities),
        ]

    def _build_capability_phase(
        self,
        capabilities: list[CountyCapabilityEntry],
    ) -> OnboardingPhase:
        if not capabilities:
            return OnboardingPhase(
                phase_code="capability_review",
                label="Capability review",
                status="blocked",
                blocking=True,
                summary="County capability matrix is missing.",
                details=["Add explicit county capability entries before onboarding validation."],
            )
        return OnboardingPhase(
            phase_code="capability_review",
            label="Capability review",
            status="done",
            blocking=False,
            summary=f"{len(capabilities)} county capability entries are available for operator review.",
            details=[
                f"{capability.capability_code}:{capability.status}"
                for capability in capabilities
            ],
        )

    def _build_validation_year_phase(
        self,
        candidate: HistoricalValidationCandidate | None,
    ) -> OnboardingPhase:
        if candidate is None:
            return OnboardingPhase(
                phase_code="validation_year_selection",
                label="Validation year selection",
                status="blocked",
                blocking=True,
                summary="No candidate validation tax year was available.",
            )
        if candidate.recommended_for_qa:
            return OnboardingPhase(
                phase_code="validation_year_selection",
                label="Validation year selection",
                status="done",
                blocking=False,
                summary=f"Use tax year {candidate.tax_year} as the repeatable onboarding QA baseline.",
                details=list(candidate.caveats),
            )
        return OnboardingPhase(
            phase_code="validation_year_selection",
            label="Validation year selection",
            status="pending",
            blocking=True,
            summary=(
                f"Tax year {candidate.tax_year} is the best available validation baseline, "
                "but it is not yet fully recommended for QA."
            ),
            details=list(candidate.caveats),
        )

    def _build_prep_phase(
        self,
        readiness: CountyTaxYearReadiness | None,
    ) -> OnboardingPhase:
        if readiness is None:
            return OnboardingPhase(
                phase_code="dataset_prep_contract",
                label="Dataset prep contract",
                status="blocked",
                blocking=True,
                summary="Validation-year readiness data is missing.",
            )

        details: list[str] = []
        manual_prep_needed = False
        for dataset_type in REQUIRED_ONBOARDING_DATASETS:
            dataset = self._dataset_by_type(readiness, dataset_type)
            if dataset is None:
                details.append(f"{dataset_type}:missing_from_readiness")
                continue
            if dataset.canonical_published:
                details.append(f"{dataset_type}:canonical_published")
            elif dataset.raw_file_count > 0:
                details.append(f"{dataset_type}:raw_files_present")
            elif dataset.availability_status == "manual_upload_required":
                manual_prep_needed = True
                details.append(f"{dataset_type}:manual_prep_required")
            else:
                details.append(f"{dataset_type}:{dataset.availability_status}")

        if all(
            (dataset := self._dataset_by_type(readiness, dataset_type)) is not None
            and dataset.canonical_published
            for dataset_type in REQUIRED_ONBOARDING_DATASETS
        ):
            return OnboardingPhase(
                phase_code="dataset_prep_contract",
                label="Dataset prep contract",
                status="done",
                blocking=False,
                summary="Required onboarding datasets are already canonically published for the validation year.",
                details=details,
            )
        summary = (
            "Adapter-ready files and prep manifests are still required before repeatable onboarding validation."
            if manual_prep_needed
            else "Required onboarding datasets are not yet fully prepared for validation."
        )
        return OnboardingPhase(
            phase_code="dataset_prep_contract",
            label="Dataset prep contract",
            status="pending",
            blocking=True,
            summary=summary,
            details=details,
        )

    def _build_publish_phase(
        self,
        readiness: CountyTaxYearReadiness | None,
    ) -> OnboardingPhase:
        if readiness is None:
            return OnboardingPhase(
                phase_code="canonical_publish_validation",
                label="Canonical publish validation",
                status="blocked",
                blocking=True,
                summary="Validation-year readiness data is missing.",
            )

        details = []
        published_count = 0
        for dataset_type in REQUIRED_ONBOARDING_DATASETS:
            dataset = self._dataset_by_type(readiness, dataset_type)
            if dataset is None:
                details.append(f"{dataset_type}:missing_from_readiness")
                continue
            if dataset.canonical_published:
                published_count += 1
                details.append(f"{dataset_type}:published")
            else:
                details.append(
                    f"{dataset_type}:{dataset.latest_import_status or 'missing'}:{dataset.latest_publish_state or 'draft'}"
                )
        if published_count == len(REQUIRED_ONBOARDING_DATASETS):
            return OnboardingPhase(
                phase_code="canonical_publish_validation",
                label="Canonical publish validation",
                status="done",
                blocking=False,
                summary="Required onboarding datasets are canonically published for the validation year.",
                details=details,
            )
        return OnboardingPhase(
            phase_code="canonical_publish_validation",
            label="Canonical publish validation",
            status="pending",
            blocking=True,
            summary="Canonical publish is still incomplete for the required onboarding datasets.",
            details=details,
        )

    def _build_search_phase(
        self,
        readiness: CountyTaxYearReadiness | None,
    ) -> OnboardingPhase:
        if readiness is None:
            return OnboardingPhase(
                phase_code="searchable_validation",
                label="Searchable validation",
                status="blocked",
                blocking=True,
                summary="Validation-year readiness data is missing.",
            )
        if readiness.derived.parcel_summary_ready and readiness.derived.search_support_ready:
            return OnboardingPhase(
                phase_code="searchable_validation",
                label="Searchable validation",
                status="done",
                blocking=False,
                summary="Parcel summary and searchable read models are ready for onboarding validation.",
                details=[
                    "parcel_summary_ready",
                    "search_support_ready",
                ],
            )
        details = []
        if not readiness.derived.parcel_summary_ready:
            details.append("parcel_summary_not_ready")
        if not readiness.derived.search_support_ready:
            details.append("search_support_not_ready")
        return OnboardingPhase(
            phase_code="searchable_validation",
            label="Searchable validation",
            status="pending",
            blocking=True,
            summary="Searchable/read-model validation is not ready yet for the onboarding baseline year.",
            details=details,
        )

    def _build_quote_phase(
        self,
        readiness: CountyTaxYearReadiness | None,
        capabilities: list[CountyCapabilityEntry],
    ) -> OnboardingPhase:
        if readiness is None:
            return OnboardingPhase(
                phase_code="quote_supportability_validation",
                label="Quote supportability validation",
                status="blocked",
                blocking=True,
                summary="Validation-year readiness data is missing.",
            )
        if readiness.derived.quote_ready:
            return OnboardingPhase(
                phase_code="quote_supportability_validation",
                label="Quote supportability validation",
                status="done",
                blocking=False,
                summary="Quote-supportable read models are ready for onboarding validation.",
            )
        limiting_capabilities = [
            capability.capability_code
            for capability in capabilities
            if capability.status in {"limited", "manual_only", "unsupported"}
        ]
        details = ["quote_read_model_not_ready"]
        details.extend(f"capability_limit:{capability_code}" for capability_code in limiting_capabilities)
        return OnboardingPhase(
            phase_code="quote_supportability_validation",
            label="Quote supportability validation",
            status="pending",
            blocking=False,
            summary=(
                "Quote validation is not fully ready yet; use county capability entries to decide "
                "whether the gap is expected or a real onboarding defect."
            ),
            details=details,
        )

    def _dataset_by_type(
        self,
        readiness: CountyTaxYearReadiness,
        dataset_type: str,
    ):
        return next((dataset for dataset in readiness.datasets if dataset.dataset_type == dataset_type), None)

    def _candidate_capabilities(
        self,
        candidate: HistoricalValidationCandidate,
    ) -> dict[str, bool]:
        capabilities = candidate.capabilities
        return {
            "parcel_summary_validation_ready": capabilities.parcel_summary_validation_ready,
            "parcel_trend_validation_ready": capabilities.parcel_trend_validation_ready,
            "neighborhood_stats_validation_ready": capabilities.neighborhood_stats_validation_ready,
            "neighborhood_trend_validation_ready": capabilities.neighborhood_trend_validation_ready,
            "feature_validation_ready": capabilities.feature_validation_ready,
            "comp_validation_ready": capabilities.comp_validation_ready,
            "valuation_validation_ready": capabilities.valuation_validation_ready,
            "savings_validation_ready": capabilities.savings_validation_ready,
            "decision_tree_validation_ready": capabilities.decision_tree_validation_ready,
            "explanation_validation_ready": capabilities.explanation_validation_ready,
            "recommendation_validation_ready": capabilities.recommendation_validation_ready,
            "quote_read_model_validation_ready": capabilities.quote_read_model_validation_ready,
        }

    def _build_recommended_actions(
        self,
        *,
        county_id: str,
        tax_years: list[int],
        current_tax_year: int,
        validation_tax_year: int | None,
        phases: list[OnboardingPhase],
    ) -> list[OnboardingAction]:
        actions: list[OnboardingAction] = []
        tax_year_args = " ".join(str(tax_year) for tax_year in tax_years)
        validation_year = validation_tax_year or current_tax_year

        for phase in phases:
            if phase.status == "done":
                continue
            if phase.phase_code == "validation_year_selection":
                actions.append(
                    OnboardingAction(
                        action_code="review_validation_year_ranking",
                        phase_code=phase.phase_code,
                        blocking=phase.blocking,
                        summary="Review historical validation ranking and confirm the repeatable QA baseline year.",
                        command_hint=(
                            "python3 -m infra.scripts.report_historical_validation "
                            f"--county-id {county_id} --tax-years {tax_year_args} "
                            f"--current-tax-year {current_tax_year}"
                        ),
                    )
                )
            elif phase.phase_code == "dataset_prep_contract":
                actions.append(
                    OnboardingAction(
                        action_code="prepare_adapter_ready_files",
                        phase_code=phase.phase_code,
                        blocking=phase.blocking,
                        summary="Prepare adapter-ready files and manifests for the validation year before onboarding validation.",
                        command_hint=(
                            "python3 -m infra.scripts.prepare_manual_county_files "
                            f"--county-id {county_id} --tax-year {validation_year} --dataset-type both"
                        ),
                    )
                )
            elif phase.phase_code == "canonical_publish_validation":
                actions.append(
                    OnboardingAction(
                        action_code="run_bounded_backfill_publish",
                        phase_code=phase.phase_code,
                        blocking=phase.blocking,
                        summary="Run bounded historical backfill and canonical publish for the validation-year onboarding datasets.",
                        command_hint=(
                            "python3 -m infra.scripts.run_historical_backfill "
                            f"--counties {county_id} --tax-years {validation_year} "
                            "--dataset-types property_roll tax_rates --ready-root <ready-root>"
                        ),
                    )
                )
            elif phase.phase_code == "searchable_validation":
                actions.append(
                    OnboardingAction(
                        action_code="verify_searchable_read_models",
                        phase_code=phase.phase_code,
                        blocking=phase.blocking,
                        summary="Verify the validation year can be traced through ingestion into searchable/read-model outputs.",
                        command_hint=(
                            "python3 -m infra.scripts.verify_ingestion_to_searchable "
                            f"--county-id {county_id} --tax-year {validation_year}"
                        ),
                    )
                )
            elif phase.phase_code == "quote_supportability_validation":
                actions.append(
                    OnboardingAction(
                        action_code="review_quote_supportability",
                        phase_code=phase.phase_code,
                        blocking=phase.blocking,
                        summary="Review quote-supportability gaps against the capability matrix before treating them as onboarding defects.",
                        command_hint=(
                            "python3 -m infra.scripts.report_readiness_metrics "
                            f"--county-id {county_id} --tax-years {validation_year}"
                        ),
                    )
                )
        return actions

    def _build_onboarding_summary(
        self,
        phases: list[OnboardingPhase],
    ) -> OnboardingSummary:
        done_phase_count = sum(1 for phase in phases if phase.status == "done")
        pending_phase_count = sum(1 for phase in phases if phase.status == "pending")
        blocked_phase_count = sum(1 for phase in phases if phase.status == "blocked")
        blocking_phase_codes = [
            phase.phase_code
            for phase in phases
            if phase.blocking and phase.status in {"pending", "blocked"}
        ]
        next_phase_code = next(
            (phase.phase_code for phase in phases if phase.status != "done"),
            None,
        )
        next_blocking_phase_code = next(
            (
                phase.phase_code
                for phase in phases
                if phase.blocking and phase.status in {"pending", "blocked"}
            ),
            None,
        )
        if blocked_phase_count > 0:
            overall_status = "blocked"
        elif pending_phase_count > 0 and blocking_phase_codes:
            overall_status = "blocked"
        elif pending_phase_count > 0:
            overall_status = "partial"
        else:
            overall_status = "ready"
        return OnboardingSummary(
            overall_status=overall_status,
            done_phase_count=done_phase_count,
            pending_phase_count=pending_phase_count,
            blocked_phase_count=blocked_phase_count,
            blocking_phase_codes=blocking_phase_codes,
            next_phase_code=next_phase_code,
            next_blocking_phase_code=next_blocking_phase_code,
        )
