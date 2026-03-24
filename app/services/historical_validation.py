from __future__ import annotations

from dataclasses import dataclass, field

from app.services.data_readiness import CountyTaxYearReadiness


@dataclass(frozen=True)
class HistoricalValidationCapabilities:
    parcel_summary_validation_ready: bool
    parcel_trend_validation_ready: bool
    neighborhood_stats_validation_ready: bool
    neighborhood_trend_validation_ready: bool
    feature_validation_ready: bool
    comp_validation_ready: bool
    valuation_validation_ready: bool
    savings_validation_ready: bool
    decision_tree_validation_ready: bool
    explanation_validation_ready: bool
    recommendation_validation_ready: bool
    quote_read_model_validation_ready: bool


@dataclass(frozen=True)
class HistoricalValidationCandidate:
    county_id: str
    tax_year: int
    readiness_score: int
    recommended_for_qa: bool
    capabilities: HistoricalValidationCapabilities
    caveats: list[str] = field(default_factory=list)


class HistoricalValidationService:
    def rank_validation_years(
        self,
        readiness_items: list[CountyTaxYearReadiness],
        *,
        current_tax_year: int | None = None,
    ) -> list[HistoricalValidationCandidate]:
        candidates = [
            self._build_candidate(readiness, current_tax_year=current_tax_year)
            for readiness in readiness_items
        ]
        return sorted(
            candidates,
            key=lambda candidate: (
                candidate.recommended_for_qa,
                candidate.readiness_score,
                candidate.tax_year,
            ),
            reverse=True,
        )

    def _build_candidate(
        self,
        readiness: CountyTaxYearReadiness,
        *,
        current_tax_year: int | None,
    ) -> HistoricalValidationCandidate:
        derived = readiness.derived
        capabilities = HistoricalValidationCapabilities(
            parcel_summary_validation_ready=derived.parcel_summary_ready,
            parcel_trend_validation_ready=derived.parcel_year_trend_ready,
            neighborhood_stats_validation_ready=derived.neighborhood_stats_ready,
            neighborhood_trend_validation_ready=derived.neighborhood_year_trend_ready,
            feature_validation_ready=derived.feature_ready and derived.parcel_year_trend_ready,
            comp_validation_ready=derived.comp_ready,
            valuation_validation_ready=derived.valuation_ready,
            savings_validation_ready=derived.savings_ready,
            decision_tree_validation_ready=derived.decision_tree_ready,
            explanation_validation_ready=derived.explanation_ready,
            recommendation_validation_ready=derived.recommendation_ready,
            quote_read_model_validation_ready=derived.quote_ready,
        )

        published_dataset_count = sum(1 for dataset in readiness.datasets if dataset.canonical_published)
        derived_ready_count = sum(
            1
            for flag in (
                capabilities.parcel_summary_validation_ready,
                capabilities.parcel_trend_validation_ready,
                capabilities.neighborhood_stats_validation_ready,
                capabilities.neighborhood_trend_validation_ready,
                capabilities.feature_validation_ready,
                capabilities.comp_validation_ready,
                capabilities.valuation_validation_ready,
                capabilities.savings_validation_ready,
                capabilities.decision_tree_validation_ready,
                capabilities.explanation_validation_ready,
                capabilities.recommendation_validation_ready,
                capabilities.quote_read_model_validation_ready,
            )
            if flag
        )
        prior_year_bonus = (
            8 if current_tax_year is not None and readiness.tax_year < current_tax_year else 0
        )
        current_year_penalty = (
            -4 if current_tax_year is not None and readiness.tax_year == current_tax_year else 0
        )
        readiness_score = (published_dataset_count * 10) + (derived_ready_count * 4) + prior_year_bonus + current_year_penalty

        caveats: list[str] = []
        if not readiness.tax_year_known:
            caveats.append("tax_year_missing")
        if not capabilities.parcel_summary_validation_ready:
            caveats.append("parcel_summary_not_ready")
        if not capabilities.neighborhood_stats_validation_ready:
            caveats.append("neighborhood_stats_not_ready")
        if not capabilities.comp_validation_ready:
            caveats.append("comp_generation_not_ready")
        if not capabilities.quote_read_model_validation_ready:
            caveats.append("quote_read_model_not_ready")
        if current_tax_year is not None and readiness.tax_year == current_tax_year:
            caveats.append("current_year_may_be_sparse")
        if any(
            dataset.availability_status == "manual_upload_required" and dataset.raw_file_count == 0
            for dataset in readiness.datasets
        ):
            caveats.append("historical_manual_backfill_missing")

        recommended_for_qa = (
            readiness.tax_year_known
            and capabilities.parcel_summary_validation_ready
            and published_dataset_count >= 2
            and (current_tax_year is None or readiness.tax_year < current_tax_year)
        )

        return HistoricalValidationCandidate(
            county_id=readiness.county_id,
            tax_year=readiness.tax_year,
            readiness_score=readiness_score,
            recommended_for_qa=recommended_for_qa,
            capabilities=capabilities,
            caveats=caveats,
        )
