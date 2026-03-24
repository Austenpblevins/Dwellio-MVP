from __future__ import annotations

from app.services.data_readiness import (
    CountyTaxYearReadiness,
    DatasetYearReadiness,
    TaxYearDerivedReadiness,
)
from app.services.historical_validation import HistoricalValidationService


def _dataset(dataset_type: str, *, tax_year: int, canonical_published: bool) -> DatasetYearReadiness:
    return DatasetYearReadiness(
        county_id="harris",
        tax_year=tax_year,
        dataset_type=dataset_type,
        source_system_code=f"{dataset_type}_source",
        access_method="manual_upload",
        availability_status="manual_upload_required",
        tax_year_known=True,
        canonical_published=canonical_published,
    )


def test_rank_validation_years_prefers_fuller_prior_years() -> None:
    service = HistoricalValidationService()
    ranked = service.rank_validation_years(
        [
            CountyTaxYearReadiness(
                county_id="harris",
                tax_year=2026,
                tax_year_known=True,
                datasets=[
                    _dataset("property_roll", tax_year=2026, canonical_published=False),
                    _dataset("tax_rates", tax_year=2026, canonical_published=False),
                    _dataset("deeds", tax_year=2026, canonical_published=False),
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=False,
                    parcel_year_trend_ready=False,
                    neighborhood_stats_ready=False,
                    neighborhood_year_trend_ready=False,
                    search_support_ready=False,
                    feature_ready=False,
                    comp_ready=False,
                    valuation_ready=False,
                    savings_ready=False,
                    decision_tree_ready=False,
                    explanation_ready=False,
                    recommendation_ready=False,
                    quote_ready=False,
                ),
            ),
            CountyTaxYearReadiness(
                county_id="harris",
                tax_year=2025,
                tax_year_known=True,
                datasets=[
                    _dataset("property_roll", tax_year=2025, canonical_published=True),
                    _dataset("tax_rates", tax_year=2025, canonical_published=True),
                    _dataset("deeds", tax_year=2025, canonical_published=True),
                ],
                derived=TaxYearDerivedReadiness(
                    parcel_summary_ready=True,
                    parcel_year_trend_ready=True,
                    neighborhood_stats_ready=True,
                    neighborhood_year_trend_ready=True,
                    search_support_ready=True,
                    feature_ready=True,
                    comp_ready=False,
                    valuation_ready=True,
                    savings_ready=True,
                    decision_tree_ready=True,
                    explanation_ready=True,
                    recommendation_ready=True,
                    quote_ready=False,
                ),
            ),
        ],
        current_tax_year=2026,
    )

    assert ranked[0].tax_year == 2025
    assert ranked[0].recommended_for_qa is True
    assert "comp_generation_not_ready" in ranked[0].caveats
    assert "current_year_may_be_sparse" in ranked[1].caveats
