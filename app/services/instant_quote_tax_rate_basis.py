from __future__ import annotations

from dataclasses import dataclass

INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS = 20


@dataclass(frozen=True)
class TaxRateBasisCandidate:
    tax_year: int
    supportable_subject_row_count: int


@dataclass(frozen=True)
class SelectedTaxRateBasis:
    quote_tax_year: int
    basis_tax_year: int | None
    fallback_applied: bool
    reason_code: str
    requested_year_supportable_subject_row_count: int
    selected_basis_supportable_subject_row_count: int
    minimum_supportable_subject_row_count: int = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS
    )

    @property
    def usable(self) -> bool:
        return self.basis_tax_year is not None


def choose_tax_rate_basis(
    *,
    quote_tax_year: int,
    candidates: list[TaxRateBasisCandidate],
    minimum_supportable_subject_row_count: int = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS
    ),
) -> SelectedTaxRateBasis:
    candidate_counts: dict[int, int] = {}
    for candidate in candidates:
        if candidate.tax_year > quote_tax_year:
            continue
        candidate_counts[candidate.tax_year] = max(
            int(candidate.supportable_subject_row_count),
            candidate_counts.get(candidate.tax_year, 0),
        )

    requested_count = candidate_counts.get(quote_tax_year, 0)
    if requested_count >= minimum_supportable_subject_row_count:
        return SelectedTaxRateBasis(
            quote_tax_year=quote_tax_year,
            basis_tax_year=quote_tax_year,
            fallback_applied=False,
            reason_code="requested_year_usable",
            requested_year_supportable_subject_row_count=requested_count,
            selected_basis_supportable_subject_row_count=requested_count,
            minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
        )

    prior_usable_years = sorted(
        (
            candidate_year,
            count,
        )
        for candidate_year, count in candidate_counts.items()
        if candidate_year < quote_tax_year and count >= minimum_supportable_subject_row_count
    )
    if prior_usable_years:
        basis_tax_year, selected_count = prior_usable_years[-1]
        return SelectedTaxRateBasis(
            quote_tax_year=quote_tax_year,
            basis_tax_year=basis_tax_year,
            fallback_applied=True,
            reason_code=(
                "fallback_requested_year_missing_supportable_subjects"
                if requested_count <= 0
                else "fallback_requested_year_below_support_threshold"
            ),
            requested_year_supportable_subject_row_count=requested_count,
            selected_basis_supportable_subject_row_count=selected_count,
            minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
        )

    return SelectedTaxRateBasis(
        quote_tax_year=quote_tax_year,
        basis_tax_year=None,
        fallback_applied=False,
        reason_code="no_usable_tax_rate_basis",
        requested_year_supportable_subject_row_count=requested_count,
        selected_basis_supportable_subject_row_count=0,
        minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
    )
