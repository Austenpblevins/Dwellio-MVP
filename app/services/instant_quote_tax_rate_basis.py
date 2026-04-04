from __future__ import annotations

from dataclasses import dataclass, replace

INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS = 20
TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES = "prior_year_adopted_rates"
TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES = (
    "current_year_unofficial_or_proposed_rates"
)
TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES = (
    "current_year_final_adopted_rates"
)
TAX_RATE_BASIS_STATUSES = frozenset(
    {
        TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES,
        TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES,
        TAX_RATE_BASIS_STATUS_CURRENT_YEAR_FINAL_ADOPTED_RATES,
    }
)


@dataclass(frozen=True)
class TaxRateBasisCandidate:
    tax_year: int
    supportable_subject_row_count: int


@dataclass(frozen=True)
class SameYearTaxRateAdoptionStatus:
    county_id: str
    tax_year: int
    adoption_status: str
    adoption_status_reason: str | None = None
    status_source: str | None = None


@dataclass(frozen=True)
class SelectedTaxRateBasis:
    quote_tax_year: int
    basis_tax_year: int | None
    fallback_applied: bool
    reason_code: str
    requested_year_supportable_subject_row_count: int
    selected_basis_supportable_subject_row_count: int
    basis_status: str | None = None
    basis_status_reason: str | None = None
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


def assign_tax_rate_basis_status(
    *,
    selection: SelectedTaxRateBasis,
    same_year_adoption_status: SameYearTaxRateAdoptionStatus | None = None,
) -> SelectedTaxRateBasis:
    if selection.basis_tax_year is None:
        return replace(
            selection,
            basis_status=None,
            basis_status_reason="no_usable_tax_rate_basis",
        )

    if selection.basis_tax_year < selection.quote_tax_year:
        return replace(
            selection,
            basis_status=TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES,
            basis_status_reason="basis_year_precedes_quote_year",
        )

    if same_year_adoption_status is not None:
        if same_year_adoption_status.tax_year != selection.basis_tax_year:
            raise ValueError("same-year adoption metadata tax year does not match basis tax year")
        if same_year_adoption_status.adoption_status not in TAX_RATE_BASIS_STATUSES:
            raise ValueError("unsupported same-year tax-rate adoption status")
        if same_year_adoption_status.adoption_status == TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES:
            raise ValueError("prior-year adopted status cannot be asserted for a same-year basis")
        return replace(
            selection,
            basis_status=same_year_adoption_status.adoption_status,
            basis_status_reason=(
                same_year_adoption_status.adoption_status_reason
                or "explicit_same_year_tax_rate_adoption_status"
            ),
        )

    return replace(
        selection,
        basis_status=TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES,
        basis_status_reason="same_year_rates_without_final_adoption_proof",
    )
