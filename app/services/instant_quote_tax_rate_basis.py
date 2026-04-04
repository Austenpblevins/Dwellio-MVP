from __future__ import annotations

from dataclasses import dataclass

INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS = 20
INSTANT_QUOTE_TAX_RATE_BASIS_MIN_EFFECTIVE_TAX_RATE_COVERAGE_RATIO = 0.85
INSTANT_QUOTE_TAX_RATE_BASIS_MIN_ASSIGNMENT_COVERAGE_RATIO = 0.90
INSTANT_QUOTE_TAX_RATE_BASIS_MIN_PARCEL_CONTINUITY_RATIO = 0.85
INSTANT_QUOTE_TAX_RATE_BASIS_WARN_PARCEL_CONTINUITY_RATIO = 0.90


@dataclass(frozen=True)
class TaxRateBasisCandidate:
    tax_year: int
    quoteable_subject_row_count: int
    supportable_subject_row_count: int
    assignment_complete_row_count: int
    continuity_parcel_match_row_count: int
    continuity_account_number_match_row_count: int = 0

    @property
    def effective_tax_rate_coverage_ratio(self) -> float:
        return _safe_ratio(
            numerator=self.supportable_subject_row_count,
            denominator=self.quoteable_subject_row_count,
        )

    @property
    def assignment_coverage_ratio(self) -> float:
        return _safe_ratio(
            numerator=self.assignment_complete_row_count,
            denominator=self.quoteable_subject_row_count,
        )

    @property
    def continuity_parcel_gap_row_count(self) -> int:
        return max(self.quoteable_subject_row_count - self.continuity_parcel_match_row_count, 0)

    @property
    def continuity_parcel_match_ratio(self) -> float:
        return _safe_ratio(
            numerator=self.continuity_parcel_match_row_count,
            denominator=self.quoteable_subject_row_count,
        )


@dataclass(frozen=True)
class SelectedTaxRateBasis:
    quote_tax_year: int
    basis_tax_year: int | None
    fallback_applied: bool
    reason_code: str
    requested_year_supportable_subject_row_count: int
    selected_basis_supportable_subject_row_count: int
    quoteable_subject_row_count: int = 0
    requested_year_effective_tax_rate_coverage_ratio: float = 0.0
    requested_year_assignment_coverage_ratio: float = 0.0
    selected_basis_effective_tax_rate_coverage_ratio: float = 0.0
    selected_basis_assignment_coverage_ratio: float = 0.0
    selected_basis_continuity_parcel_match_row_count: int = 0
    selected_basis_continuity_parcel_gap_row_count: int = 0
    selected_basis_continuity_parcel_match_ratio: float = 0.0
    selected_basis_continuity_account_number_match_row_count: int = 0
    selected_basis_warning_codes: tuple[str, ...] = ()
    requested_year_blocker_codes: tuple[str, ...] = ()
    minimum_supportable_subject_row_count: int = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS
    )
    minimum_effective_tax_rate_coverage_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_EFFECTIVE_TAX_RATE_COVERAGE_RATIO
    )
    minimum_assignment_coverage_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_ASSIGNMENT_COVERAGE_RATIO
    )
    minimum_parcel_continuity_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_PARCEL_CONTINUITY_RATIO
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
    minimum_effective_tax_rate_coverage_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_EFFECTIVE_TAX_RATE_COVERAGE_RATIO
    ),
    minimum_assignment_coverage_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_ASSIGNMENT_COVERAGE_RATIO
    ),
    minimum_parcel_continuity_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_MIN_PARCEL_CONTINUITY_RATIO
    ),
    warning_parcel_continuity_ratio: float = (
        INSTANT_QUOTE_TAX_RATE_BASIS_WARN_PARCEL_CONTINUITY_RATIO
    ),
) -> SelectedTaxRateBasis:
    deduped_candidates: dict[int, TaxRateBasisCandidate] = {}
    for candidate in candidates:
        if candidate.tax_year > quote_tax_year:
            continue
        existing_candidate = deduped_candidates.get(candidate.tax_year)
        if existing_candidate is None or _candidate_sort_key(candidate) > _candidate_sort_key(
            existing_candidate
        ):
            deduped_candidates[candidate.tax_year] = candidate

    requested_candidate = deduped_candidates.get(
        quote_tax_year,
        TaxRateBasisCandidate(
            tax_year=quote_tax_year,
            quoteable_subject_row_count=0,
            supportable_subject_row_count=0,
            assignment_complete_row_count=0,
            continuity_parcel_match_row_count=0,
            continuity_account_number_match_row_count=0,
        ),
    )
    requested_blockers = _candidate_blocker_codes(
        candidate=requested_candidate,
        quote_tax_year=quote_tax_year,
        minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
        minimum_effective_tax_rate_coverage_ratio=minimum_effective_tax_rate_coverage_ratio,
        minimum_assignment_coverage_ratio=minimum_assignment_coverage_ratio,
        minimum_parcel_continuity_ratio=minimum_parcel_continuity_ratio,
    )
    if not requested_blockers:
        return _build_selected_basis(
            quote_tax_year=quote_tax_year,
            basis_candidate=requested_candidate,
            fallback_applied=False,
            reason_code="requested_year_usable",
            requested_candidate=requested_candidate,
            requested_blocker_codes=requested_blockers,
            warning_parcel_continuity_ratio=warning_parcel_continuity_ratio,
            minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
            minimum_effective_tax_rate_coverage_ratio=minimum_effective_tax_rate_coverage_ratio,
            minimum_assignment_coverage_ratio=minimum_assignment_coverage_ratio,
            minimum_parcel_continuity_ratio=minimum_parcel_continuity_ratio,
        )

    prior_candidates = sorted(
        (
            candidate
            for candidate in deduped_candidates.values()
            if candidate.tax_year < quote_tax_year
            and not _candidate_blocker_codes(
                candidate=candidate,
                quote_tax_year=quote_tax_year,
                minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
                minimum_effective_tax_rate_coverage_ratio=minimum_effective_tax_rate_coverage_ratio,
                minimum_assignment_coverage_ratio=minimum_assignment_coverage_ratio,
                minimum_parcel_continuity_ratio=minimum_parcel_continuity_ratio,
            )
        ),
        key=lambda candidate: candidate.tax_year,
    )
    if prior_candidates:
        selected_candidate = prior_candidates[-1]
        return _build_selected_basis(
            quote_tax_year=quote_tax_year,
            basis_candidate=selected_candidate,
            fallback_applied=True,
            reason_code=_fallback_reason_for_requested_candidate(requested_candidate),
            requested_candidate=requested_candidate,
            requested_blocker_codes=requested_blockers,
            warning_parcel_continuity_ratio=warning_parcel_continuity_ratio,
            minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
            minimum_effective_tax_rate_coverage_ratio=minimum_effective_tax_rate_coverage_ratio,
            minimum_assignment_coverage_ratio=minimum_assignment_coverage_ratio,
            minimum_parcel_continuity_ratio=minimum_parcel_continuity_ratio,
        )

    return SelectedTaxRateBasis(
        quote_tax_year=quote_tax_year,
        basis_tax_year=None,
        fallback_applied=False,
        reason_code="no_usable_tax_rate_basis",
        requested_year_supportable_subject_row_count=requested_candidate.supportable_subject_row_count,
        selected_basis_supportable_subject_row_count=0,
        quoteable_subject_row_count=requested_candidate.quoteable_subject_row_count,
        requested_year_effective_tax_rate_coverage_ratio=(
            requested_candidate.effective_tax_rate_coverage_ratio
        ),
        requested_year_assignment_coverage_ratio=requested_candidate.assignment_coverage_ratio,
        requested_year_blocker_codes=requested_blockers,
        minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
        minimum_effective_tax_rate_coverage_ratio=minimum_effective_tax_rate_coverage_ratio,
        minimum_assignment_coverage_ratio=minimum_assignment_coverage_ratio,
        minimum_parcel_continuity_ratio=minimum_parcel_continuity_ratio,
    )


def _build_selected_basis(
    *,
    quote_tax_year: int,
    basis_candidate: TaxRateBasisCandidate,
    fallback_applied: bool,
    reason_code: str,
    requested_candidate: TaxRateBasisCandidate,
    requested_blocker_codes: tuple[str, ...],
    warning_parcel_continuity_ratio: float,
    minimum_supportable_subject_row_count: int,
    minimum_effective_tax_rate_coverage_ratio: float,
    minimum_assignment_coverage_ratio: float,
    minimum_parcel_continuity_ratio: float,
) -> SelectedTaxRateBasis:
    warning_codes = _candidate_warning_codes(
        candidate=basis_candidate,
        quote_tax_year=quote_tax_year,
        warning_parcel_continuity_ratio=warning_parcel_continuity_ratio,
    )
    return SelectedTaxRateBasis(
        quote_tax_year=quote_tax_year,
        basis_tax_year=basis_candidate.tax_year,
        fallback_applied=fallback_applied,
        reason_code=reason_code,
        requested_year_supportable_subject_row_count=requested_candidate.supportable_subject_row_count,
        selected_basis_supportable_subject_row_count=basis_candidate.supportable_subject_row_count,
        quoteable_subject_row_count=requested_candidate.quoteable_subject_row_count,
        requested_year_effective_tax_rate_coverage_ratio=(
            requested_candidate.effective_tax_rate_coverage_ratio
        ),
        requested_year_assignment_coverage_ratio=requested_candidate.assignment_coverage_ratio,
        selected_basis_effective_tax_rate_coverage_ratio=(
            basis_candidate.effective_tax_rate_coverage_ratio
        ),
        selected_basis_assignment_coverage_ratio=basis_candidate.assignment_coverage_ratio,
        selected_basis_continuity_parcel_match_row_count=(
            basis_candidate.continuity_parcel_match_row_count
        ),
        selected_basis_continuity_parcel_gap_row_count=(
            basis_candidate.continuity_parcel_gap_row_count
        ),
        selected_basis_continuity_parcel_match_ratio=(
            basis_candidate.continuity_parcel_match_ratio
        ),
        selected_basis_continuity_account_number_match_row_count=(
            basis_candidate.continuity_account_number_match_row_count
        ),
        selected_basis_warning_codes=warning_codes,
        requested_year_blocker_codes=requested_blocker_codes,
        minimum_supportable_subject_row_count=minimum_supportable_subject_row_count,
        minimum_effective_tax_rate_coverage_ratio=minimum_effective_tax_rate_coverage_ratio,
        minimum_assignment_coverage_ratio=minimum_assignment_coverage_ratio,
        minimum_parcel_continuity_ratio=minimum_parcel_continuity_ratio,
    )


def _candidate_blocker_codes(
    *,
    candidate: TaxRateBasisCandidate,
    quote_tax_year: int,
    minimum_supportable_subject_row_count: int,
    minimum_effective_tax_rate_coverage_ratio: float,
    minimum_assignment_coverage_ratio: float,
    minimum_parcel_continuity_ratio: float,
) -> tuple[str, ...]:
    blocker_codes: list[str] = []
    if candidate.supportable_subject_row_count < minimum_supportable_subject_row_count:
        if candidate.supportable_subject_row_count <= 0:
            blocker_codes.append("missing_supportable_subjects")
        else:
            blocker_codes.append("below_supportable_subject_floor")
    if candidate.effective_tax_rate_coverage_ratio < minimum_effective_tax_rate_coverage_ratio:
        blocker_codes.append("below_effective_tax_rate_coverage_threshold")
    if candidate.assignment_coverage_ratio < minimum_assignment_coverage_ratio:
        blocker_codes.append("below_tax_assignment_coverage_threshold")
    if (
        candidate.tax_year < quote_tax_year
        and candidate.continuity_parcel_match_ratio < minimum_parcel_continuity_ratio
    ):
        blocker_codes.append("below_parcel_continuity_threshold")
    return tuple(blocker_codes)


def _candidate_warning_codes(
    *,
    candidate: TaxRateBasisCandidate,
    quote_tax_year: int,
    warning_parcel_continuity_ratio: float,
) -> tuple[str, ...]:
    warning_codes: list[str] = []
    if (
        candidate.tax_year < quote_tax_year
        and candidate.continuity_parcel_match_ratio < warning_parcel_continuity_ratio
    ):
        warning_codes.append("parcel_continuity_warning")
    if (
        candidate.tax_year < quote_tax_year
        and candidate.continuity_account_number_match_row_count > 0
    ):
        warning_codes.append("account_number_continuity_diagnostic")
    return tuple(warning_codes)


def _fallback_reason_for_requested_candidate(candidate: TaxRateBasisCandidate) -> str:
    if candidate.supportable_subject_row_count <= 0:
        return "fallback_requested_year_missing_supportable_subjects"
    if (
        candidate.supportable_subject_row_count
        < INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS
    ):
        return "fallback_requested_year_below_support_threshold"
    if (
        candidate.effective_tax_rate_coverage_ratio
        < INSTANT_QUOTE_TAX_RATE_BASIS_MIN_EFFECTIVE_TAX_RATE_COVERAGE_RATIO
    ):
        return "fallback_requested_year_below_effective_coverage_threshold"
    if (
        candidate.assignment_coverage_ratio
        < INSTANT_QUOTE_TAX_RATE_BASIS_MIN_ASSIGNMENT_COVERAGE_RATIO
    ):
        return "fallback_requested_year_below_assignment_coverage_threshold"
    return "fallback_requested_year_not_usable"


def _candidate_sort_key(candidate: TaxRateBasisCandidate) -> tuple[int, int, int, int]:
    return (
        candidate.supportable_subject_row_count,
        candidate.assignment_complete_row_count,
        candidate.continuity_parcel_match_row_count,
        candidate.continuity_account_number_match_row_count,
    )


def _safe_ratio(*, numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)
