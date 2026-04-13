from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import Field

from app.models.common import DwellioBaseModel, JsonDict, TaxYearFallbackMetadata
from app.models.parcel import ParcelAutocompleteResponse, ParcelSearchResult


class SearchRequest(DwellioBaseModel):
    address: str = Field(..., min_length=3)


class SearchResponse(DwellioBaseModel):
    results: list[ParcelSearchResult]


class AutocompleteResponse(ParcelAutocompleteResponse):
    pass


class QuoteResponse(TaxYearFallbackMetadata, DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    parcel_id: UUID
    address: str
    current_notice_value: float | None = None
    market_value_point: float | None = None
    equity_value_point: float | None = None
    defensible_value_point: float | None = None
    gross_tax_savings_point: float | None = None
    expected_tax_savings_point: float | None = None
    expected_tax_savings_low: float | None = None
    expected_tax_savings_high: float | None = None
    estimated_contingency_fee: float | None = None
    confidence: str | None = None
    basis: str | None = None
    protest_recommendation: str | None = None
    explanation_json: JsonDict = Field(default_factory=dict)
    explanation_bullets: list[str] = Field(default_factory=list)


class QuoteExplanationResponse(TaxYearFallbackMetadata, DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    explanation_json: JsonDict = Field(default_factory=dict)
    explanation_bullets: list[str] = Field(default_factory=list)


InstantQuoteEstimateStrengthLabel = Literal["high", "medium", "low"]
InstantQuoteUnsupportedReason = Literal[
    "unsupported_property_type",
    "missing_living_area",
    "missing_assessment_basis",
    "missing_neighborhood_code",
    "missing_effective_tax_rate",
    "instant_quote_not_ready",
    "thin_market_support",
    "low_confidence_refined_review",
    "tax_limitation_uncertain",
    "implausible_savings_outlier",
]


class InstantQuoteSubject(DwellioBaseModel):
    parcel_id: UUID
    address: str
    neighborhood_code: str | None = None
    school_district_name: str | None = None
    property_type_code: str | None = None
    property_class_code: str | None = None
    living_area_sf: float | None = None
    year_built: int | None = None
    notice_value: float | None = None
    homestead_flag: bool | None = None
    freeze_flag: bool | None = None


class InstantQuoteEstimate(DwellioBaseModel):
    savings_range_low: float | None = None
    savings_range_high: float | None = None
    savings_midpoint_display: float | None = None
    estimate_bucket: str | None = None
    estimate_strength_label: InstantQuoteEstimateStrengthLabel | None = None
    tax_protection_limited: bool = False
    tax_protection_note: str | None = None


class InstantQuoteExplanation(DwellioBaseModel):
    methodology: str
    estimate_strength_label: InstantQuoteEstimateStrengthLabel | None = None
    summary: str
    bullets: list[str] = Field(default_factory=list)
    limitation_note: str | None = None


class InstantQuoteResponse(TaxYearFallbackMetadata, DwellioBaseModel):
    supported: bool
    county_id: str
    tax_year: int
    account_number: str
    basis_code: str
    subject: InstantQuoteSubject | None = None
    estimate: InstantQuoteEstimate | None = None
    explanation: InstantQuoteExplanation
    disclaimers: list[str] = Field(default_factory=list)
    unsupported_reason: InstantQuoteUnsupportedReason | None = None
    next_step_cta: str | None = None
