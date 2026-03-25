from __future__ import annotations

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
