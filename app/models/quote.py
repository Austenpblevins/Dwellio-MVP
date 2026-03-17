from __future__ import annotations
from typing import List, Optional
from uuid import UUID
from pydantic import Field
from app.models.common import DwellioBaseModel, JsonDict
from app.models.parcel import ParcelSearchResult

class SearchRequest(DwellioBaseModel):
    address: str = Field(..., min_length=3)

class SearchResponse(DwellioBaseModel):
    results: List[ParcelSearchResult]

class QuoteResponse(DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    parcel_id: UUID
    address: str
    current_notice_value: Optional[float] = None
    market_value_point: Optional[float] = None
    equity_value_point: Optional[float] = None
    defensible_value_point: Optional[float] = None
    gross_tax_savings_point: Optional[float] = None
    expected_tax_savings_point: Optional[float] = None
    expected_tax_savings_low: Optional[float] = None
    expected_tax_savings_high: Optional[float] = None
    estimated_contingency_fee: Optional[float] = None
    confidence: Optional[str] = None
    basis: Optional[str] = None
    protest_recommendation: Optional[str] = None
    explanation_json: JsonDict = Field(default_factory=dict)
    explanation_bullets: List[str] = Field(default_factory=list)
