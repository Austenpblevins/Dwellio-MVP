from __future__ import annotations
from datetime import date
from typing import Optional
from uuid import UUID
from app.models.common import DwellioBaseModel

class ParcelSale(DwellioBaseModel):
    parcel_id: UUID
    county_id: str
    tax_year: Optional[int] = None
    sale_date: Optional[date] = None
    sale_price: Optional[float] = None
    list_price: Optional[float] = None
    days_on_market: Optional[int] = None
    sale_price_psf: Optional[float] = None
    time_adjusted_price: Optional[float] = None
    validity_code: Optional[str] = None
    arms_length_flag: Optional[bool] = None
    restricted_flag: bool = False

class NeighborhoodStats(DwellioBaseModel):
    county_id: str
    tax_year: int
    neighborhood_code: str
    property_type_code: str
    period_months: int
    sale_count: int
    median_sale_psf: Optional[float] = None
    median_sale_price: Optional[float] = None

class CompCandidate(DwellioBaseModel):
    comp_candidate_pool_id: UUID
    subject_parcel_id: UUID
    comp_parcel_id: UUID
    comp_type: str
    rank_num: Optional[int] = None
    similarity_score: Optional[float] = None
