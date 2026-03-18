from __future__ import annotations

from datetime import date
from uuid import UUID

from app.models.common import DwellioBaseModel


class ParcelSale(DwellioBaseModel):
    parcel_id: UUID
    county_id: str
    tax_year: int | None = None
    sale_date: date | None = None
    sale_price: float | None = None
    list_price: float | None = None
    days_on_market: int | None = None
    sale_price_psf: float | None = None
    time_adjusted_price: float | None = None
    validity_code: str | None = None
    arms_length_flag: bool | None = None
    restricted_flag: bool = False

class NeighborhoodStats(DwellioBaseModel):
    county_id: str
    tax_year: int
    neighborhood_code: str
    property_type_code: str
    period_months: int
    sale_count: int
    median_sale_psf: float | None = None
    median_sale_price: float | None = None

class CompCandidate(DwellioBaseModel):
    comp_candidate_pool_id: UUID
    subject_parcel_id: UUID
    comp_parcel_id: UUID
    comp_type: str
    rank_num: int | None = None
    similarity_score: float | None = None
