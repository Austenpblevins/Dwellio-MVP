from __future__ import annotations

from uuid import UUID

from app.models.common import DwellioBaseModel


class CountyConfig(DwellioBaseModel):
    county_id: str
    tax_year: int | None = None
    parser_module_name: str | None = None
    protest_deadline_rule: str | None = None
    minimum_gap_threshold_pct: float = 0.05
    minimum_savings_threshold: float = 300.0

class ParcelBase(DwellioBaseModel):
    parcel_id: UUID | None = None
    county_id: str
    tax_year: int | None = None
    account_number: str
    cad_property_id: str | None = None
    situs_address: str | None = None
    situs_city: str | None = None
    situs_state: str | None = 'TX'
    situs_zip: str | None = None
    owner_name: str | None = None
    property_type_code: str | None = None
    property_class_code: str | None = None
    neighborhood_code: str | None = None
    subdivision_name: str | None = None
    school_district_name: str | None = None

class ParcelSearchResult(DwellioBaseModel):
    county_id: str
    account_number: str
    parcel_id: UUID
    address: str
    situs_zip: str | None = None
    owner_name: str | None = None
    match_score: float
