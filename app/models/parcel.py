from __future__ import annotations
from typing import Optional
from uuid import UUID
from app.models.common import DwellioBaseModel

class CountyConfig(DwellioBaseModel):
    county_id: str
    tax_year: Optional[int] = None
    parser_module_name: Optional[str] = None
    protest_deadline_rule: Optional[str] = None
    minimum_gap_threshold_pct: float = 0.05
    minimum_savings_threshold: float = 300.0

class ParcelBase(DwellioBaseModel):
    parcel_id: Optional[UUID] = None
    county_id: str
    tax_year: Optional[int] = None
    account_number: str
    cad_property_id: Optional[str] = None
    situs_address: Optional[str] = None
    situs_city: Optional[str] = None
    situs_state: Optional[str] = 'TX'
    situs_zip: Optional[str] = None
    owner_name: Optional[str] = None
    property_type_code: Optional[str] = None
    property_class_code: Optional[str] = None
    neighborhood_code: Optional[str] = None
    subdivision_name: Optional[str] = None
    school_district_name: Optional[str] = None

class ParcelSearchResult(DwellioBaseModel):
    county_id: str
    account_number: str
    parcel_id: UUID
    address: str
    situs_zip: Optional[str] = None
    owner_name: Optional[str] = None
    match_score: float
