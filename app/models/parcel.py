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
    situs_state: str | None = "TX"
    situs_zip: str | None = None
    owner_name: str | None = None
    property_type_code: str | None = None
    property_class_code: str | None = None
    neighborhood_code: str | None = None
    subdivision_name: str | None = None
    school_district_name: str | None = None


class ParcelSearchResult(DwellioBaseModel):
    county_id: str
    tax_year: int | None = None
    account_number: str
    parcel_id: UUID
    address: str
    situs_zip: str | None = None
    owner_name: str | None = None
    match_basis: str
    match_score: float
    confidence_label: str


class ParcelAutocompleteResponse(DwellioBaseModel):
    suggestions: list[ParcelSearchResult]


class ParcelSummaryResponse(DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    parcel_id: UUID
    address: str
    owner_name: str | None = None
    property_type_code: str | None = None
    property_class_code: str | None = None
    neighborhood_code: str | None = None
    subdivision_name: str | None = None
    school_district_name: str | None = None
    living_area_sf: float | None = None
    year_built: int | None = None
    effective_age: float | None = None
    bedrooms: int | None = None
    full_baths: float | None = None
    half_baths: float | None = None
    land_sf: float | None = None
    land_acres: float | None = None
    market_value: float | None = None
    assessed_value: float | None = None
    appraised_value: float | None = None
    certified_value: float | None = None
    notice_value: float | None = None
    exemption_value_total: float | None = None
    homestead_flag: bool | None = None
    over65_flag: bool | None = None
    disabled_flag: bool | None = None
    disabled_veteran_flag: bool | None = None
    freeze_flag: bool | None = None
    effective_tax_rate: float | None = None
    estimated_taxable_value: float | None = None
    estimated_annual_tax: float | None = None
    completeness_score: float
    warning_codes: list[str]
    public_summary_ready_flag: bool
    admin_review_required: bool | None = None
