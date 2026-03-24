from __future__ import annotations

from app.models.common import DwellioBaseModel


class AdminCountyYearDatasetReadiness(DwellioBaseModel):
    dataset_type: str
    source_system_code: str
    access_method: str
    availability_status: str
    raw_file_count: int
    latest_import_status: str | None = None
    latest_publish_state: str | None = None
    stage_status: str
    blockers: list[str]


class AdminCountyYearDerivedReadiness(DwellioBaseModel):
    parcel_summary_ready: bool
    search_support_ready: bool
    feature_ready: bool
    comp_ready: bool
    quote_ready: bool
    parcel_summary_row_count: int
    search_document_row_count: int
    parcel_feature_row_count: int
    comp_pool_row_count: int
    quote_row_count: int


class AdminCountyYearReadiness(DwellioBaseModel):
    county_id: str
    tax_year: int
    overall_status: str
    readiness_score: int
    trend_label: str
    trend_delta: int | None = None
    tax_year_known: bool
    blockers: list[str]
    datasets: list[AdminCountyYearDatasetReadiness]
    derived: AdminCountyYearDerivedReadiness


class AdminCountyYearReadinessDashboard(DwellioBaseModel):
    access_scope: str = "internal"
    county_id: str
    tax_years: list[int]
    readiness_rows: list[AdminCountyYearReadiness]


class AdminSearchScoreComponents(DwellioBaseModel):
    basis_rank: int
    address_similarity: float
    search_text_similarity: float
    owner_similarity: float


class AdminSearchInspectCandidate(DwellioBaseModel):
    county_id: str
    tax_year: int | None = None
    account_number: str
    parcel_id: str
    address: str
    situs_zip: str | None = None
    owner_name: str | None = None
    match_basis: str
    match_score: float
    confidence_label: str
    confidence_reasons: list[str]
    matched_fields: list[str]
    score_components: AdminSearchScoreComponents


class AdminSearchInspectResponse(DwellioBaseModel):
    access_scope: str = "internal"
    query: str
    normalized_address_query: str
    normalized_owner_query: str | None = None
    candidates: list[AdminSearchInspectCandidate]
