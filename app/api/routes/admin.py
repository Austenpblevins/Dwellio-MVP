from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from app.api.admin import get_county_year_readiness, get_search_inspection
from app.models.admin import AdminCountyYearReadinessDashboard, AdminSearchInspectResponse

router = APIRouter()


@router.get(
    "/admin/readiness/{county_id}",
    response_model=AdminCountyYearReadinessDashboard,
)
def county_year_readiness_endpoint(
    county_id: str,
    tax_years: Annotated[list[int], Query(min_length=1)],
) -> AdminCountyYearReadinessDashboard:
    return get_county_year_readiness(county_id=county_id, tax_years=tax_years)


@router.get(
    "/admin/search/inspect",
    response_model=AdminSearchInspectResponse,
)
def search_inspection_endpoint(
    query: Annotated[str, Query(min_length=3)],
    limit: Annotated[int, Query(ge=1, le=25)] = 10,
) -> AdminSearchInspectResponse:
    return get_search_inspection(query, limit=limit)
