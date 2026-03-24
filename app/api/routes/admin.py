from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from app.api.admin import get_county_year_readiness
from app.models.admin import AdminCountyYearReadinessDashboard

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
