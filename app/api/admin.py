from __future__ import annotations

from app.models.admin import (
    AdminCountyYearReadinessDashboard,
    AdminSearchInspectResponse,
)
from app.services.address_resolver import AddressResolverService
from app.services.admin_readiness import AdminReadinessService


def get_county_year_readiness(
    county_id: str,
    tax_years: list[int],
) -> AdminCountyYearReadinessDashboard:
    service = AdminReadinessService()
    return service.build_dashboard(county_id=county_id, tax_years=tax_years)


def get_search_inspection(
    query: str,
    *,
    limit: int = 10,
) -> AdminSearchInspectResponse:
    service = AddressResolverService()
    return service.inspect_search(query, limit=limit)
