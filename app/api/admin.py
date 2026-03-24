from __future__ import annotations

from app.models.admin import AdminCountyYearReadinessDashboard
from app.services.admin_readiness import AdminReadinessService


def get_county_year_readiness(
    county_id: str,
    tax_years: list[int],
) -> AdminCountyYearReadinessDashboard:
    service = AdminReadinessService()
    return service.build_dashboard(county_id=county_id, tax_years=tax_years)
