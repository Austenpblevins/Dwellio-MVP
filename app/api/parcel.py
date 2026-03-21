from __future__ import annotations

from app.models.parcel import ParcelSummaryResponse
from app.services.parcel_summary import ParcelSummaryService


def get_parcel_summary(county_id: str, tax_year: int, account_number: str) -> ParcelSummaryResponse:
    service = ParcelSummaryService()
    return service.get_summary(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
    )
