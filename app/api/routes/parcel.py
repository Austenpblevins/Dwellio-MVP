from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.api.parcel import get_parcel_summary
from app.models.parcel import ParcelSummaryResponse

router = APIRouter()


@router.get(
    "/parcel/{county_id}/{tax_year}/{account_number}",
    response_model=ParcelSummaryResponse,
)
def parcel_summary_endpoint(
    county_id: str,
    tax_year: int,
    account_number: str,
) -> ParcelSummaryResponse:
    try:
        return get_parcel_summary(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
