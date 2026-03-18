from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import EmailStr, Field

from app.api.leads import create_lead
from app.models.common import DwellioBaseModel

router = APIRouter()


class LeadCreateRequest(DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    owner_name: str | None = None
    email: EmailStr | None = None
    phone: str | None = None
    consent_to_contact: bool = Field(default=False)


@router.post("/lead")
def create_lead_endpoint(payload: LeadCreateRequest) -> dict[str, str]:
    try:
        create_lead(payload)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    return {"status": "accepted"}

