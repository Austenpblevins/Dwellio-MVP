from __future__ import annotations

from fastapi import APIRouter

from app.api.leads import create_lead
from app.models.lead import LeadCreateRequest, LeadCreateResponse

router = APIRouter()


@router.post("/lead", response_model=LeadCreateResponse)
def create_lead_endpoint(payload: LeadCreateRequest) -> LeadCreateResponse:
    return create_lead(payload)
