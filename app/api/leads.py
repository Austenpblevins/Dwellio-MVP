from __future__ import annotations
from uuid import UUID
from app.models.quote import LeadCreateRequest, LeadCreateResponse

def create_lead(payload: LeadCreateRequest) -> LeadCreateResponse:
    return LeadCreateResponse(lead_id=UUID('00000000-0000-0000-0000-000000000001'))
