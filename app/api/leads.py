from __future__ import annotations

from app.models.lead import LeadCreateRequest, LeadCreateResponse
from app.services.lead_capture import LeadCaptureService


def create_lead(payload: LeadCreateRequest) -> LeadCreateResponse:
    service = LeadCaptureService()
    return service.create_lead(payload)
