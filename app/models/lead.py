from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import EmailStr, Field

from app.models.common import DwellioBaseModel

LeadContextStatus = Literal[
    "quote_ready",
    "missing_quote_ready_row",
    "unsupported_property_type",
    "unsupported_county",
]


class LeadCreateRequest(DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    owner_name: str | None = None
    email: EmailStr | None = None
    phone: str | None = None
    consent_to_contact: bool = Field(default=False)
    source_channel: str | None = None
    anonymous_session_id: str | None = None
    funnel_stage: str | None = None
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None
    utm_term: str | None = None
    utm_content: str | None = None


class LeadCreateResponse(DwellioBaseModel):
    status: Literal["accepted"] = "accepted"
    lead_id: UUID
    context_status: LeadContextStatus
    lead_capture_allowed: bool = True
    county_supported: bool
    property_supported: bool | None = None
    quote_ready: bool
    parcel_id: UUID | None = None
    requested_tax_year: int
    served_tax_year: int | None = None
    tax_year_fallback_applied: bool = False
    tax_year_fallback_reason: str | None = None

