from __future__ import annotations

from uuid import UUID

from app.models.common import DwellioBaseModel


class ProtestCaseCreate(DwellioBaseModel):
    client_id: UUID
    parcel_id: UUID
    tax_year: int
    valuation_run_id: UUID | None = None
    case_status: str = 'created'
