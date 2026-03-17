from __future__ import annotations
from datetime import date, datetime
from typing import Optional
from uuid import UUID
from app.models.common import DwellioBaseModel

class ProtestCaseCreate(DwellioBaseModel):
    client_id: UUID
    parcel_id: UUID
    tax_year: int
    valuation_run_id: Optional[UUID] = None
    case_status: str = 'created'
