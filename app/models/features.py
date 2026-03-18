from __future__ import annotations

from uuid import UUID

from app.models.common import DwellioBaseModel, JsonDict


class ParcelFeatures(DwellioBaseModel):
    parcel_id: UUID
    tax_year: int
    feature_json: JsonDict
