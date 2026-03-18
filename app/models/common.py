from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class DwellioBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True, extra='forbid')

JsonDict = dict[str, Any]
JsonList = list[Any]

class ApiEnvelope(DwellioBaseModel):
    success: bool = True
    message: str | None = None
