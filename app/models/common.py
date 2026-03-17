from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict

class DwellioBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True, extra='forbid')

JsonDict = Dict[str, Any]
JsonList = List[Any]

class ApiEnvelope(DwellioBaseModel):
    success: bool = True
    message: Optional[str] = None
