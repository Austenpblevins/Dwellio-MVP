from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class DwellioBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True, extra='forbid')

JsonDict = dict[str, Any]
JsonList = list[Any]

DataFreshnessLabel = Literal["current_year", "prior_year_fallback"]


class TaxYearFallbackMetadata(DwellioBaseModel):
    requested_tax_year: int
    served_tax_year: int
    tax_year_fallback_applied: bool
    tax_year_fallback_reason: str | None = None
    data_freshness_label: DataFreshnessLabel | None = None

class ApiEnvelope(DwellioBaseModel):
    success: bool = True
    message: str | None = None
