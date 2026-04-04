from __future__ import annotations

from app.services.instant_quote_tax_rate_adoption_status import (
    InstantQuoteTaxRateAdoptionStatusService,
)
from app.services.schema_readiness import assert_job_schema_ready
from app.utils.logging import get_logger

logger = get_logger(__name__)


def run(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    tax_rate_basis_status: str | None = None,
    tax_rate_basis_status_reason: str | None = None,
    tax_rate_basis_status_note: str | None = None,
    tax_rate_basis_status_source: str | None = None,
) -> None:
    if county_id is None or tax_year is None:
        raise ValueError("job_set_tax_rate_adoption_status requires county_id and tax_year.")
    if tax_rate_basis_status is None:
        raise ValueError(
            "job_set_tax_rate_adoption_status requires tax_rate_basis_status."
        )

    assert_job_schema_ready("job_set_tax_rate_adoption_status", tax_year=tax_year)
    record = InstantQuoteTaxRateAdoptionStatusService().upsert_status(
        county_id=county_id,
        tax_year=tax_year,
        adoption_status=tax_rate_basis_status,
        adoption_status_reason=tax_rate_basis_status_reason,
        status_source=tax_rate_basis_status_source,
        source_note=tax_rate_basis_status_note,
    )
    logger.info(
        "job_set_tax_rate_adoption_status finished",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "tax_rate_basis_status": record.adoption_status,
            "tax_rate_basis_status_reason": record.adoption_status_reason,
            "tax_rate_basis_status_source": record.status_source,
            "tax_rate_basis_status_note": record.source_note,
            "next_step": (
                "Rerun job_refresh_instant_quote and job_validate_instant_quote for this county-year."
            ),
        },
    )
