from __future__ import annotations

from app.services.instant_quote_validation import InstantQuoteValidationService
from app.utils.logging import get_logger

logger = get_logger(__name__)


def run(*, county_id: str | None = None, tax_year: int | None = None) -> None:
    if county_id is None or tax_year is None:
        raise ValueError("job_validate_instant_quote requires county_id and tax_year.")

    report = InstantQuoteValidationService().build_report(
        county_id=county_id,
        tax_year=tax_year,
    )
    logger.info(
        "job_validate_instant_quote finished",
        extra={"county_id": county_id, "tax_year": tax_year, "validation_report": report.as_dict()},
    )
