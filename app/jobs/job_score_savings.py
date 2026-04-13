from __future__ import annotations

from app.services.schema_readiness import assert_job_schema_ready
from app.services.quote_generation import QuoteGenerationService
from app.utils.logging import get_logger

logger = get_logger(__name__)

def run(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    account_numbers: tuple[str, ...] | None = None,
) -> None:
    logger.info('job_score_savings started', extra={'county_id': county_id, 'tax_year': tax_year})
    assert_job_schema_ready("job_score_savings", tax_year=tax_year)
    summary = QuoteGenerationService().score_savings(
        county_id=county_id,
        tax_year=tax_year,
        account_numbers=account_numbers,
    )
    logger.info(
        'job_score_savings finished',
        extra={'county_id': county_id, 'tax_year': tax_year, **summary.as_log_extra()},
    )
