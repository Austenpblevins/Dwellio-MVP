from __future__ import annotations

from app.services.instant_quote import InstantQuoteRefreshService
from app.services.schema_readiness import assert_job_schema_ready
from app.utils.logging import get_logger

logger = get_logger(__name__)


def run(*, county_id: str | None = None, tax_year: int | None = None) -> None:
    logger.info(
        "job_refresh_instant_quote started",
        extra={"county_id": county_id, "tax_year": tax_year},
    )
    assert_job_schema_ready("job_refresh_instant_quote", tax_year=tax_year)
    summary = InstantQuoteRefreshService().refresh(county_id=county_id, tax_year=tax_year)
    logger.info(
        "job_refresh_instant_quote finished",
        extra={"county_id": county_id, "tax_year": tax_year, **summary.as_log_extra()},
    )
