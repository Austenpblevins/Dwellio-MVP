from __future__ import annotations

from collections.abc import Callable

from app.utils.logging import get_logger

logger = get_logger(__name__)

def execute_job(job_name: str, job_callable: Callable[..., None], *, county_id: str | None = None, tax_year: int | None = None) -> None:
    logger.info('job started', extra={'job_name': job_name, 'county_id': county_id, 'tax_year': tax_year})
    try:
        job_callable(county_id=county_id, tax_year=tax_year)
        logger.info('job succeeded', extra={'job_name': job_name, 'county_id': county_id, 'tax_year': tax_year})
    except Exception:
        logger.exception('job failed', extra={'job_name': job_name, 'county_id': county_id, 'tax_year': tax_year})
        raise
