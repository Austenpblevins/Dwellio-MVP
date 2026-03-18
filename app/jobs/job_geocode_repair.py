from __future__ import annotations

from app.utils.logging import get_logger

logger = get_logger(__name__)

def run(*, county_id: str | None = None, tax_year: int | None = None) -> None:
    logger.info('job_geocode_repair started', extra={'county_id': county_id, 'tax_year': tax_year})
    # TODO: implement job_geocode_repair
    logger.info('job_geocode_repair finished', extra={'county_id': county_id, 'tax_year': tax_year})
