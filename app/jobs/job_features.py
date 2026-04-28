from __future__ import annotations

from app.services.fort_bend_bathroom_features import FortBendBathroomFeatureService
from app.services.schema_readiness import assert_job_schema_ready
from app.utils.logging import get_logger

logger = get_logger(__name__)

def run(*, county_id: str | None = None, tax_year: int | None = None) -> None:
    if county_id is None or tax_year is None:
        raise ValueError("job_features requires county_id and tax_year.")
    logger.info('job_features started', extra={'county_id': county_id, 'tax_year': tax_year})
    assert_job_schema_ready("job_features", tax_year=tax_year)
    if county_id != "fort_bend":
        logger.info(
            "job_features has no county-specific feature materialization for this county_id; skipping.",
            extra={'county_id': county_id, 'tax_year': tax_year},
        )
        return
    bathroom_summary = FortBendBathroomFeatureService().materialize_features(
        county_id=county_id,
        tax_year=tax_year,
    )
    logger.info('job_features finished', extra={'county_id': county_id, 'tax_year': tax_year})
    logger.info(
        "job_features Fort Bend bathroom layer materialized",
        extra={'county_id': county_id, 'tax_year': tax_year, 'summary': bathroom_summary},
    )
