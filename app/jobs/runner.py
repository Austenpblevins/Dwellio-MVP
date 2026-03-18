from __future__ import annotations

from collections.abc import Callable
from typing import Any

from app.utils.logging import get_logger

logger = get_logger(__name__)

def execute_job(
    job_name: str,
    job_callable: Callable[..., Any],
    **job_kwargs: Any,
) -> Any:
    logger.info("job started", extra={"job_name": job_name, **job_kwargs})
    try:
        result = job_callable(**job_kwargs)
        logger.info("job succeeded", extra={"job_name": job_name, **job_kwargs})
        return result
    except Exception:
        logger.exception("job failed", extra={"job_name": job_name, **job_kwargs})
        raise
