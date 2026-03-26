from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class LiveAcquisitionConfig:
    access_method: str
    source_url: str | None = None
    source_file_path: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    retry_attempts: int = 1
    backoff_seconds: float = 0.0
    timeout_seconds: float = 30.0


def env_override_for_dataset(
    *,
    county_id: str,
    dataset_type: str,
    tax_year: int,
    default_source_url: str | None,
) -> LiveAcquisitionConfig | None:
    env_prefixes = _candidate_prefixes(
        county_id=county_id,
        dataset_type=dataset_type,
        tax_year=tax_year,
    )

    source_file_path = _resolve_env(env_prefixes, "SOURCE_FILE_PATH")
    source_url = _resolve_env(env_prefixes, "SOURCE_URL")
    if not source_file_path and not source_url:
        return None

    headers = _resolve_headers(env_prefixes)
    timeout_seconds = _resolve_float(env_prefixes, "TIMEOUT_SECONDS", default=30.0)
    retry_attempts = _resolve_int(env_prefixes, "RETRY_ATTEMPTS", default=3)
    backoff_seconds = _resolve_float(env_prefixes, "BACKOFF_SECONDS", default=1.0)

    return LiveAcquisitionConfig(
        access_method="live_file" if source_file_path else "live_http",
        source_url=source_url or default_source_url,
        source_file_path=source_file_path,
        headers=headers,
        retry_attempts=max(retry_attempts, 1),
        backoff_seconds=max(backoff_seconds, 0.0),
        timeout_seconds=max(timeout_seconds, 1.0),
    )


def load_live_content(config: LiveAcquisitionConfig) -> bytes:
    if config.access_method == "live_file":
        if not config.source_file_path:
            raise ValueError("Live file acquisition requires source_file_path.")
        return Path(config.source_file_path).expanduser().resolve().read_bytes()
    if config.access_method == "live_http":
        if not config.source_url:
            raise ValueError("Live HTTP acquisition requires source_url.")
        return _download_with_retry(config)
    raise ValueError(f"Unsupported live acquisition method: {config.access_method}.")


def infer_live_filename(
    *,
    county_id: str,
    dataset_type: str,
    tax_year: int,
    file_format: str,
    config: Any,
) -> str:
    if config.source_file_path:
        return Path(config.source_file_path).name

    if config.source_url:
        parsed = urlparse(config.source_url)
        candidate = Path(parsed.path).name
        if candidate:
            suffix = Path(candidate).suffix.lower()
            if suffix:
                return candidate

    extension = file_format.lower().strip(".")
    return f"{county_id}-{dataset_type}-{tax_year}.{extension}"


def _download_with_retry(config: LiveAcquisitionConfig) -> bytes:
    assert config.source_url is not None
    headers = getattr(config, "headers", getattr(config, "request_headers", {}))
    timeout_seconds = getattr(config, "timeout_seconds", 30.0)
    retry_attempts = getattr(config, "retry_attempts", 1)
    backoff_seconds = getattr(config, "backoff_seconds", 0.0)
    last_error: Exception | None = None
    for attempt in range(1, retry_attempts + 1):
        request = Request(config.source_url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
        except HTTPError as exc:
            last_error = exc
            if exc.code not in {408, 425, 429, 500, 502, 503, 504}:
                raise
        except (URLError, OSError) as exc:
            last_error = exc

        if attempt == retry_attempts:
            break
        sleep_seconds = backoff_seconds * attempt
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    assert last_error is not None
    raise last_error


def _candidate_prefixes(*, county_id: str, dataset_type: str, tax_year: int) -> list[str]:
    normalized_county = county_id.upper().replace("-", "_")
    normalized_dataset = dataset_type.upper().replace("-", "_")
    return [
        f"DWELLIO_{normalized_county}_{normalized_dataset}_{tax_year}",
        f"DWELLIO_{normalized_county}_{normalized_dataset}",
    ]


def _resolve_env(prefixes: list[str], suffix: str) -> str | None:
    for prefix in prefixes:
        value = os.getenv(f"{prefix}_{suffix}")
        if value:
            return value
    return None


def _resolve_headers(prefixes: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}

    headers_json = _resolve_env(prefixes, "HEADERS_JSON")
    if headers_json:
        loaded = json.loads(headers_json)
        if not isinstance(loaded, dict):
            raise ValueError("HEADERS_JSON must decode to a JSON object.")
        headers.update({str(key): str(value) for key, value in loaded.items()})

    bearer_token = _resolve_env(prefixes, "BEARER_TOKEN")
    if bearer_token:
        headers.setdefault("Authorization", f"Bearer {bearer_token}")

    cookie = _resolve_env(prefixes, "COOKIE")
    if cookie:
        headers.setdefault("Cookie", cookie)

    user_agent = _resolve_env(prefixes, "USER_AGENT")
    if user_agent:
        headers.setdefault("User-Agent", user_agent)

    auth_header_name = _resolve_env(prefixes, "AUTH_HEADER_NAME")
    auth_header_value = _resolve_env(prefixes, "AUTH_HEADER_VALUE")
    if auth_header_name and auth_header_value:
        headers.setdefault(auth_header_name, auth_header_value)

    return headers


def _resolve_int(prefixes: list[str], suffix: str, *, default: int) -> int:
    value = _resolve_env(prefixes, suffix)
    if value is None:
        return default
    return int(value)


def _resolve_float(prefixes: list[str], suffix: str, *, default: float) -> float:
    value = _resolve_env(prefixes, suffix)
    if value is None:
        return default
    return float(value)
