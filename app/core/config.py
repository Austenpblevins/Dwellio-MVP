from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROTECTED_SHARED_BASELINE_DATABASE_URL = "postgresql://postgres:postgres@localhost:54322/postgres"
PROTECTED_SHARED_BASELINE_PORT = 54322


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    env: str = Field(default="dev", alias="DWELLIO_ENV")
    log_level: str = Field(default="INFO", alias="DWELLIO_LOG_LEVEL")
    api_host: str = Field(default="0.0.0.0", alias="DWELLIO_API_HOST")
    api_port: int = Field(default=8000, alias="DWELLIO_API_PORT")

    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:54322/postgres",
        alias="DWELLIO_DATABASE_URL",
    )
    db_connect_timeout_seconds: int = Field(default=5, alias="DWELLIO_DB_CONNECT_TIMEOUT_SECONDS")
    raw_archive_root: str = Field(default=".dwellio/raw", alias="DWELLIO_RAW_ARCHIVE_ROOT")

    default_tax_year: int = Field(default=2026, alias="DWELLIO_DEFAULT_TAX_YEAR")
    admin_api_token: str = Field(default="dev-admin-token", alias="DWELLIO_ADMIN_API_TOKEN")
    instant_quote_v5_enabled: bool = Field(
        default=False,
        alias="DWELLIO_INSTANT_QUOTE_V5_ENABLED",
    )
    instant_quote_v5_savings_translation_enabled: bool = Field(
        default=False,
        alias="DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ENABLED",
    )
    instant_quote_v5_savings_translation_county_ids: str = Field(
        default="fort_bend",
        alias="DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_COUNTY_IDS",
    )
    instant_quote_v5_savings_translation_rollout_states: str = Field(
        default="total_exemption_low_cash",
        alias="DWELLIO_INSTANT_QUOTE_V5_SAVINGS_TRANSLATION_ROLLOUT_STATES",
    )
    instant_quote_denominator_shift_alert_threshold: float = Field(
        default=0.05,
        alias="DWELLIO_INSTANT_QUOTE_DENOMINATOR_SHIFT_ALERT_THRESHOLD",
    )

    @model_validator(mode="after")
    def validate_stage21_database_isolation(self) -> Settings:
        parsed = urlparse(self.database_url)
        protected_host = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        protected_port = parsed.port == PROTECTED_SHARED_BASELINE_PORT
        protected_path = parsed.path.rstrip("/") == "/postgres"
        if protected_host and protected_port and protected_path:
            raise ValueError(
                "Stage 21 branch is baseline-protected and cannot run against "
                f"{PROTECTED_SHARED_BASELINE_DATABASE_URL}. "
                "Point DWELLIO_DATABASE_URL at an isolated Stage 21 database instead."
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
