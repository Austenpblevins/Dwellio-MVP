from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    default_tax_year: int = Field(default=2026, alias="DWELLIO_DEFAULT_TAX_YEAR")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

