from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

from infrastructure.jwt.config import JWTConfig
from infrastructure.postgres.config import PostgresConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
    )

    host: str = "0.0.0.0"  # nosec B104 — bind address configured via env
    port: int = 8000
    log_level: Literal["debug", "info", "warning", "error"] = "info"
    secure_cookies: bool = False

    postgres: PostgresConfig
    jwt: JWTConfig
