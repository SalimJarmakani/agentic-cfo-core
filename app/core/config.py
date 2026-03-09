from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Agent CFO Core API"
    app_version: str = "0.1.0"

    pghost: str = "localhost"
    pgport: int = 5432
    pgdatabase: str = "caixabank"
    pguser: str = "postgres"
    pgpassword: str = ""

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j12345"
    neo4j_database: str = "caixabank"

    default_recent_tx_limit: int = 25
    default_risky_merchants_limit: int = 20

    llm_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    llm_api_key: str = ""
    llm_model: str = "gemini-3-flash-preview"
    llm_timeout_seconds: int = 30

    # Comma-separated origins allowed for CORS, e.g. "http://localhost:5173,https://app.example.com"
    cors_origins: str = "http://localhost:5173"

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
