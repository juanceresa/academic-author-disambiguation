"""Centralized configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from .env file or environment variables."""

    scopus_api_key: str = ""
    bigquery_project: str = "steadfast-task-437611-f3"
    crossref_email: str = ""

    model_config = {"env_file": ".env"}


settings = Settings()
