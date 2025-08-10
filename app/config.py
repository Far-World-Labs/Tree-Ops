import os
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Application
    app_name: str = "Agentic Storage API"
    environment: str = os.getenv("ENVIRONMENT", "development")
    debug: bool = environment == "development"

    # Database
    database_url: str = os.environ["DATABASE_URL"]

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # CORS
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]

    # Redis (optional)
    redis_url: str | None = os.getenv("REDIS_URL", None)
    redis_key_prefix: str = os.getenv("REDIS_KEY_PREFIX", "tree_ops:")

    model_config = {"env_file": ".env"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
