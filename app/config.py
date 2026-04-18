from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    db_path: Path = (
        Path(__file__).parent.parent / "data" / "warehouse" / "commercepulse.duckdb"
    )
    api_base_url: str = "http://localhost:8000"
    cache_ttl_seconds: int = 300  # 5 minutes

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CP_",
        case_sensitive=False,
    )


settings = AppSettings()
