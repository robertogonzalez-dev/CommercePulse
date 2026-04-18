from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_path: Path = Path(__file__).parent.parent / "data" / "warehouse" / "commercepulse.duckdb"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False
    cors_origins: list[str] = [
        "http://localhost:8501",
        "http://localhost:3000",
        "http://127.0.0.1:8501",
    ]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CP_",
        case_sensitive=False,
    )


settings = Settings()
