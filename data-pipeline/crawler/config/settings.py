"""
config/settings.py — central configuration loaded from environment / defaults.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CRAWLER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------ paths
    project_root: Path = Field(default=Path(__file__).parent.parent)

    @property
    def seed_configs_dir(self) -> Path:
        return self.project_root / "config" / "seed_configs"

    @property
    def storage_dir(self) -> Path:
        return self.project_root / ".crawlee_storage"

    # --------------------------------------------------------------- crawlee
    max_concurrency: int = 5               # parallel requests
    request_timeout_secs: int = 30
    max_retries: int = 3
    retry_delay_secs: float = 2.0

    # ---------------------------------------------------------------- scoping
    global_max_depth: int = 4             # hard ceiling, overrides per-seed value
    max_requests_per_domain: int = 500    # per crawl run

    # --------------------------------------------------------------- headless
    use_playwright: bool = False          # False = httpx (faster); True = playwright
    headless: bool = True
    browser_type: str = "chromium"       # chromium | firefox | webkit

    # ---------------------------------------------------------------- logging
    log_level: str = "INFO"


# singleton — import this everywhere
settings = Settings()