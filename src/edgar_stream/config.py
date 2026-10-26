"""Settings read from the environment and an optional git-ignored .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PLACEHOLDER_AGENT = "Your Name your.email@example.com"


def load_env(path: str | Path = ".env") -> None:
    """Copy KEY=VALUE lines from a .env file into os.environ without overriding."""
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Settings:
    home: Path
    user_agent: str
    kafka_bootstrap: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str

    @property
    def warehouse_path(self) -> Path:
        return Path(os.environ.get("WAREHOUSE_PATH", self.home / "data" / "warehouse.duckdb"))

    @property
    def days_dir(self) -> Path:
        return Path(os.environ.get("DAYS_DIR", self.home / "data" / "days"))

    @property
    def objects_dir(self) -> Path:
        return Path(os.environ.get("OBJECTS_DIR", self.home / "data" / "objects"))

    @property
    def site_dir(self) -> Path:
        return Path(os.environ.get("SITE_DIR", self.home / "site"))


def get_settings() -> Settings:
    home = Path(os.environ.get("EDGAR_HOME", "."))
    load_env(home / ".env")
    return Settings(
        home=home,
        user_agent=os.environ.get("EDGAR_USER_AGENT", ""),
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        s3_endpoint=os.environ.get("S3_ENDPOINT", "http://localhost:9100"),
        s3_access_key=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        s3_bucket=os.environ.get("S3_BUCKET", "edgar-raw"),
    )
