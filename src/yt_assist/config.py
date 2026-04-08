"""Runtime configuration loading and validation."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

TOKEN_PLACEHOLDERS = {"", "PUT_YOUR_BOT_TOKEN_HERE", "YOUR_DISCORD_BOT_TOKEN"}


class DiscordConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str
    test_guild_id: int | None = None
    prefix: str
    admin_user_ids: list[int]
    admin_channel_ids: list[int] = Field(default_factory=list)
    allowed_channel_ids: list[int]
    receipt_log_channel_id: int | None = None
    status_text: str
    stats_delete_after_seconds: int
    pending_upload_timeout_seconds: int
    transient_message_timeout_seconds: int = 120
    receipt_idle_timeout_seconds: int = 300
    receipt_idle_warning_seconds: int = 60
    log_seen_messages: bool

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("discord.prefix cannot be blank")
        return stripped

    @field_validator("test_guild_id", mode="before")
    @classmethod
    def normalize_optional_guild_id(cls, value: object) -> object:
        if value in {None, "", 0, "0"}:
            return None
        return value


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database_path: Path
    catalog_path: Path
    templates_path: Path = Path("config") / "templates.json"
    contracts_path: Path = Path("config") / "contracts.json"
    export_dir: Path
    import_dir: Path = Path("import")
    attachment_dir: Path
    log_dir: Path


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: str


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discord: DiscordConfig
    storage: StorageConfig
    logging: LoggingConfig

    @classmethod
    def load_from(
        cls,
        path: Path,
        env: dict[str, str] | None = None,
        *,
        require_token: bool = True,
    ) -> Self:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        config = cls.model_validate(raw)
        config.resolve_token(env or os.environ, require_token=require_token)
        return config

    def resolve_token(self, env: dict[str, str], *, require_token: bool = True) -> None:
        env_token = env.get("YT_ASSIST_DISCORD_TOKEN", "").strip()
        file_token = self.discord.token.strip()
        resolved = env_token or file_token
        if require_token and resolved in TOKEN_PLACEHOLDERS:
            raise ValueError("discord.token cannot be blank or a placeholder")
        self.discord.token = resolved

    def ensure_directories(self) -> None:
        directories = {
            self.storage.database_path.parent,
            self.storage.export_dir,
            self.storage.import_dir,
            self.storage.attachment_dir,
            self.storage.log_dir,
            self.storage.catalog_path.parent,
            self.storage.templates_path.parent,
            self.storage.contracts_path.parent,
        }
        for directory in sorted(path for path in directories if path is not None):
            directory.mkdir(parents=True, exist_ok=True)


def load_runtime_config(path: Path | None = None, *, require_token: bool = True) -> AppConfig:
    config_path = path or Path("config") / "app.toml"
    return AppConfig.load_from(config_path, require_token=require_token)
