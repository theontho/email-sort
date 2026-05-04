import os
import platform
import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

APP_NAME = "email-sort"
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class ConfigLoadError(RuntimeError):
    """Raised when configuration cannot be loaded."""


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str = "qwen/qwen3.6-35b-a3b"
    classification_body_chars: int = 4000
    lmstudio_key: str = "lm-studio"
    fastmail_token: str | None = None
    my_domains: list[str] = Field(default_factory=list)
    data_dir: Path | None = None
    log_level: LogLevel = "INFO"

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value


class ServerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    url: str
    workers: int = 1
    api_key: str | None = None
    model: str | None = None
    disabled: bool = False


class ImapConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str | None = None
    port: int = 993
    username: str | None = None
    password: str | None = None
    use_ssl: bool = True
    folders: list[str] = Field(default_factory=lambda: ["INBOX"])


class SieveConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str | None = None
    port: int = 4190
    username: str | None = None
    password: str | None = None


class UnsubscribeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_per_hour: int = 10
    max_per_day: int = 50
    safe_senders: list[str] = Field(default_factory=list)


class SmtpConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str | None = None
    port: int = 465
    username: str | None = None
    password: str | None = None


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    general: GeneralConfig = Field(default_factory=GeneralConfig)
    servers: list[ServerConfig] = Field(default_factory=list)
    imap: ImapConfig = Field(default_factory=ImapConfig)
    sieve: SieveConfig = Field(default_factory=SieveConfig)
    unsubscribe: UnsubscribeConfig = Field(default_factory=UnsubscribeConfig)
    smtp: SmtpConfig = Field(default_factory=SmtpConfig)

    @classmethod
    def load(cls) -> AppConfig:
        path = get_config_path()
        if not path.exists():
            return cls()
        try:
            with path.open("rb") as file:
                data = tomllib.load(file)
            return cls.model_validate(data)
        except (OSError, tomllib.TOMLDecodeError, ValidationError) as exc:
            raise ConfigLoadError(f"Could not load config at {path}: {exc}") from exc


def get_default_config_dir() -> Path:
    if platform.system() == "Windows":
        return Path(os.environ.get("APPDATA", "~")).expanduser() / APP_NAME
    return Path("~/.config").expanduser() / APP_NAME


def _get_config_path() -> Path:
    env_path = os.environ.get("EMAIL_SORT_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()
    local_path = Path("conf.toml")
    if local_path.exists():
        return local_path.resolve()
    return (get_default_config_dir() / "conf.toml").resolve()


_config: AppConfig | None = None
_config_path: Path | None = None


def load_config(reload: bool = False) -> AppConfig:
    global _config, _config_path
    path = get_config_path()
    if _config is None or reload or path != _config_path:
        _config = AppConfig.load()
        _config_path = path
    return _config


def get_config(reload: bool = False) -> AppConfig:
    return load_config(reload=reload)


def get_setting(key: str, default: Any = None) -> Any:
    config = get_config().general
    if key in config.model_fields_set:
        return getattr(config, key)
    env_value = os.environ.get(key.upper())
    if env_value is not None:
        return env_value
    return getattr(config, key, default)


def get_section(section: str) -> dict[str, Any]:
    value = getattr(get_config(), section)
    return value.model_dump()


def get_section_setting(section: str, key: str, default: Any = None) -> Any:
    value = getattr(get_config(), section)
    if key in value.model_fields_set:
        return getattr(value, key)
    env_value = os.environ.get(f"{section}_{key}".upper())
    if env_value is not None:
        return env_value
    return getattr(value, key, default)


def get_servers() -> list[dict[str, Any]]:
    return [server.model_dump(exclude_none=True) for server in get_config().servers]


def get_config_dir() -> Path:
    config_dir = get_config_path().parent
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_config_path() -> Path:
    return _get_config_path()
