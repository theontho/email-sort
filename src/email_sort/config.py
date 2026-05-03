import os
import platform
import tomllib
from pathlib import Path
from typing import Any, Dict, List


def get_default_config_dir() -> Path:
    """Returns the platform-specific default configuration directory."""
    if platform.system() == "Windows":
        # Windows: %APPDATA%/email-sort
        return Path(os.environ.get("APPDATA", "~")).expanduser() / "email-sort"
    else:
        # macOS/Linux: ~/.config/email-sort
        return Path("~/.config/email-sort").expanduser()


def _get_config_path() -> Path:
    """
    Determines the path to the configuration file.
    Order of precedence:
    1. EMAIL_SORT_CONFIG environment variable
    2. Local ./conf.toml
    3. Platform-specific default config directory
    """
    env_path = os.environ.get("EMAIL_SORT_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()

    local_path = Path("conf.toml")
    if local_path.exists():
        return local_path.resolve()

    return (get_default_config_dir() / "conf.toml").resolve()


CONF_PATH = _get_config_path()
CONFIG_DIR = CONF_PATH.parent

_config: Dict[str, Any] = {}


def load_config():
    global _config
    if CONF_PATH.exists():
        try:
            with open(CONF_PATH, "rb") as f:
                _config = tomllib.load(f)
        except Exception as e:
            print(f"Error loading config at {CONF_PATH}: {e}")
            _config = {}
    return _config


def get_setting(key: str, default: Any = None) -> Any:
    if not _config:
        load_config()

    # Check [general] section first
    general = _config.get("general", {})
    if key in general:
        return general[key]

    # Fallback to environment variables
    return os.environ.get(key.upper(), default)


def get_section(section: str) -> Dict[str, Any]:
    if not _config:
        load_config()
    value = _config.get(section, {})
    return value if isinstance(value, dict) else {}


def get_section_setting(section: str, key: str, default: Any = None) -> Any:
    section_data = get_section(section)
    if key in section_data:
        return section_data[key]

    env_key = f"{section}_{key}".upper()
    return os.environ.get(env_key, default)


def get_servers() -> List[Dict[str, Any]]:
    if not _config:
        load_config()
    return _config.get("servers", [])


def get_config_dir() -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def get_config_path() -> Path:
    return CONF_PATH
