import socket
from pathlib import Path
from urllib.parse import urlparse

import requests

from email_sort.config import get_config, get_config_path
from email_sort.db import _get_db_path
from email_sort.heuristics import MODEL_PATH


def _writable_dir(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    test_path = path / ".email-sort-write-test"
    try:
        test_path.write_text("ok")
        test_path.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def run_precheck(check_servers: bool = False) -> tuple[bool, list[str]]:
    config = get_config()
    results: list[str] = []
    ok = True

    config_path = get_config_path()
    results.append(f"config: {config_path}")
    if config_path.exists():
        results.append("config file: present")
    else:
        results.append("config file: missing; defaults will be used")

    db_path = _get_db_path()
    if _writable_dir(db_path.parent):
        results.append(f"database directory writable: {db_path.parent}")
    else:
        ok = False
        results.append(f"database directory not writable: {db_path.parent}")

    if MODEL_PATH.exists():
        results.append(f"language model: {MODEL_PATH}")
    else:
        results.append(f"language model missing: {MODEL_PATH} (will download on demand)")

    if not config.servers:
        results.append("LLM servers: none configured; default localhost fallback will be used")
    for server in config.servers:
        if server.disabled:
            results.append(f"LLM server disabled: {server.name}")
            continue
        parsed = urlparse(server.url)
        if not parsed.hostname:
            ok = False
            results.append(f"LLM server invalid URL: {server.name} {server.url}")
            continue
        try:
            socket.getaddrinfo(parsed.hostname, parsed.port or 80)
            results.append(f"LLM server DNS ok: {server.name}")
        except socket.gaierror as exc:
            ok = False
            results.append(f"LLM server DNS failed: {server.name}: {exc}")
            continue
        if check_servers:
            try:
                response = requests.get(f"{server.url.rstrip('/')}/models", timeout=10)
                results.append(f"LLM server reachable: {server.name} HTTP {response.status_code}")
            except requests.RequestException as exc:
                ok = False
                results.append(f"LLM server unreachable: {server.name}: {exc}")

    return ok, results
