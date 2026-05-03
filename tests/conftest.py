import sqlite3

import pytest


@pytest.fixture(autouse=True)
def isolated_config(monkeypatch, tmp_path):
    monkeypatch.setenv("EMAIL_SORT_CONFIG", str(tmp_path / "conf.toml"))
    monkeypatch.setenv("EMAIL_SORT_DB", str(tmp_path / "emails.db"))
    from email_sort import config

    config.load_config(reload=True)
    yield
    config.load_config(reload=True)


@pytest.fixture
def sqlite_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
