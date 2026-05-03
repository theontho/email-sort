import email
from email import policy
from pathlib import Path

import pytest

from email_sort.classify import parse_classification
from email_sort.config import AppConfig, get_section_setting, get_setting, load_config
from email_sort.db import EMAIL_TABLE, create_email_table
from email_sort.email_parse import message_record, upsert_email
from email_sort.log import setup_logging
from email_sort.precheck import _writable_dir, run_precheck
from email_sort.sender_analysis import _addresses_contain_domain
from email_sort.unsubscribe_agent import _is_safe_url


def test_upsert_uses_source_and_provider_id(sqlite_conn):
    cursor = sqlite_conn.cursor()
    create_email_table(cursor)

    message = email.message_from_string(
        "From: Example <a@example.com>\nTo: me@example.net\nSubject: Hello\n\nBody",
        policy=policy.default,
    )
    first = message_record(message, "gmail", provider_id="provider-1")
    second = message_record(message, "fastmail", provider_id="provider-1")

    upsert_email(cursor, first)
    upsert_email(cursor, second)
    sqlite_conn.commit()

    cursor.execute(f"SELECT source, provider_id FROM {EMAIL_TABLE} ORDER BY source")
    assert [tuple(row) for row in cursor.fetchall()] == [
        ("fastmail", "provider-1"),
        ("gmail", "provider-1"),
    ]


def test_missing_message_id_gets_stable_provider_id():
    message = email.message_from_string(
        "From: Example <a@example.com>\nSubject: Hello\n\nBody",
        policy=policy.default,
    )
    record = message_record(message, "custom-source")

    assert record["message_id"] == ""
    assert len(record["provider_id"]) == 64
    assert record["source"] == "custom-source"


def test_unsafe_unsubscribe_urls_are_blocked():
    assert _is_safe_url("http://example.com/unsubscribe")[0] is False
    assert _is_safe_url("https://127.0.0.1/unsubscribe")[0] is False
    assert _is_safe_url("https://localhost/unsubscribe")[0] is False


def test_heuristic_fields_do_not_fill_llm_fields(sqlite_conn):
    cursor = sqlite_conn.cursor()
    create_email_table(cursor)
    cursor.execute(
        f"""
        INSERT INTO {EMAIL_TABLE} (
            source, provider_id, message_id, sender, heuristic_category,
            heuristic_action, heuristic_confidence, category, action, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "gmail",
            "1",
            "<1@example>",
            "news@example.com",
            "Automated",
            "Archive",
            1.0,
            None,
            None,
            None,
        ),
    )
    sqlite_conn.commit()

    cursor.execute(
        f"SELECT heuristic_category, heuristic_action, category, action FROM {EMAIL_TABLE}"
    )
    row = cursor.fetchone()
    assert row["heuristic_category"] == "Automated"
    assert row["heuristic_action"] == "Archive"
    assert row["category"] is None
    assert row["action"] is None


def test_parse_classification_extracts_last_valid_csv_line():
    content = """
    I should classify this as a password email.
    Security, 0.97, Password Reset, Authentication
    """
    assert parse_classification(content) == (
        "Security",
        0.97,
        "Password Reset",
        "Authentication",
    )


def test_parse_classification_normalizes_case():
    assert parse_classification("spam, 0.95, phishing, promotional") == (
        "Spam",
        0.95,
        "phishing",
        "Promotional",
    )


def test_domain_matching_uses_parsed_addresses():
    assert _addresses_contain_domain("User <user@mail.com>", "mail.com") is True
    assert _addresses_contain_domain("User <user@gmail.com>", "mail.com") is False


def test_typed_config_rejects_unknown_keys():
    with pytest.raises(ValueError):
        AppConfig.model_validate({"general": {"unknown": True}})


def test_typed_config_requires_list_shapes():
    with pytest.raises(ValueError):
        AppConfig.model_validate({"imap": {"folders": "INBOX,Archive"}})


def test_env_fallback_for_unconfigured_secrets(monkeypatch):
    monkeypatch.setenv("FASTMAIL_TOKEN", "fastmail-secret")
    monkeypatch.setenv("SMTP_PASSWORD", "smtp-secret")
    load_config(reload=True)

    assert get_setting("fastmail_token") == "fastmail-secret"
    assert get_section_setting("smtp", "password") == "smtp-secret"


def test_writable_dir_handles_mkdir_errors(monkeypatch, tmp_path):
    def fail_mkdir(self, *args, **kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr(Path, "mkdir", fail_mkdir)

    assert _writable_dir(tmp_path / "blocked") is False


def test_precheck_fails_on_unhealthy_server(monkeypatch, tmp_path):
    config_path = tmp_path / "conf.toml"
    config_path.write_text('[[servers]]\nname = "bad"\nurl = "http://example.invalid/v1"\n')

    class Response:
        ok = False
        status_code = 500

    monkeypatch.setenv("EMAIL_SORT_CONFIG", str(config_path))
    monkeypatch.setattr("email_sort.precheck.socket.getaddrinfo", lambda *args: [])
    monkeypatch.setattr("email_sort.precheck.requests.get", lambda *args, **kwargs: Response())
    load_config(reload=True)

    ok, results = run_precheck(check_servers=True)

    assert ok is False
    assert "LLM server unhealthy: bad HTTP 500" in results


def test_setup_logging_survives_unwritable_file_handler(monkeypatch):
    def fail_file_handler(*args, **kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr("logging.FileHandler", fail_file_handler)

    setup_logging(log_file="/blocked/email-sort.log")
