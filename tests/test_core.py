import argparse
import email
from email import policy
from pathlib import Path

import pytest

from email_sort.classify import parse_classification
from email_sort.cli import build_parser
from email_sort.config import AppConfig, get_section_setting, get_setting, load_config
from email_sort.db import EMAIL_TABLE, create_email_table, get_db
from email_sort.email_parse import message_record, upsert_email
from email_sort.log import setup_logging
from email_sort.precheck import _writable_dir, run_precheck
from email_sort.sender_analysis import _addresses_contain_domain, _compute, _parse_date
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


def test_rule_fields_do_not_fill_llm_or_heuristic_fields(sqlite_conn):
    cursor = sqlite_conn.cursor()
    create_email_table(cursor)
    cursor.execute(
        f"""
        INSERT INTO {EMAIL_TABLE} (
            source, provider_id, message_id, sender, rule_category,
            rule_action, rule_confidence, rule_source, category, action,
            heuristic_category, heuristic_action
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "gmail",
            "1",
            "<1@example>",
            "friend@example.com",
            "Personal",
            "Mandatory",
            1.0,
            "user-reply",
            None,
            None,
            None,
            None,
        ),
    )
    sqlite_conn.commit()

    cursor.execute(
        f"SELECT rule_category, rule_action, category, action, heuristic_category FROM {EMAIL_TABLE}"
    )
    row = cursor.fetchone()
    assert row["rule_category"] == "Personal"
    assert row["rule_action"] == "Mandatory"
    assert row["category"] is None
    assert row["action"] is None
    assert row["heuristic_category"] is None


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


def test_classification_status_and_log_panels_are_separate():
    from email_sort import classify

    classify.active_tasks.clear()
    classify.finished_tasks.clear()

    classify.add_log("startup log")
    classify.update_status(1, "active worker")
    classify.update_status(1, "finished worker", is_finished=True)

    active_panel = classify.create_active_panel()
    log_panel = classify.create_log_panel()

    assert active_panel.title == "Active Workers"
    assert log_panel.title == "Recent Log"
    assert list(classify.active_tasks.values()) == []
    assert list(classify.finished_tasks) == ["finished worker", "startup log"]


def test_classify_limit_zero_does_not_process_rows(monkeypatch, capsys):
    from email_sort import classify

    conn = get_db()
    try:
        cursor = conn.cursor()
        create_email_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO {EMAIL_TABLE} (
                source, provider_id, sender, subject, snippet, language,
                is_not_for_me, dmarc_fail, dmarc_arc_override
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("test", "1", "sender@example.com", "Hello", "Body", "en", 0, 0, 0),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(classify, "apply_sender_prefilters", lambda source: 0)
    monkeypatch.setattr(classify, "apply_has_user_reply_prefilter", lambda source: 0)

    classify.classify_emails(limit=0)

    assert "No emails to classify." in capsys.readouterr().out


def test_classify_skips_rule_classified_rows(monkeypatch, capsys):
    from email_sort import classify

    conn = get_db()
    try:
        cursor = conn.cursor()
        create_email_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO {EMAIL_TABLE} (
                source, provider_id, sender, subject, snippet, language,
                is_not_for_me, dmarc_fail, dmarc_arc_override, rule_category,
                rule_action, rule_confidence, rule_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "test",
                "1",
                "sender@example.com",
                "Hello",
                "Body",
                "en",
                0,
                0,
                0,
                "Personal",
                "Mandatory",
                1.0,
                "user-reply",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(classify, "apply_sender_prefilters", lambda source: 0)
    monkeypatch.setattr(classify, "apply_has_user_reply_prefilter", lambda source: 0)

    classify.classify_emails()

    assert "No emails to classify." in capsys.readouterr().out


def test_classification_writer_advances_progress_per_result(monkeypatch):
    from email_sort.classify import classification_writer

    class FakeProgress:
        def __init__(self):
            self.advanced = 0

        def update(self, task, advance=0):
            self.advanced += advance

    result_queue = __import__("queue").Queue()
    result_queue.put(("Finance", 0.9, "Receipt", "Informational", "model", 1.0, 1))
    result_queue.put(("Security", 0.8, "Login", "Authentication", "model", 1.0, 2))
    result_queue.put(None)
    progress = FakeProgress()
    written_batches = []

    monkeypatch.setattr("email_sort.classify._write_batch", lambda cursor, batch: written_batches.append(list(batch)))

    classification_writer(result_queue, progress, "task", batch_size=100)

    assert progress.advanced == 2
    assert len(written_batches) == 1
    assert len(written_batches[0]) == 2


def test_domain_matching_uses_parsed_addresses():
    assert _addresses_contain_domain("User <user@mail.com>", "mail.com") is True
    assert _addresses_contain_domain("User <user@gmail.com>", "mail.com") is False


def test_sender_analysis_normalizes_mixed_timezone_dates():
    assert _parse_date("2026-01-01T12:00:00+00:00").tzinfo is None
    assert _parse_date("Thu, 01 Jan 2026 07:00:00 -0500").tzinfo is None

    stats = _compute(
        "sender",
        "news@example.com",
        [
            {
                "sender_domain": "example.com",
                "to_address": "me@example.net",
                "date": "2026-01-01T12:00:00+00:00",
                "effective_category": "Newsletter",
                "dmarc_fail": 0,
            },
            {
                "sender_domain": "example.com",
                "to_address": "me@example.net",
                "date": "Thu, 01 Jan 2026 07:30:00 -0500",
                "effective_category": "Newsletter",
                "dmarc_fail": 0,
            },
            {
                "sender_domain": "example.com",
                "to_address": "me@example.net",
                "date": "2026-01-01T13:00:00",
                "effective_category": "Newsletter",
                "dmarc_fail": 0,
            },
        ],
    )

    assert stats["first_seen"] == "2026-01-01T12:00:00"
    assert stats["last_seen"] == "2026-01-01T13:00:00"
    assert stats["burst_count"] == 1


def test_user_reply_prefilter_uses_rule_fields(sqlite_conn, monkeypatch):
    from email_sort import sender_analysis

    cursor = sqlite_conn.cursor()
    create_email_table(cursor)
    cursor.execute(
        """
        CREATE TABLE sender_stats (
            sender TEXT PRIMARY KEY,
            has_user_reply BOOLEAN NOT NULL DEFAULT 0
        )
        """
    )
    cursor.execute("INSERT INTO sender_stats (sender, has_user_reply) VALUES (?, ?)", ("@example.com", 1))
    cursor.execute(
        f"""
        INSERT INTO {EMAIL_TABLE} (
            source, provider_id, sender, sender_domain, category, action
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("test", "1", "friend@example.com", "example.com", None, None),
    )
    sqlite_conn.commit()

    class ConnWrapper:
        def __init__(self, conn):
            self.conn = conn

        def cursor(self):
            return self.conn.cursor()

        def commit(self):
            self.conn.commit()

        def close(self):
            pass

    monkeypatch.setattr(sender_analysis, "get_db", lambda: ConnWrapper(sqlite_conn))

    assert sender_analysis.apply_has_user_reply_prefilter() == 1
    cursor.execute(f"SELECT category, action, rule_category, rule_action, rule_source FROM {EMAIL_TABLE}")
    row = cursor.fetchone()
    assert row["category"] is None
    assert row["action"] is None
    assert row["rule_category"] == "Personal"
    assert row["rule_action"] == "Mandatory"
    assert row["rule_source"] == "user-reply"


def test_sender_analysis_uses_effective_separated_category():
    stats = _compute(
        "sender",
        "promo@example.com",
        [
            {
                "sender_domain": "example.com",
                "to_address": "me@example.net",
                "date": "2026-01-01T12:00:00Z",
                "effective_category": "Promotional",
                "dmarc_fail": 0,
            },
            {
                "sender_domain": "example.com",
                "to_address": "me@example.net",
                "date": "2026-01-02T12:00:00Z",
                "effective_category": "Spam",
                "dmarc_fail": 0,
            },
        ],
    )

    assert stats["promotional_ratio"] == 0.5
    assert stats["spam_ratio"] == 0.5


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


def test_cli_subcommands_have_visible_help():
    def check_parser(parser):
        for action in parser._actions:
            if not isinstance(action, argparse._SubParsersAction):
                continue
            for choice_action in action._choices_actions:
                assert choice_action.help not in (None, argparse.SUPPRESS, ""), choice_action.dest
            for subparser in action.choices.values():
                check_parser(subparser)

    check_parser(build_parser())


def test_heuristics_uses_progress_for_interactive_runs(monkeypatch):
    from email_sort import heuristics

    class FakeModel:
        def predict(self, text):
            return (["__label__en"], [0.99])

    class FakeProgress:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.tasks = []
            self.started = False
            self.stopped = False

        def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

        def add_task(self, description, total=None):
            self.tasks.append({"description": description, "total": total, "advanced": 0})
            return len(self.tasks) - 1

        def advance(self, task, advance=1):
            self.tasks[task]["advanced"] += advance

    created_progress = []

    def fake_make_progress(**kwargs):
        progress = FakeProgress(**kwargs)
        created_progress.append(progress)
        return progress

    conn = get_db()
    try:
        cursor = conn.cursor()
        create_email_table(cursor)
        cursor.executemany(
            f"""
            INSERT INTO {EMAIL_TABLE} (
                source, provider_id, subject, date, snippet, to_address, headers,
                body_html, dmarc_fail, has_arc, arc_auth_results, thread_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "test",
                    "1",
                    "Daily Digest",
                    "2026-01-01T10:00:00Z",
                    "hello",
                    "me@example.com",
                    '{"List-Id": "digest.example.com"}',
                    '<a href="https://example.com/unsubscribe">unsubscribe</a>',
                    0,
                    0,
                    "",
                    "thread-1",
                ),
                (
                    "test",
                    "2",
                    "Daily Digest",
                    "2026-01-01T11:00:00Z",
                    "hello again",
                    "me@example.com",
                    "{}",
                    "",
                    0,
                    0,
                    "",
                    "thread-1",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(heuristics, "download_model", lambda: None)
    monkeypatch.setattr(heuristics.fasttext, "load_model", lambda path: FakeModel())
    monkeypatch.setattr(heuristics.sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(heuristics, "make_progress", fake_make_progress)

    heuristics.run_heuristics()

    task_descriptions = [task["description"] for progress in created_progress for task in progress.tasks]
    assert "Running heuristics in emails" in task_descriptions
    assert "Preparing duplicate/digest rows" in task_descriptions
    assert "Detecting duplicate messages" in task_descriptions
    assert "Detecting digest senders" in task_descriptions
    assert "Propagating thread classifications" in task_descriptions
    assert all(progress.started and progress.stopped for progress in created_progress)

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT heuristic_category FROM {EMAIL_TABLE} WHERE provider_id = ?",
            ("2",),
        )
        assert cursor.fetchone()["heuristic_category"] == "Newsletter"
    finally:
        conn.close()


def test_heuristics_rolls_back_current_batch_on_interrupt(monkeypatch):
    from email_sort import heuristics

    class FakeModel:
        def predict(self, text):
            raise KeyboardInterrupt

    conn = get_db()
    try:
        cursor = conn.cursor()
        create_email_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO {EMAIL_TABLE} (
                source, provider_id, subject, snippet, to_address, headers,
                body_html, dmarc_fail, has_arc, arc_auth_results
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("test", "1", "Hello", "body", "me@example.com", "{}", "", 0, 0, ""),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(heuristics, "download_model", lambda: None)
    monkeypatch.setattr(heuristics.fasttext, "load_model", lambda path: FakeModel())
    monkeypatch.setattr(heuristics.sys.stdout, "isatty", lambda: False)

    with pytest.raises(SystemExit) as exc_info:
        heuristics.run_heuristics()

    assert exc_info.value.code == 130


def test_heuristics_incremental_and_recompute_modes(monkeypatch):
    from email_sort import heuristics

    class FakeModel:
        calls = 0

        def predict(self, text):
            self.calls += 1
            return (["__label__en"], [0.99])

    model = FakeModel()
    conn = get_db()
    try:
        cursor = conn.cursor()
        create_email_table(cursor)
        cursor.executemany(
            f"""
            INSERT INTO {EMAIL_TABLE} (
                source, provider_id, subject, snippet, to_address, headers,
                body_html, dmarc_fail, has_arc, arc_auth_results, heuristic_processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("test", "old", "Old", "already done", "me@example.com", "{}", "", 0, 0, "", "2026-01-01 00:00:00"),
                ("test", "new", "New", "needs work", "me@example.com", "{}", "", 0, 0, "", None),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(heuristics, "download_model", lambda: None)
    monkeypatch.setattr(heuristics.fasttext, "load_model", lambda path: model)
    monkeypatch.setattr(heuristics.sys.stdout, "isatty", lambda: False)

    heuristics.run_heuristics()
    assert model.calls == 1

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT provider_id, heuristic_processed_at FROM {EMAIL_TABLE} ORDER BY provider_id"
        )
        rows = {row["provider_id"]: row["heuristic_processed_at"] for row in cursor.fetchall()}
        assert rows["old"] == "2026-01-01 00:00:00"
        assert rows["new"] is not None
    finally:
        conn.close()

    heuristics.run_heuristics(recompute=True)
    assert model.calls == 3
