import os
import sqlite3
from pathlib import Path

from email_sort.config import get_config_dir, get_setting

EMAIL_TABLE = "emails"
EMAIL_TABLES = (EMAIL_TABLE,)


def _get_db_path() -> Path:
    """
    Determines the path to the SQLite database.
    Order of precedence:
    1. EMAIL_SORT_DB environment variable
    2. data_dir setting in conf.toml
    3. Platform-specific default (config dir/data/emails.db)
    """
    env_path = os.environ.get("EMAIL_SORT_DB")
    if env_path:
        return Path(env_path).expanduser()

    data_dir = get_setting("data_dir")
    if data_dir:
        return Path(data_dir).expanduser() / "emails.db"

    return get_config_dir() / "data" / "emails.db"


def get_db():
    db_path = _get_db_path()
    db_dir = db_path.parent
    if not db_dir.exists():
        db_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(c: sqlite3.Cursor, table_name: str) -> bool:
    c.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,))
    return c.fetchone() is not None


def column_exists(c: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    if not table_exists(c, table_name):
        return False
    c.execute(f"PRAGMA table_info({table_name})")
    return any(row[1] == column_name for row in c.fetchall())


def add_column_if_missing(
    c: sqlite3.Cursor, table_name: str, column_name: str, column_type: str
) -> None:
    if not column_exists(c, table_name, column_name):
        c.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def create_email_table(c, table_name: str = EMAIL_TABLE):
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT '',
            provider_id TEXT NOT NULL,
            message_id TEXT,
            sender TEXT,
            sender_domain TEXT,
            to_address TEXT,
            subject TEXT,
            date TEXT,
            snippet TEXT,
            body_text TEXT,
            body_html TEXT,
            cc TEXT,
            bcc TEXT,
            reply_to TEXT,
            keywords TEXT,
            mailbox_ids TEXT,
            has_attachment BOOLEAN,
            headers TEXT,
            list_unsubscribe TEXT,
            list_unsubscribe_post TEXT,
            body_unsubscribe_links TEXT,
            heuristic_matches TEXT,
            dmarc_fail BOOLEAN DEFAULT 0,
            spf_fail BOOLEAN DEFAULT 0,
            arc_auth_results TEXT,
            has_arc BOOLEAN DEFAULT 0,
            dkim_pass BOOLEAN DEFAULT 0,
            dmarc_arc_override BOOLEAN DEFAULT 0,
            language TEXT,
            is_not_for_me BOOLEAN DEFAULT 0,
            is_duplicate BOOLEAN DEFAULT 0,
            is_digest BOOLEAN DEFAULT 0,
            heuristic_category TEXT,
            heuristic_action TEXT,
            heuristic_confidence REAL,
            rule_category TEXT,
            rule_action TEXT,
            rule_confidence REAL,
            rule_source TEXT,
            heuristic_processed_at TEXT,
            category TEXT,
            confidence REAL,
            suggested_category TEXT,
            summary TEXT,
            classify_model TEXT,
            classify_time REAL,
            action TEXT,
            thread_id TEXT,
            delivered_to TEXT,
            UNIQUE(source, provider_id)
        )
    """)
    # Add columns if they don't exist (for existing databases)
    columns = [
        ("body_text", "TEXT"),
        ("body_html", "TEXT"),
        ("cc", "TEXT"),
        ("bcc", "TEXT"),
        ("reply_to", "TEXT"),
        ("keywords", "TEXT"),
        ("mailbox_ids", "TEXT"),
        ("has_attachment", "BOOLEAN"),
        ("headers", "TEXT"),
        ("list_unsubscribe_post", "TEXT"),
        ("body_unsubscribe_links", "TEXT"),
        ("heuristic_matches", "TEXT"),
        ("provider_id", "TEXT NOT NULL DEFAULT ''"),
        ("thread_id", "TEXT"),
        ("delivered_to", "TEXT"),
        ("arc_auth_results", "TEXT"),
        ("has_arc", "BOOLEAN DEFAULT 0"),
        ("dkim_pass", "BOOLEAN DEFAULT 0"),
        ("dmarc_arc_override", "BOOLEAN DEFAULT 0"),
        ("is_duplicate", "BOOLEAN DEFAULT 0"),
        ("is_digest", "BOOLEAN DEFAULT 0"),
        ("heuristic_category", "TEXT"),
        ("heuristic_action", "TEXT"),
        ("heuristic_confidence", "REAL"),
        ("rule_category", "TEXT"),
        ("rule_action", "TEXT"),
        ("rule_confidence", "REAL"),
        ("rule_source", "TEXT"),
        ("heuristic_processed_at", "TEXT"),
        ("summary", "TEXT"),
    ]
    for col_name, col_type in columns:
        add_column_if_missing(c, table_name, col_name, col_type)
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_source ON {table_name}(source)")
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_sender ON {table_name}(sender)")
    c.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_sender_domain ON {table_name}(sender_domain)"
    )
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_message_id ON {table_name}(message_id)")
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_category ON {table_name}(category)")
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_thread_id ON {table_name}(thread_id)")
    c.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_heuristic_processed_at ON {table_name}(heuristic_processed_at)"
    )
    c.execute(f"DROP INDEX IF EXISTS idx_{table_name}_classify_queue")
    c.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_classify_queue_v2
        ON {table_name}(id)
        WHERE COALESCE(category, '') = ''
          AND COALESCE(rule_category, '') = ''
          AND COALESCE(heuristic_category, '') = ''
          AND language = 'en'
          AND is_not_for_me = 0
          AND (dmarc_fail = 0 OR dmarc_arc_override = 1)
    """)


def init_db():
    conn = get_db()
    c = conn.cursor()
    create_email_table(c, EMAIL_TABLE)
    conn.commit()
    conn.close()
    from email_sort.migrate import migrate

    migrate(verbose=False)


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {_get_db_path()}")
