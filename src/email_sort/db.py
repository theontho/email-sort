import sqlite3
import os
from pathlib import Path
from email_sort.config import get_config_dir, get_setting


EMAIL_TABLES = ("fastmail", "google_emails")


def _get_db_path() -> Path:
    """
    Determines the path to the SQLite database.
    Order of precedence:
    1. EMAIL_SORT_DB environment variable
    2. data_dir setting in conf.toml
    3. Platform-specific default (CONFIG_DIR/data/emails.db)
    """
    env_path = os.environ.get("EMAIL_SORT_DB")
    if env_path:
        return Path(env_path)

    data_dir = get_setting("data_dir")
    if data_dir:
        return Path(data_dir).expanduser() / "emails.db"

    return get_config_dir() / "data" / "emails.db"


DB_PATH = _get_db_path()


def get_db():
    db_dir = DB_PATH.parent
    if not db_dir.exists():
        db_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
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


def create_email_table(c, table_name):
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            message_id TEXT UNIQUE,
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
            language TEXT,
            is_not_for_me BOOLEAN DEFAULT 0,
            category TEXT,
            confidence REAL,
            suggested_category TEXT,
            classify_model TEXT,
            classify_time REAL,
            action TEXT
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
        ("thread_id", "TEXT"),
        ("delivered_to", "TEXT"),
    ]
    for col_name, col_type in columns:
        add_column_if_missing(c, table_name, col_name, col_type)


def init_db():
    conn = get_db()
    c = conn.cursor()
    for table_name in EMAIL_TABLES:
        create_email_table(c, table_name)
    conn.commit()
    conn.close()
    from email_sort.migrate import migrate

    migrate(verbose=False)


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
