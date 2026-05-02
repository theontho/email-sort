import sqlite3
import os
from pathlib import Path
from email_sort.config import get_config_dir, get_setting


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
        ("thread_id", "TEXT"),
        ("delivered_to", "TEXT"),
    ]
    for col_name, col_type in columns:
        try:
            c.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            # Column already exists
            pass


def init_db():
    conn = get_db()
    c = conn.cursor()
    create_email_table(c, "emails")
    create_email_table(c, "google_emails")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
