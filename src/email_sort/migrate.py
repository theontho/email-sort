import sqlite3
from pathlib import Path

from email_sort.db import (
    DB_PATH,
    EMAIL_TABLE,
    LEGACY_EMAIL_TABLES,
    add_column_if_missing,
    create_email_table,
    table_exists,
)


MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _ensure_base_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    create_email_table(cursor, EMAIL_TABLE)
    conn.commit()


def _copy_legacy_table(cursor: sqlite3.Cursor, table_name: str) -> None:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,))
    if cursor.fetchone() is None:
        return

    cursor.execute(f"PRAGMA table_info({table_name})")
    legacy_columns = {row[1] for row in cursor.fetchall()}
    copy_columns = [
        "message_id",
        "sender",
        "sender_domain",
        "to_address",
        "subject",
        "date",
        "snippet",
        "body_text",
        "body_html",
        "cc",
        "bcc",
        "reply_to",
        "keywords",
        "mailbox_ids",
        "has_attachment",
        "headers",
        "list_unsubscribe",
        "list_unsubscribe_post",
        "body_unsubscribe_links",
        "heuristic_matches",
        "dmarc_fail",
        "spf_fail",
        "arc_auth_results",
        "has_arc",
        "dkim_pass",
        "dmarc_arc_override",
        "language",
        "is_not_for_me",
        "is_duplicate",
        "is_digest",
        "category",
        "confidence",
        "suggested_category",
        "classify_model",
        "classify_time",
        "action",
        "thread_id",
        "delivered_to",
    ]
    select_values = [
        column if column in legacy_columns else f"NULL AS {column}" for column in copy_columns
    ]
    cursor.execute(
        f"""
        INSERT OR IGNORE INTO {EMAIL_TABLE} (
            source, provider_id, {", ".join(copy_columns)}
        )
        SELECT
            ?,
            COALESCE(NULLIF(message_id, ''), printf('%s:%s', ?, id)),
            {", ".join(select_values)}
        FROM {table_name}
        """,
        (table_name, table_name),
    )


def _migrate_legacy_email_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for table_name in LEGACY_EMAIL_TABLES:
        _copy_legacy_table(cursor, table_name)


def _get_current_version(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
    cursor.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
    return int(cursor.fetchone()[0])


def _available_migrations() -> list[tuple[int, Path]]:
    if not MIGRATIONS_DIR.exists():
        return []
    migrations: list[tuple[int, Path]] = []
    for path in MIGRATIONS_DIR.glob("*.sql"):
        try:
            migrations.append((int(path.name.split("_", 1)[0]), path))
        except ValueError:
            continue
    return sorted(migrations, key=lambda item: item[0])


def _apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    cursor = conn.cursor()
    sql = path.read_text()
    cursor.executescript(sql)


def _migration_3(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for table_name in (*LEGACY_EMAIL_TABLES, EMAIL_TABLE):
        if not table_exists(cursor, table_name):
            continue
        add_column_if_missing(cursor, table_name, "arc_auth_results", "TEXT")
        add_column_if_missing(cursor, table_name, "has_arc", "BOOLEAN DEFAULT 0")
        add_column_if_missing(cursor, table_name, "dkim_pass", "BOOLEAN DEFAULT 0")
        add_column_if_missing(cursor, table_name, "dmarc_arc_override", "BOOLEAN DEFAULT 0")


def _migration_5(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for table_name in (*LEGACY_EMAIL_TABLES, EMAIL_TABLE):
        if not table_exists(cursor, table_name):
            continue
        add_column_if_missing(cursor, table_name, "is_duplicate", "BOOLEAN DEFAULT 0")
        add_column_if_missing(cursor, table_name, "is_digest", "BOOLEAN DEFAULT 0")


PYTHON_MIGRATIONS = {
    3: _migration_3,
    5: _migration_5,
}


def migrate(verbose: bool = True) -> None:
    if verbose:
        print(f"Using database: {DB_PATH}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        _ensure_base_schema(conn)
        _migrate_legacy_email_tables(conn)
        current_version = _get_current_version(conn)
        pending = [
            (version, path)
            for version, path in _available_migrations()
            if version > current_version
        ]

        if not pending:
            if verbose:
                print("Database schema is up to date.")
            return

        cursor = conn.cursor()
        for version, path in pending:
            if verbose:
                print(f"Applying migration {version}: {path.name}")
            try:
                python_migration = PYTHON_MIGRATIONS.get(version)
                if python_migration:
                    python_migration(conn)
                else:
                    _apply_sql(conn, path)
                cursor.execute(
                    "INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (version,)
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        if verbose:
            print("All migrations completed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
