import sqlite3
from pathlib import Path

from email_sort.db import DB_PATH, add_column_if_missing, create_email_table


MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _ensure_base_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    create_email_table(cursor, "fastmail")
    create_email_table(cursor, "google_emails")
    conn.commit()


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
    for table_name in ("fastmail", "google_emails"):
        add_column_if_missing(cursor, table_name, "arc_auth_results", "TEXT")
        add_column_if_missing(cursor, table_name, "has_arc", "BOOLEAN DEFAULT 0")
        add_column_if_missing(cursor, table_name, "dkim_pass", "BOOLEAN DEFAULT 0")
        add_column_if_missing(cursor, table_name, "dmarc_arc_override", "BOOLEAN DEFAULT 0")


def _migration_5(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for table_name in ("fastmail", "google_emails"):
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
