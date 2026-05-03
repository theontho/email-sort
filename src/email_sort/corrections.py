import json
from collections.abc import Iterable

from email_sort.db import EMAIL_TABLE, get_db


def _find_email(cursor, message_id: str):
    cursor.execute(
        f"""
        SELECT id, source, message_id, sender, sender_domain, category, action,
               rule_category, rule_action, heuristic_category, heuristic_action
        FROM {EMAIL_TABLE}
        WHERE message_id = ? OR provider_id = ?
        """,
        (message_id, message_id),
    )
    return cursor.fetchone()


def _override_keys(sender: str | None, sender_domain: str | None) -> Iterable[str]:
    if sender:
        yield sender.lower()
    if sender_domain:
        yield f"@{sender_domain.lower()}"


def _maybe_create_overrides(
    cursor, sender: str, sender_domain: str, category: str, action: str
) -> list[str]:
    created: list[str] = []
    for key in _override_keys(sender, sender_domain):
        if key.startswith("@"):
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM corrections c
                JOIN emails e ON e.message_id = c.message_id
                WHERE e.sender_domain = ?
                  AND c.corrected_category = ?
                  AND c.corrected_action = ?
                """,
                (key[1:], category, action),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM corrections c
                JOIN emails e ON e.message_id = c.message_id
                WHERE lower(e.sender) = ?
                  AND c.corrected_category = ?
                  AND c.corrected_action = ?
                """,
                (key, category, action),
            )
        if cursor.fetchone()[0] >= 2:
            cursor.execute(
                """
                INSERT INTO sender_overrides (sender, override_category, override_action)
                VALUES (?, ?, ?)
                ON CONFLICT(sender) DO UPDATE SET
                    override_category = excluded.override_category,
                    override_action = excluded.override_action
                """,
                (key, category, action),
            )
            created.append(key)
    return created


def create_correction(message_id: str, corrected_category: str, corrected_action: str) -> dict:
    conn = get_db()
    try:
        cursor = conn.cursor()
        row = _find_email(cursor, message_id)
        if not row:
            raise ValueError(f"Email with message_id={message_id} not found")

        cursor.execute(
            """
            INSERT INTO corrections (
                message_id, original_category, corrected_category, original_action, corrected_action
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                original_category = excluded.original_category,
                corrected_category = excluded.corrected_category,
                original_action = excluded.original_action,
                corrected_action = excluded.corrected_action,
                corrected_at = CURRENT_TIMESTAMP
            """,
            (
                message_id,
                row["category"] or row["rule_category"] or row["heuristic_category"],
                corrected_category,
                row["action"] or row["rule_action"] or row["heuristic_action"],
                corrected_action,
            ),
        )
        cursor.execute(
            f"""
            UPDATE {EMAIL_TABLE}
            SET rule_category = ?,
                rule_action = ?,
                rule_confidence = 1.0,
                rule_source = 'manual-correction'
            WHERE id = ?
            """,
            (corrected_category, corrected_action, row["id"]),
        )
        overrides = _maybe_create_overrides(
            cursor,
            row["sender"] or "",
            row["sender_domain"] or "",
            corrected_category,
            corrected_action,
        )
        conn.commit()
        return {
            "message_id": message_id,
            "source": row["source"],
            "original_category": row["category"]
            or row["rule_category"]
            or row["heuristic_category"],
            "corrected_category": corrected_category,
            "original_action": row["action"] or row["rule_action"] or row["heuristic_action"],
            "corrected_action": corrected_action,
            "overrides": overrides,
        }
    finally:
        conn.close()


def list_corrections() -> list[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM corrections ORDER BY corrected_at DESC")
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def export_corrections_jsonl() -> str:
    return "\n".join(json.dumps(row, sort_keys=True) for row in list_corrections())


def get_sender_override(sender: str | None, sender_domain: str | None = None) -> dict | None:
    conn = get_db()
    try:
        cursor = conn.cursor()
        for key in _override_keys(sender, sender_domain):
            cursor.execute("SELECT * FROM sender_overrides WHERE sender = ?", (key,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None
    finally:
        conn.close()


def apply_sender_prefilters(source: str | None = None) -> int:
    conn = get_db()
    try:
        cursor = conn.cursor()
        source_filter = "AND source = ?" if source else ""
        params = (source,) if source else ()
        cursor.execute("SELECT sender, override_category, override_action FROM sender_overrides")
        overrides = {row["sender"]: row for row in cursor.fetchall()}
        if not overrides:
            return 0
        cursor.execute(
            f"""
            SELECT id, sender, sender_domain
            FROM {EMAIL_TABLE}
            WHERE (rule_category IS NULL OR rule_category = '')
            {source_filter}
            """,
            params,
        )
        updates = []
        for row in cursor.fetchall():
            override = None
            for key in _override_keys(row["sender"], row["sender_domain"]):
                if key in overrides:
                    override = overrides[key]
                    break
            if override:
                updates.append(
                    (override["override_category"], override["override_action"], row["id"])
                )
        if updates:
            cursor.executemany(
                f"UPDATE {EMAIL_TABLE} SET rule_category = ?, rule_action = ?, rule_confidence = 1.0, rule_source = 'sender-override' WHERE id = ?",
                updates,
            )
            conn.commit()
        return len(updates)
    finally:
        conn.close()
