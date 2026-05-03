from datetime import datetime, timedelta

from email_sort.db import EMAIL_TABLES, get_db


def check_failed_unsubscribes() -> list[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=14)).isoformat()
        cursor.execute(
            """
            SELECT sender, MIN(attempted_at) AS attempted_at
            FROM unsubscribe_log
            WHERE status = 'success' AND attempted_at <= ?
            GROUP BY sender
            """,
            (cutoff,),
        )
        failed = []
        cursor.execute("DELETE FROM failed_unsubscribes")
        for unsub in cursor.fetchall():
            count = 0
            last_received = None
            for table_name in EMAIL_TABLES:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) AS count, MAX(date) AS last_received
                    FROM {table_name}
                    WHERE sender = ? AND date > ?
                    """,
                    (unsub["sender"], unsub["attempted_at"]),
                )
                row = cursor.fetchone()
                count += row["count"] or 0
                if row["last_received"] and (not last_received or row["last_received"] > last_received):
                    last_received = row["last_received"]
            if count:
                item = {
                    "sender": unsub["sender"],
                    "unsubscribed_at": unsub["attempted_at"],
                    "email_count": count,
                    "last_received": last_received,
                }
                failed.append(item)
                cursor.execute(
                    """
                    INSERT INTO failed_unsubscribes (
                        sender, unsubscribed_at, emails_after_unsubscribe, last_received
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(sender) DO UPDATE SET
                        unsubscribed_at = excluded.unsubscribed_at,
                        emails_after_unsubscribe = excluded.emails_after_unsubscribe,
                        last_received = excluded.last_received,
                        checked_at = CURRENT_TIMESTAMP
                    """,
                    (item["sender"], item["unsubscribed_at"], item["email_count"], item["last_received"]),
                )
        conn.commit()
        return failed
    finally:
        conn.close()
