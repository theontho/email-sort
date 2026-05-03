import json

from email_sort.db import EMAIL_TABLE, get_db
from email_sort.progress import make_progress


def migrate_labels(source=None):
    conn = get_db()
    c = conn.cursor()

    print(f"Fetching emails from {source or 'all sources'}...")
    source_filter = "AND source = ?" if source else ""
    params = (source,) if source else ()
    c.execute(
        f"""
        SELECT id, headers
        FROM {EMAIL_TABLE}
        WHERE (mailbox_ids IS NULL OR thread_id IS NULL OR delivered_to IS NULL)
        {source_filter}
        """,
        params,
    )
    rows = c.fetchall()

    print(f"Migrating labels for {len(rows)} emails...")

    progress = make_progress()
    with progress:
        task = progress.add_task("Migrating metadata", total=len(rows))
        for i, (email_id, headers_json) in enumerate(rows):
            if not headers_json:
                progress.advance(task)
                continue

            try:
                headers = json.loads(headers_json)

                # Gmail labels
                gmail_labels = headers.get("x-gmail-labels") or headers.get("X-Gmail-Labels", [])
                mailbox_ids = ",".join(gmail_labels) if gmail_labels else ""

                # Thread ID
                thread_ids = headers.get("x-gm-thrid") or headers.get("X-GM-THRID", [])
                thread_id = thread_ids[0] if thread_ids else ""

                # Delivered To
                delivered_tos = headers.get("delivered-to") or headers.get("Delivered-To", [])
                delivered_to = delivered_tos[0] if delivered_tos else ""

                c.execute(
                    f"UPDATE {EMAIL_TABLE} SET mailbox_ids = ?, thread_id = ?, delivered_to = ? WHERE id = ?",
                    (mailbox_ids, thread_id, delivered_to, email_id),
                )
            except Exception as e:
                print(f"Error migrating {email_id}: {e}")

            progress.advance(task)
            if i % 1000 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    import sys

    source = sys.argv[1] if len(sys.argv) > 1 else None
    migrate_labels(source)
