import mailbox
import sys
import time

from email_sort.db import get_db, init_db
from email_sort.email_parse import message_record, upsert_email
from email_sort.progress import make_progress


def parse_mbox(mbox_path, table_name="google_emails"):
    init_db()
    print(f"Opening {mbox_path}...")
    print("Indexing mbox with Python mailbox module. This can take a while for 10GB+ files.")
    mbox = mailbox.mbox(mbox_path)
    try:
        total_messages = len(mbox)
        print(f"Found {total_messages} messages.")
    except Exception as exc:
        total_messages = None
        print(f"Could not pre-count messages: {exc}")

    conn = get_db()
    cursor = conn.cursor()
    processed = 0
    skipped = 0
    is_interactive = sys.stdout.isatty()
    start_time = time.time()
    last_update = start_time

    progress = make_progress()

    try:
        if is_interactive:
            with progress:
                task = progress.add_task("Ingesting mbox", total=total_messages)
                for message in mbox:
                    try:
                        record = message_record(message, "gmail")
                        upsert_email(cursor, table_name, record)
                    except Exception as exc:
                        skipped += 1
                        progress.console.print(
                            f"[yellow]Skipping malformed message:[/yellow] {exc}"
                        )
                    processed += 1
                    progress.advance(task)
                    if processed % 1000 == 0:
                        conn.commit()
        else:
            for message in mbox:
                try:
                    record = message_record(message, "gmail")
                    upsert_email(cursor, table_name, record)
                except Exception as exc:
                    skipped += 1
                    print(f"Skipping malformed message: {exc}")
                processed += 1
                now = time.time()
                if now - last_update >= 60:
                    elapsed = now - start_time
                    rate = processed / elapsed if elapsed else 0
                    total_part = f"/{total_messages}" if total_messages is not None else ""
                    print(
                        f"[{time.strftime('%H:%M:%S')}] Ingested {processed}{total_part} messages "
                        f"({rate:.2f}/s, skipped {skipped})"
                    )
                    last_update = now
                if processed % 1000 == 0:
                    conn.commit()
        conn.commit()
    finally:
        conn.close()

    print(
        f"Finished parsing {processed} messages from mbox into {table_name}. "
        f"Skipped {skipped} malformed messages."
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m email_sort.ingest_mbox path/to/takeout.mbox [table_name]")
        sys.exit(1)
    parse_mbox(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "google_emails")
