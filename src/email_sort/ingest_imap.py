import email
import imaplib
import sys
import time
from email import policy

from email_sort.config import get_section_setting
from email_sort.db import get_db, init_db
from email_sort.email_parse import message_record, upsert_email
from email_sort.progress import make_progress


def _connect():
    host = get_section_setting("imap", "host")
    port = int(get_section_setting("imap", "port", 993))
    username = get_section_setting("imap", "username")
    password = get_section_setting("imap", "password")
    use_ssl = get_section_setting("imap", "use_ssl", True)
    if not host or not username or not password:
        raise ValueError("Missing [imap] host, username, or password in config")
    client = imaplib.IMAP4_SSL(host, port) if use_ssl else imaplib.IMAP4(host, port)
    client.login(username, password)
    return client


def ingest_imap(watch: bool = False, source: str = "imap") -> None:
    init_db()
    folders = get_section_setting("imap", "folders", ["INBOX"])
    client = _connect()
    conn = get_db()
    cursor = conn.cursor()
    processed = 0
    try:
        while True:
            for folder in folders:
                status, _ = client.select(f'"{folder}"')
                if status != "OK":
                    print(f"Could not select IMAP folder {folder}")
                    continue
                status, data = client.search(None, "ALL")
                if status != "OK":
                    print(f"Could not search IMAP folder {folder}")
                    continue
                all_message_ids = data[0].split()
                cursor.execute(
                    "SELECT provider_id FROM emails WHERE source = ? AND provider_id LIKE ?",
                    (source, f"{folder}:%"),
                )
                seen = {str(row["provider_id"]).split(":", 1)[1] for row in cursor.fetchall()}
                message_ids = [
                    message_id
                    for message_id in all_message_ids
                    if message_id.decode(errors="replace") not in seen
                ]
                last_update = time.time()
                if sys.stdout.isatty():
                    progress = make_progress()
                    with progress:
                        task = progress.add_task(f"IMAP {folder}", total=len(message_ids))
                        for message_num in message_ids:
                            status, fetched = client.fetch(message_num, "(RFC822)")
                            if status == "OK" and fetched and isinstance(fetched[0], tuple):
                                message = email.message_from_bytes(
                                    fetched[0][1], policy=policy.default
                                )
                                provider_id = f"{folder}:{message_num.decode(errors='replace')}"
                                record = message_record(message, source, provider_id=provider_id)
                                upsert_email(cursor, record)
                                processed += 1
                            progress.advance(task)
                            if processed % 500 == 0:
                                conn.commit()
                else:
                    for message_num in message_ids:
                        status, fetched = client.fetch(message_num, "(RFC822)")
                        if status != "OK" or not fetched or not isinstance(fetched[0], tuple):
                            continue
                        message = email.message_from_bytes(fetched[0][1], policy=policy.default)
                        provider_id = f"{folder}:{message_num.decode(errors='replace')}"
                        record = message_record(message, source, provider_id=provider_id)
                        upsert_email(cursor, record)
                        processed += 1
                        if processed % 500 == 0:
                            conn.commit()
                        if time.time() - last_update >= 60:
                            print(
                                f"[{time.strftime('%H:%M:%S')}] IMAP ingested {processed} messages"
                            )
                            last_update = time.time()
                conn.commit()
            if not watch:
                break
            print("Watching IMAP folders; polling again in 60 seconds...")
            time.sleep(60)
    finally:
        conn.close()
        client.logout()
    print(f"IMAP ingestion complete. Processed {processed} messages.")


if __name__ == "__main__":
    ingest_imap("--watch" in sys.argv)
