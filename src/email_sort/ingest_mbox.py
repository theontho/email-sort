import mailbox
import email.utils
from email.header import decode_header
import json
from tqdm import tqdm
from email_sort.db import get_db


def decode_str(s):
    if not s:
        return ""
    try:
        decoded_list = decode_header(s)
        parts = []
        for content, charset in decoded_list:
            if isinstance(content, bytes):
                try:
                    parts.append(content.decode(charset or "utf-8", errors="replace"))
                except LookupError:
                    parts.append(content.decode("utf-8", errors="replace"))
            else:
                parts.append(content)
        return "".join(parts)
    except Exception:
        return str(s)


def parse_mbox(mbox_path, table_name="fastmail"):
    print(f"Opening {mbox_path}...")
    print("Indexing MBOX (this may take several minutes for 10GB+ files)...")
    mbox = mailbox.mbox(mbox_path)

    # Using a simple loop to get count with a progress bar if we wanted,
    # but let's just use total=None if len() is too slow.
    # Actually len(mbox) is usually what people use.
    total_messages = None
    try:
        # This will trigger the indexing
        total_messages = len(mbox)
        print(f"Found {total_messages} messages.")
    except Exception as e:
        print(f"Could not pre-calculate count: {e}")

    conn = get_db()
    c = conn.cursor()

    processed_count = 0
    with tqdm(total=total_messages, desc="Ingesting emails", unit="msg") as pbar:
        for message in mbox:
            if processed_count % 1000 == 0 and processed_count > 0:
                conn.commit()

            pbar.update(1)

            source = "gmail"
            message_id = decode_str(message.get("Message-ID", ""))
            subject = decode_str(message.get("Subject", ""))
            sender = decode_str(message.get("From", ""))
            to_address = decode_str(message.get("To", ""))
            date = decode_str(message.get("Date", ""))
            list_unsubscribe = decode_str(message.get("List-Unsubscribe", ""))
            list_unsubscribe_post = decode_str(message.get("List-Unsubscribe-Post", ""))

            body_text = ""
            body_html = ""
            if message.is_multipart():
                for part in message.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/plain":
                        try:
                            payload = part.get_payload(decode=True)
                            if isinstance(payload, bytes):
                                text = payload.decode(
                                    part.get_content_charset() or "utf-8",
                                    errors="ignore",
                                )
                                body_text += text
                        except Exception:
                            pass
                    elif content_type == "text/html":
                        try:
                            payload = part.get_payload(decode=True)
                            if isinstance(payload, bytes):
                                html = payload.decode(
                                    part.get_content_charset() or "utf-8",
                                    errors="ignore",
                                )
                                body_html += html
                        except Exception:
                            pass
            else:
                try:
                    payload = message.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        text = payload.decode(
                            message.get_content_charset() or "utf-8", errors="ignore"
                        )
                        if message.get_content_type() == "text/html":
                            body_html = text
                        else:
                            body_text = text
                except Exception:
                    pass

            snippet = body_text[:2000] if body_text else ""
            if not snippet and body_html:
                snippet = "[HTML Content]"

            sender_domain = ""
            if sender:
                email_match = email.utils.parseaddr(sender)[1]
                if "@" in email_match:
                    sender_domain = email_match.split("@")[-1].lower()

            dmarc_fail = "dmarc=fail" in str(message.get("Authentication-Results", "")).lower()
            spf_fail = "spf=fail" in str(message.get("Received-SPF", "")).lower()

            cc = decode_str(message.get("Cc", ""))
            bcc = decode_str(message.get("Bcc", ""))
            reply_to = decode_str(message.get("Reply-To", ""))

            # Headers dict
            headers_dict: dict[str, list[str]] = {}
            for name, value in message.items():
                if name not in headers_dict:
                    headers_dict[name] = []
                headers_dict[name].append(decode_str(value))
            headers_json = json.dumps(headers_dict)

            # Extract Gmail labels and other metadata
            gmail_labels = headers_dict.get("X-Gmail-Labels", [])
            mailbox_ids = ",".join(gmail_labels) if gmail_labels else ""

            thread_ids = headers_dict.get("X-GM-THRID", [])
            thread_id = thread_ids[0] if thread_ids else ""

            delivered_tos = headers_dict.get("Delivered-To", [])
            delivered_to = delivered_tos[0] if delivered_tos else ""

            try:
                c.execute(
                    f"""
                    INSERT OR IGNORE INTO {table_name} 
                    (source, message_id, sender, sender_domain, to_address, subject, date, snippet, body_text, body_html, list_unsubscribe, list_unsubscribe_post, dmarc_fail, spf_fail, cc, bcc, reply_to, headers, mailbox_ids, thread_id, delivered_to)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        source,
                        message_id,
                        sender,
                        sender_domain,
                        to_address,
                        subject,
                        date,
                        snippet,
                        body_text,
                        body_html,
                        list_unsubscribe,
                        list_unsubscribe_post,
                        dmarc_fail,
                        spf_fail,
                        cc,
                        bcc,
                        reply_to,
                        headers_json,
                        mailbox_ids,
                        thread_id,
                        delivered_to,
                    ),
                )
            except Exception as e:
                print(f"Error inserting {message_id}: {e}")

            processed_count += 1

    conn.commit()
    conn.close()
    print(f"Finished parsing {processed_count} messages from mbox into {table_name}.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m email_sort.ingest_mbox path/to/takeout.mbox [table_name]")
        sys.exit(1)

    mbox_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else "fastmail"
    parse_mbox(mbox_path, table_name)
