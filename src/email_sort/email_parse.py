import email.utils
import json
import re
from email.header import decode_header, make_header
from email.message import Message


def _safe_header_value(value) -> str:
    try:
        return sqlite_safe_text(decode_str(value)).replace("\r", " ").replace("\n", " ")
    except Exception:
        return sqlite_safe_text(str(value)).replace("\r", " ").replace("\n", " ")


def sqlite_safe_text(value):
    if not isinstance(value, str):
        return value
    return value.encode("utf-8", errors="surrogatepass").decode("utf-8", errors="replace")


def decode_str(value) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(str(value))))
    except Exception:
        return str(value)


def headers_dict(message: Message) -> dict[str, list[str]]:
    headers: dict[str, list[str]] = {}
    raw_items = getattr(message, "raw_items", None)
    items = raw_items() if raw_items else message.items()
    for name, value in items:
        headers.setdefault(str(name).lower(), []).append(_safe_header_value(value))
    return headers


def get_header(headers: dict[str, list[str]], name: str) -> str:
    values = headers.get(name.lower(), [])
    return values[0] if values else ""


def auth_flag(value: str, name: str, result: str) -> bool:
    return bool(re.search(rf"\b{re.escape(name)}\s*=\s*{re.escape(result)}\b", value or "", re.I))


def sender_parts(raw_from: str) -> tuple[str, str]:
    address = email.utils.parseaddr(raw_from)[1] or raw_from
    address = address.strip().lower()
    domain = address.split("@", 1)[1] if "@" in address else ""
    return address, domain


def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        return email.utils.parsedate_to_datetime(value).isoformat()
    except Exception:
        return decode_str(value)


def extract_body(message: Message) -> tuple[str, str, int]:
    body_text = ""
    body_html = ""
    has_attachment = 0
    parts = message.walk() if message.is_multipart() else [message]
    for part in parts:
        content_disposition = str(part.get("Content-Disposition") or "").lower()
        if "attachment" in content_disposition:
            has_attachment = 1
            continue
        content_type = part.get_content_type()
        try:
            payload = part.get_payload(decode=True)
            if isinstance(payload, bytes):
                text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            else:
                text = str(part.get_payload() or "")
        except Exception:
            continue
        if content_type == "text/plain":
            body_text += text
        elif content_type == "text/html":
            body_html += text
    return body_text, body_html, has_attachment


def message_record(message: Message, source: str) -> dict:
    headers = headers_dict(message)
    sender, sender_domain = sender_parts(get_header(headers, "from"))
    auth_results = get_header(headers, "authentication-results")
    arc_auth_results = get_header(headers, "arc-authentication-results")
    has_arc = bool(get_header(headers, "arc-seal"))
    body_text, body_html, has_attachment = extract_body(message)
    message_id = get_header(headers, "message-id")
    if not message_id:
        message_id = (
            f"{source}:{get_header(headers, 'date')}:{sender}:{get_header(headers, 'subject')}"
        )
    return {
        "source": source,
        "message_id": sqlite_safe_text(message_id),
        "sender": sender,
        "sender_domain": sender_domain,
        "to_address": get_header(headers, "to"),
        "subject": decode_str(get_header(headers, "subject")),
        "date": parse_date(get_header(headers, "date")),
        "snippet": (body_text or body_html)[:2000],
        "body_text": sqlite_safe_text(body_text),
        "body_html": sqlite_safe_text(body_html),
        "cc": get_header(headers, "cc"),
        "bcc": get_header(headers, "bcc"),
        "reply_to": get_header(headers, "reply-to"),
        "has_attachment": has_attachment,
        "headers": sqlite_safe_text(json.dumps(headers)),
        "list_unsubscribe": get_header(headers, "list-unsubscribe"),
        "list_unsubscribe_post": get_header(headers, "list-unsubscribe-post"),
        "dmarc_fail": int(auth_flag(auth_results, "dmarc", "fail")),
        "spf_fail": int(
            auth_flag(auth_results, "spf", "fail")
            or "spf=fail" in get_header(headers, "received-spf").lower()
        ),
        "arc_auth_results": arc_auth_results,
        "has_arc": int(has_arc),
        "dkim_pass": int(auth_flag(auth_results, "dkim", "pass")),
        "dmarc_arc_override": int(has_arc and auth_flag(arc_auth_results, "dmarc", "pass")),
        "mailbox_ids": ",".join(headers.get("x-gmail-labels", [])),
        "thread_id": get_header(headers, "x-gm-thrid"),
        "delivered_to": get_header(headers, "delivered-to"),
    }


def upsert_email(cursor, table_name: str, record: dict) -> None:
    fields = [
        "source",
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
        "has_attachment",
        "headers",
        "list_unsubscribe",
        "list_unsubscribe_post",
        "dmarc_fail",
        "spf_fail",
        "arc_auth_results",
        "has_arc",
        "dkim_pass",
        "dmarc_arc_override",
        "mailbox_ids",
        "thread_id",
        "delivered_to",
    ]
    placeholders = ", ".join("?" for _ in fields)
    update_clause = ", ".join(
        f"{field}=excluded.{field}" for field in fields if field != "message_id"
    )
    cursor.execute(
        f"""
        INSERT INTO {table_name} ({", ".join(fields)})
        VALUES ({placeholders})
        ON CONFLICT(message_id) DO UPDATE SET {update_clause}
        """,
        [sqlite_safe_text(record.get(field)) for field in fields],
    )
