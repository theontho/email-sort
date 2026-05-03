import json
import re
import sys
import time

import requests

from email_sort.config import get_setting
from email_sort.db import get_db, init_db
from email_sort.email_parse import sqlite_safe_text
from email_sort.progress import make_progress


def _first_header(headers_list, name):
    lower_name = name.lower()
    return next(
        (h.get("value", "") for h in headers_list if h.get("name", "").lower() == lower_name), ""
    )


def _auth_flag(auth_results: str, name: str, result: str) -> bool:
    return bool(re.search(rf"\b{re.escape(name)}\s*=\s*{re.escape(result)}\b", auth_results, re.I))


def _fetch_all_email_ids(api_url: str, headers: dict, account_id: str) -> list[str]:
    email_ids: list[str] = []
    position = 0
    total = None
    while True:
        query_req = {
            "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
            "methodCalls": [
                [
                    "Email/query",
                    {"accountId": account_id, "position": position, "limit": 5000},
                    "0",
                ]
            ],
        }
        res = requests.post(api_url, headers=headers, json=query_req, timeout=60)
        res.raise_for_status()
        query_res = res.json()["methodResponses"][0][1]
        batch = query_res.get("ids", [])
        if total is None:
            total = query_res.get("total")
        if not batch:
            break
        email_ids.extend(batch)
        position += len(batch)
        if total is not None and position >= total:
            break
    if total is not None and len(email_ids) != total:
        print(f"Warning: Fastmail reported {total} emails but returned {len(email_ids)} IDs")
    return email_ids


def ingest_fastmail(source: str = "fastmail"):
    init_db()
    token = get_setting("fastmail_token")
    if not token:
        print("Please set FASTMAIL_TOKEN environment variable.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    res = requests.get("https://api.fastmail.com/jmap/session", headers=headers, timeout=60)
    res.raise_for_status()
    session = res.json()

    api_url = session["apiUrl"]
    account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]

    print("Querying Fastmail for message IDs...")
    email_ids = _fetch_all_email_ids(api_url, headers, account_id)
    print(f"Found {len(email_ids)} emails. Fetching details...")

    conn = get_db()
    c = conn.cursor()

    is_interactive = sys.stdout.isatty()
    if is_interactive:
        progress = make_progress()
        progress.start()
        progress_task = progress.add_task("Ingesting Fastmail", total=len(email_ids))
    else:
        start_time = time.time()
        last_update = start_time
        processed = 0

    batch_size = 100
    for i in range(0, len(email_ids), batch_size):
        batch_ids = email_ids[i : i + batch_size]

        if not is_interactive:
            now = time.time()
            if now - last_update >= 60:
                rate = processed / (now - start_time) if now > start_time else 0
                print(
                    f"[{time.strftime('%H:%M:%S')}] Processed {processed}/{len(email_ids)} emails. Rate: {rate:.2f} emails/s"
                )
                last_update = now

        fetch_req = {
            "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
            "methodCalls": [
                [
                    "Email/get",
                    {
                        "accountId": account_id,
                        "ids": batch_ids,
                        "properties": [
                            "id",
                            "subject",
                            "from",
                            "to",
                            "cc",
                            "bcc",
                            "replyTo",
                            "receivedAt",
                            "preview",
                            "headers",
                            "textBody",
                            "htmlBody",
                            "bodyValues",
                            "keywords",
                            "mailboxIds",
                            "hasAttachment",
                        ],
                        "bodyProperties": ["partId", "value", "isTruncated"],
                        "fetchTextBodyValues": True,
                        "fetchHTMLBodyValues": True,
                    },
                    "0",
                ]
            ],
        }

        res = requests.post(api_url, headers=headers, json=fetch_req, timeout=60)
        res.raise_for_status()
        fetch_res = res.json()

        method_response = fetch_res["methodResponses"][0]
        if method_response[0] == "error":
            print(f"JMAP Error: {method_response[1]}")
            break

        emails_data = method_response[1].get("list", [])
        if not emails_data:
            print(f"No emails returned in batch {i}. Response: {fetch_res}")
            break

        for email_data in emails_data:
            provider_id = email_data["id"]
            subject = email_data.get("subject", "")
            date = email_data.get("receivedAt", "")

            # Try to get the actual body content
            body_text = ""
            text_body_parts = email_data.get("textBody", [])
            body_values = email_data.get("bodyValues", {})

            if text_body_parts:
                for part in text_body_parts:
                    part_id = part.get("partId")
                    if part_id in body_values:
                        body_text += body_values[part_id].get("value", "")

            body_html = ""
            html_body_parts = email_data.get("htmlBody", [])
            if html_body_parts:
                for part in html_body_parts:
                    part_id = part.get("partId")
                    if part_id in body_values:
                        body_html += body_values[part_id].get("value", "")

            snippet = email_data.get("preview", "")
            if not snippet and body_text:
                snippet = body_text[:2000]

            from_arr = email_data.get("from", [])
            sender = from_arr[0]["email"] if from_arr else ""
            sender_domain = sender.split("@")[-1].lower() if "@" in sender else ""

            to_arr = email_data.get("to", [])
            to_address = ", ".join([t["email"] for t in to_arr]) if to_arr else ""

            # Extract headers from the list
            headers_list = email_data.get("headers", [])
            list_unsubscribe = _first_header(headers_list, "list-unsubscribe")
            list_unsubscribe_post = _first_header(headers_list, "list-unsubscribe-post")
            message_id = _first_header(headers_list, "message-id")
            auth_results = _first_header(headers_list, "authentication-results")
            arc_auth_results = _first_header(headers_list, "arc-authentication-results")
            arc_seal = _first_header(headers_list, "arc-seal")
            dmarc_fail = _auth_flag(auth_results, "dmarc", "fail")
            spf_fail = _auth_flag(auth_results, "spf", "fail")
            dkim_pass = _auth_flag(auth_results, "dkim", "pass")
            has_arc = bool(arc_seal)
            dmarc_arc_override = has_arc and _auth_flag(arc_auth_results, "dmarc", "pass")

            cc = json.dumps(email_data.get("cc")) if email_data.get("cc") else None
            bcc = json.dumps(email_data.get("bcc")) if email_data.get("bcc") else None
            reply_to = json.dumps(email_data.get("replyTo")) if email_data.get("replyTo") else None
            keywords = (
                json.dumps(email_data.get("keywords")) if email_data.get("keywords") else None
            )
            mailbox_ids = (
                json.dumps(email_data.get("mailboxIds")) if email_data.get("mailboxIds") else None
            )
            has_attachment = 1 if email_data.get("hasAttachment") else 0

            # Optimize headers
            headers_dict: dict[str, list[str]] = {}
            for h in headers_list:
                name = h.get("name")
                value = h.get("value")
                if name not in headers_dict:
                    headers_dict[name] = []
                headers_dict[name].append(value)
            headers_json = json.dumps(headers_dict)

            try:
                # Use INSERT OR REPLACE to update existing snippets
                c.execute(
                    """
                    INSERT INTO emails
                    (source, provider_id, message_id, sender, sender_domain, to_address, subject, date, snippet, body_text, body_html, list_unsubscribe, list_unsubscribe_post, dmarc_fail, spf_fail, cc, bcc, reply_to, keywords, mailbox_ids, has_attachment, headers, arc_auth_results, has_arc, dkim_pass, dmarc_arc_override)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, provider_id) DO UPDATE SET
                        message_id=excluded.message_id,
                        sender=excluded.sender,
                        sender_domain=excluded.sender_domain,
                        to_address=excluded.to_address,
                        subject=excluded.subject,
                        date=excluded.date,
                        snippet=excluded.snippet,
                        body_text=excluded.body_text,
                        body_html=excluded.body_html,
                        list_unsubscribe=excluded.list_unsubscribe,
                        list_unsubscribe_post=excluded.list_unsubscribe_post,
                        dmarc_fail=excluded.dmarc_fail,
                        spf_fail=excluded.spf_fail,
                        cc=excluded.cc,
                        bcc=excluded.bcc,
                        reply_to=excluded.reply_to,
                        keywords=excluded.keywords,
                        mailbox_ids=excluded.mailbox_ids,
                        has_attachment=excluded.has_attachment,
                        headers=excluded.headers,
                        arc_auth_results=excluded.arc_auth_results,
                        has_arc=excluded.has_arc,
                        dkim_pass=excluded.dkim_pass,
                        dmarc_arc_override=excluded.dmarc_arc_override
                """,
                    (
                        source,
                        provider_id,
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
                        keywords,
                        mailbox_ids,
                        has_attachment,
                        sqlite_safe_text(headers_json),
                        sqlite_safe_text(arc_auth_results),
                        int(has_arc),
                        int(dkim_pass),
                        int(dmarc_arc_override),
                    ),
                )
            except Exception as e:
                print(f"Error inserting/updating {message_id}: {e}")

            if is_interactive:
                progress.advance(progress_task)
            else:
                processed += 1

        conn.commit()

    if is_interactive:
        progress.stop()

    conn.close()
    print("Fastmail ingestion complete.")


if __name__ == "__main__":
    ingest_fastmail()
