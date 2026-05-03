import requests
import json
import sys
import time
from tqdm import tqdm
from email_sort.db import get_db
from email_sort.config import get_setting


def ingest_fastmail():
    token = get_setting("fastmail_token")
    if not token:
        print("Please set FASTMAIL_TOKEN environment variable.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    res = requests.get("https://api.fastmail.com/jmap/session", headers=headers)
    res.raise_for_status()
    session = res.json()

    api_url = session["apiUrl"]
    account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]

    print("Querying Fastmail for message IDs...")
    query_req = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [["Email/query", {"accountId": account_id, "limit": 50000}, "0"]],
    }

    res = requests.post(api_url, headers=headers, json=query_req)
    res.raise_for_status()
    query_res = res.json()

    email_ids = query_res["methodResponses"][0][1]["ids"]
    print(f"Found {len(email_ids)} emails. Fetching details...")

    conn = get_db()
    c = conn.cursor()

    is_interactive = sys.stdout.isatty()
    if is_interactive:
        pbar = tqdm(total=len(email_ids), desc="Ingesting Fastmail")
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

        res = requests.post(api_url, headers=headers, json=fetch_req)
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
            source = "fastmail"
            message_id = email_data["id"]
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
            list_unsubscribe = next(
                (h["value"] for h in headers_list if h["name"].lower() == "list-unsubscribe"),
                "",
            )
            list_unsubscribe_post = next(
                (h["value"] for h in headers_list if h["name"].lower() == "list-unsubscribe-post"),
                "",
            )
            auth_results = next(
                (h["value"] for h in headers_list if h["name"].lower() == "authentication-results"),
                "",
            ).lower()
            dmarc_fail = "dmarc=fail" in auth_results
            spf_fail = "spf=fail" in auth_results

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
                    INSERT INTO fastmail 
                    (source, message_id, sender, sender_domain, to_address, subject, date, snippet, body_text, body_html, list_unsubscribe, list_unsubscribe_post, dmarc_fail, spf_fail, cc, bcc, reply_to, keywords, mailbox_ids, has_attachment, headers)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(message_id) DO UPDATE SET 
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
                        headers=excluded.headers
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
                        keywords,
                        mailbox_ids,
                        has_attachment,
                        headers_json,
                    ),
                )
            except Exception as e:
                print(f"Error inserting/updating {message_id}: {e}")

            if is_interactive:
                pbar.update(1)
            else:
                processed += 1

        conn.commit()

    if is_interactive:
        pbar.close()

    conn.close()
    print("Fastmail ingestion complete.")


if __name__ == "__main__":
    ingest_fastmail()
