import fasttext  # type: ignore
import json
import os
import re
import urllib.request
import warnings
from collections import defaultdict
from datetime import timezone

import email.utils
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning

from email_sort.config import get_setting, get_config_dir
from email_sort.db import EMAIL_TABLE, get_db

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# Path relative to centralized config directory
MODEL_PATH = get_config_dir() / "models" / "lid.176.bin"


def download_model():
    if not os.path.exists(MODEL_PATH):
        print("Downloading fasttext language model (this takes a moment)...")
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        urllib.request.urlretrieve(MODEL_URL, MODEL_PATH)


def _parse_date(value):
    if not value:
        return None
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        try:
            parsed = email.utils.parsedate_to_datetime(str(value))
        except Exception:
            return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _subject_key(subject):
    return re.sub(r"\s+", " ", (subject or "").strip().lower())


def _looks_like_digest(subject):
    subject = subject or ""
    patterns = [
        r"\bdaily digest\b",
        r"\bweekly (summary|digest|roundup)\b",
        r"\bmonthly (summary|digest|roundup)\b",
        r"\bnewsletter\s*#?\d+\b",
        r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b",
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}\b",
    ]
    return any(re.search(pattern, subject, re.I) for pattern in patterns)


def _update_duplicate_and_digest_flags(cursor):
    cursor.execute(f"SELECT id, sender, subject, date FROM {EMAIL_TABLE}")
    rows = [dict(row) for row in cursor.fetchall()]
    duplicate_ids: set[int] = set()
    by_sender_subject = defaultdict(list)
    by_sender = defaultdict(list)

    for row in rows:
        parsed = _parse_date(row.get("date"))
        row["parsed_date"] = parsed
        sender = (row.get("sender") or "").lower()
        if sender:
            by_sender[sender].append(row)
        if sender and parsed:
            by_sender_subject[(sender, _subject_key(row.get("subject")))].append(row)

    for group in by_sender_subject.values():
        group.sort(key=lambda item: item["parsed_date"])
        for idx, row in enumerate(group):
            window = [
                other
                for other in group
                if abs((other["parsed_date"] - row["parsed_date"]).total_seconds()) <= 86400
            ]
            if len(window) > 1:
                latest = max(window, key=lambda item: item["parsed_date"])
                duplicate_ids.update(item["id"] for item in window if item["id"] != latest["id"])

    digest_ids: set[int] = set()
    for group in by_sender.values():
        dated = [row for row in group if row.get("parsed_date")]
        dated.sort(key=lambda item: item["parsed_date"])
        if any(_looks_like_digest(row.get("subject")) for row in group):
            digest_ids.update(row["id"] for row in group if _looks_like_digest(row.get("subject")))
        if len(dated) >= 3:
            gaps = [
                (dated[i]["parsed_date"] - dated[i - 1]["parsed_date"]).total_seconds() / 86400
                for i in range(1, len(dated))
            ]
            regular = any(
                sum(1 for gap in gaps if abs(gap - cadence) <= tolerance) >= 2
                for cadence, tolerance in ((1, 0.35), (7, 1.0), (30, 3.0))
            )
            if regular:
                digest_ids.update(
                    row["id"] for row in group if _looks_like_digest(row.get("subject"))
                )

    cursor.execute(f"UPDATE {EMAIL_TABLE} SET is_duplicate = 0, is_digest = 0")
    if duplicate_ids:
        cursor.executemany(
            f"UPDATE {EMAIL_TABLE} SET is_duplicate = 1 WHERE id = ?",
            [(email_id,) for email_id in duplicate_ids],
        )
    if digest_ids:
        cursor.executemany(
            f"UPDATE {EMAIL_TABLE} SET is_digest = 1 WHERE id = ?",
            [(email_id,) for email_id in digest_ids],
        )


def run_heuristics():
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    download_model()
    # Suppress warning
    fasttext.FastText.eprint = lambda x: None
    model = fasttext.load_model(str(MODEL_PATH))

    conn = get_db()
    c = conn.cursor()

    for table_name in [EMAIL_TABLE]:
        c.execute(
            f"SELECT id, subject, snippet, to_address, headers, body_html, dmarc_fail, has_arc, arc_auth_results FROM {table_name}"
        )
        rows = c.fetchall()

        if not rows:
            continue

        print(f"Running heuristics on {len(rows)} emails in {table_name}...")

        updates = []
        my_domains = get_setting("my_domains", ["icloud.com", "appleid.com", "gmail.com"])

        for row in rows:
            email_id = row["id"]
            subject = row["subject"] or ""
            snippet = row["snippet"] or ""
            to_address = row["to_address"] or ""
            headers_raw = row["headers"]
            body_html = row["body_html"] or ""
            arc_auth_results = (row["arc_auth_results"] or "").lower()
            dmarc_arc_override = int(
                bool(row["dmarc_fail"])
                and bool(row["has_arc"])
                and "dmarc=pass" in arc_auth_results
            )
            headers = {}
            if headers_raw:
                try:
                    headers = json.loads(headers_raw)
                except Exception:
                    pass

            # HTML Unsubscribe link extraction
            unsub_links = []
            if body_html:
                try:
                    soup = BeautifulSoup(body_html, "html.parser")
                    for a in soup.find_all("a", href=True):
                        text = a.get_text().lower()
                        href = str(a["href"]).lower()
                        if "unsubscribe" in text or "unsubscribe" in href or "opt-out" in text:
                            unsub_links.append(str(a["href"]))
                    # Limit to first few to keep DB clean
                    unsub_links = unsub_links[:5]
                except Exception:
                    pass
            unsub_links_json = json.dumps(unsub_links) if unsub_links else None

            # Language detection
            text_to_detect = (subject + " " + snippet)[:200].replace("\n", " ").strip()
            lang = "unknown"
            if text_to_detect:
                try:
                    predictions = model.predict(text_to_detect)
                    lang = predictions[0][0].replace("__label__", "")
                except Exception:
                    pass

            # Recipient check
            is_not_for_me = 0
            to_addr_lower = to_address.lower()
            if not any(domain in to_addr_lower for domain in my_domains):
                is_not_for_me = 1

            # Header-based classification
            category = None
            action = None
            confidence = None
            heuristic_matches = {}

            # Helper to check headers (handles list values from ingest)
            def get_header(name):
                val = headers.get(name) or headers.get(name.lower())
                if isinstance(val, list) and val:
                    return val[0]
                return val or ""

            list_id = get_header("List-Id")
            precedence = get_header("Precedence").lower()
            auto_submitted = get_header("Auto-Submitted").lower()
            x_auto_suppress = get_header("X-Auto-Response-Suppress").lower()
            feedback_id = get_header("Feedback-ID")
            x_mailer = get_header("X-Mailer").lower()

            # 1. Newsletter / Promotional (Strong signals)
            if list_id:
                category = "Newsletter"
                action = "Informational"
                confidence = 1.0
                heuristic_matches["List-Id"] = list_id

            esp_headers = {
                "X-Mailgun-Sid": get_header("X-Mailgun-Sid"),
                "X-SES-Outgoing": get_header("X-SES-Outgoing"),
                "X-SendGrid-Track": get_header("X-SendGrid-Track"),
            }
            for h_name, h_val in esp_headers.items():
                if h_val:
                    category = category or "Promotional"
                    action = action or "Informational"
                    confidence = confidence or 0.9
                    heuristic_matches[h_name] = h_val

            if "campaign" in x_mailer or "mailchimp" in x_mailer:
                category = category or "Promotional"
                action = action or "Informational"
                confidence = confidence or 0.9
                heuristic_matches["X-Mailer"] = x_mailer
            elif feedback_id:
                category = category or "Promotional"
                action = action or "Informational"
                confidence = confidence or 0.8
                heuristic_matches["Feedback-ID"] = feedback_id

            # 2. Automated (Receipts, Alerts, Notifications)
            if auto_submitted in ["auto-generated", "auto-replied"]:
                category = "Automated"
                action = "Archive"
                confidence = 1.0
                heuristic_matches["Auto-Submitted"] = auto_submitted
            elif x_auto_suppress:
                category = "Automated"
                action = "Archive"
                confidence = 1.0
                heuristic_matches["X-Auto-Response-Suppress"] = x_auto_suppress
            elif precedence in ["bulk", "list", "junk"]:
                if not category:  # Don't override Newsletter if already set
                    category = "Automated"
                    action = "Informational"
                    confidence = 0.8
                    heuristic_matches["Precedence"] = precedence

            if unsub_links and not category:
                category = "Automated"
                action = "Informational"
                confidence = 0.8
                heuristic_matches["body_unsubscribe_links"] = unsub_links

            heuristic_matches_json = json.dumps(heuristic_matches) if heuristic_matches else None

            updates.append(
                (
                    lang,
                    is_not_for_me,
                    category,
                    action,
                    confidence,
                    unsub_links_json,
                    heuristic_matches_json,
                    dmarc_arc_override,
                    email_id,
                )
            )

            if len(updates) >= 1000:
                c.executemany(
                    f"UPDATE {table_name} SET language=?, is_not_for_me=?, heuristic_category=COALESCE(heuristic_category, ?), heuristic_action=COALESCE(heuristic_action, ?), heuristic_confidence=COALESCE(heuristic_confidence, ?), body_unsubscribe_links=?, heuristic_matches=?, dmarc_arc_override=? WHERE id=?",
                    updates,
                )
                conn.commit()
                updates = []

        if updates:
            c.executemany(
                f"UPDATE {table_name} SET language=?, is_not_for_me=?, heuristic_category=COALESCE(heuristic_category, ?), heuristic_action=COALESCE(heuristic_action, ?), heuristic_confidence=COALESCE(heuristic_confidence, ?), body_unsubscribe_links=?, heuristic_matches=?, dmarc_arc_override=? WHERE id=?",
                updates,
            )
            conn.commit()

        print(f"Detecting duplicates and digests in {table_name}...")
        _update_duplicate_and_digest_flags(c)
        conn.commit()

        # Thread-aware heuristic propagation
        print(f"Propagating thread heuristic classifications in {table_name}...")
        c.execute(f"""
            UPDATE {table_name} 
            SET heuristic_category = (
                SELECT heuristic_category FROM {table_name} e2 
                WHERE e2.thread_id = {table_name}.thread_id 
                AND e2.heuristic_category IS NOT NULL 
                AND e2.thread_id != ''
                LIMIT 1
            )
            WHERE heuristic_category IS NULL 
            AND thread_id != ''
            AND EXISTS (
                SELECT 1 FROM {table_name} e3 
                WHERE e3.thread_id = {table_name}.thread_id 
                AND e3.heuristic_category IS NOT NULL
            )
        """)
        conn.commit()

    print("Heuristics complete.")


if __name__ == "__main__":
    run_heuristics()
