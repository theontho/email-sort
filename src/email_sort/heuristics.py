from email_sort.db import get_db
import fasttext  # type: ignore
import os
import urllib.request
import json
from bs4 import BeautifulSoup
from email_sort.config import get_setting, get_config_dir

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# Path relative to centralized config directory
MODEL_PATH = get_config_dir() / "models" / "lid.176.bin"


def download_model():
    if not os.path.exists(MODEL_PATH):
        print("Downloading fasttext language model (this takes a moment)...")
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        urllib.request.urlretrieve(MODEL_URL, MODEL_PATH)


def run_heuristics():
    download_model()
    # Suppress warning
    fasttext.FastText.eprint = lambda x: None
    model = fasttext.load_model(str(MODEL_PATH))

    conn = get_db()
    c = conn.cursor()

    for table_name in ["fastmail", "google_emails"]:
        c.execute(f"SELECT id, subject, snippet, to_address, headers, body_html FROM {table_name}")
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
                    email_id,
                )
            )

            if len(updates) >= 1000:
                c.executemany(
                    f"UPDATE {table_name} SET language=?, is_not_for_me=?, category=COALESCE(category, ?), action=COALESCE(action, ?), confidence=COALESCE(confidence, ?), body_unsubscribe_links=?, heuristic_matches=? WHERE id=?",
                    updates,
                )
                conn.commit()
                updates = []

        if updates:
            c.executemany(
                f"UPDATE {table_name} SET language=?, is_not_for_me=?, category=COALESCE(category, ?), action=COALESCE(action, ?), confidence=COALESCE(confidence, ?), body_unsubscribe_links=?, heuristic_matches=? WHERE id=?",
                updates,
            )
            conn.commit()

        # Thread-aware classification propagation
        print(f"Propagating thread classifications in {table_name}...")
        c.execute(f"""
            UPDATE {table_name} 
            SET category = (
                SELECT category FROM {table_name} e2 
                WHERE e2.thread_id = {table_name}.thread_id 
                AND e2.category IS NOT NULL 
                AND e2.thread_id != ''
                LIMIT 1
            )
            WHERE category IS NULL 
            AND thread_id != ''
            AND EXISTS (
                SELECT 1 FROM {table_name} e3 
                WHERE e3.thread_id = {table_name}.thread_id 
                AND e3.category IS NOT NULL
            )
        """)
        conn.commit()

    print("Heuristics complete.")

    print("Heuristics complete.")


if __name__ == "__main__":
    run_heuristics()
