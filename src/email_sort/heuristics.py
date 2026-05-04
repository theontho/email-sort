import email.utils
import json
import os
import re
import sys
import time
import urllib.request
import warnings
from collections import defaultdict
from datetime import UTC

import fasttext  # type: ignore
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from email_sort.config import get_config_dir, get_setting
from email_sort.db import EMAIL_TABLE, add_column_if_missing, get_db
from email_sort.progress import make_progress

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# Path relative to centralized config directory
MODEL_PATH = get_config_dir() / "models" / "lid.176.bin"


def _normalize_domains(values) -> tuple[set[str], set[str]]:
    addresses: set[str] = set()
    domains: set[str] = set()
    for value in values or []:
        item = str(value).strip().lower()
        if not item:
            continue
        if "@" in item:
            addresses.add(item)
            domains.add(item.rsplit("@", 1)[1])
        else:
            domains.add(item.lstrip("@"))
    return addresses, domains


def _domain_matches(domain: str, targets: set[str]) -> bool:
    return any(domain == target or domain.endswith(f".{target}") for target in targets)


def _addresses_match_domains(values: list[str], my_domains) -> bool:
    target_addresses, target_domains = _normalize_domains(my_domains)
    for _, address in email.utils.getaddresses(values):
        address = address.lower()
        if not address or "@" not in address:
            continue
        domain = address.rsplit("@", 1)[1]
        if address in target_addresses or _domain_matches(domain, target_domains):
            return True
    return False


def _text_matches(text: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, text, re.I) for pattern in patterns)


def _notification_match(
    sender: str,
    sender_domain: str,
    text: str,
    category: str,
    action: str,
    confidence: float,
    name: str,
    patterns: tuple[str, ...],
    domains: tuple[str, ...] = (),
    senders: tuple[str, ...] = (),
    identity_enough: bool = False,
) -> tuple[str, str, float, dict] | None:
    domain_hit = sender_domain in domains or any(
        sender_domain.endswith(f".{domain}") for domain in domains
    )
    sender_hit = any(token in sender for token in senders)
    text_hit = _text_matches(text, patterns)
    if not text_hit and not (identity_enough and (domain_hit or sender_hit)):
        return None

    evidence = {}
    if domain_hit:
        evidence[f"{name}_domain"] = sender_domain
    if sender_hit:
        evidence[f"{name}_sender"] = sender
    if text_hit:
        evidence[f"{name}_text"] = text[:160]
    return category, action, confidence, evidence


def _deterministic_notification_classification(
    sender: str,
    sender_domain: str,
    subject: str,
    snippet: str,
    headers: dict,
) -> tuple[str, str, float, dict] | None:
    sender_l = (sender or "").lower()
    domain_l = (sender_domain or "").lower()
    text = " ".join([sender_l, domain_l, subject or "", snippet or ""]).lower()

    security = _notification_match(
        sender_l,
        domain_l,
        text,
        "Security",
        "Authentication",
        0.96,
        "security_notification",
        (
            r"\b(two[- ]?factor|2fa|mfa|verification code|security code|one[- ]?time code|otp)\b",
            r"\b(password reset|reset your password|verify your email|confirm your email)\b",
            r"\b(new sign[- ]?in|new login|login attempt|suspicious activity)\b",
            r"\b(account locked|account access|security alert)\b",
        ),
        domains=("accounts.google.com", "appleid.apple.com", "github.com", "facebookmail.com"),
        senders=("security", "no-reply@accounts", "account-security"),
    )
    if security:
        return security

    shipping = _notification_match(
        sender_l,
        domain_l,
        text,
        "Shipping",
        "Informational",
        0.95,
        "shipping_notification",
        (
            r"\b(package|shipment|tracking|track your|delivery|delivered|out for delivery)\b",
            r"\b(expected delivery|scheduled delivery|order has shipped|shipping label)\b",
            r"\b(ups|usps|fedex|dhl)\b",
        ),
        domains=("ups.com", "usps.com", "fedex.com", "dhl.com"),
        senders=("tracking", "shipment", "delivery"),
        identity_enough=True,
    )
    if shipping:
        return shipping

    finance_mandatory = _notification_match(
        sender_l,
        domain_l,
        text,
        "Finance",
        "Mandatory",
        0.95,
        "finance_mandatory_notification",
        (
            r"\b(payment is due|payment due|statement available|bill is ready|minimum payment)\b",
            r"\b(account ending|transaction alert|fraud alert|card declined|deposit received)\b",
            r"\b(tax document|1099|invoice overdue)\b",
        ),
        domains=(
            "chase.com",
            "paypal.com",
            "citi.com",
            "americanexpress.com",
            "bankofamerica.com",
            "wellsfargo.com",
            "interactivebrokers.com",
        ),
        senders=("alerts@", "statement", "billing", "donotreply@interactivebrokers"),
    )
    if finance_mandatory:
        return finance_mandatory

    finance_receipt = _notification_match(
        sender_l,
        domain_l,
        text,
        "Finance",
        "Informational",
        0.92,
        "finance_receipt_notification",
        (
            r"\b(receipt|you sent a payment|payment received|payment confirmation)\b",
            r"\b(order confirmation|purchase confirmation|transaction receipt)\b",
        ),
        domains=("paypal.com", "stripe.com", "squareup.com"),
        senders=("receipt", "billing", "payments"),
    )
    if finance_receipt:
        return finance_receipt

    marketplace = _notification_match(
        sender_l,
        domain_l,
        text,
        "Shopping",
        "Informational",
        0.9,
        "marketplace_notification",
        (
            r"\b(marketplace|buyer message|seller message|new message from|offer received)\b",
            r"\b(item sold|item shipped|order update|your order|purchase update)\b",
            r"\b(auction|bid|outbid|listing|craigslist)\b",
        ),
        domains=("amazon.com", "amazon.ca", "ebay.com", "ebay.ca", "etsy.com", "craigslist.org"),
        senders=("marketplace", "endofitem", "reply@craigslist", "orders@"),
        identity_enough=True,
    )
    if marketplace:
        return marketplace

    social = _notification_match(
        sender_l,
        domain_l,
        text,
        "Social",
        "Social",
        0.9,
        "social_notification",
        (
            r"\b(friend request|tagged you|mentioned you|commented on|liked your|sent you a message)\b",
            r"\b(new follower|connection request|invited you|event invitation)\b",
        ),
        domains=("facebookmail.com", "linkedin.com", "twitter.com", "x.com", "instagram.com", "meetup.com"),
        senders=("confirm+", "eventmaster", "wallmaster"),
        identity_enough=True,
    )
    if social:
        return social

    calendar = _notification_match(
        sender_l,
        domain_l,
        text,
        "Personal",
        "Mandatory",
        0.88,
        "calendar_notification",
        (
            r"\b(calendar invitation|invitation:|updated invitation|event reminder|appointment reminder)\b",
            r"\b(meeting invitation|accepted:|declined:|tentative:)\b",
        ),
        domains=("calendar.google.com", "google.com", "icloud.com", "outlook.com"),
        senders=("calendar", "invitation"),
    )
    if calendar:
        return calendar

    return None


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
        return parsed.astimezone(UTC).replace(tzinfo=None)
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


def _log_progress(
    description: str, completed: int, total: int, started_at: float, last_update: float
) -> float:
    now = time.time()
    if now - last_update < 60:
        return last_update
    rate = completed / (now - started_at) if now > started_at else 0
    print(f"[{time.strftime('%H:%M:%S')}] {description}: {completed}/{total} ({rate:.2f}/s)")
    return now


def _update_duplicate_and_digest_flags(
    cursor, table_name: str = EMAIL_TABLE, interactive: bool = False
):
    cursor.execute(f"SELECT id, sender, subject, date FROM {table_name}")
    rows = [dict(row) for row in cursor.fetchall()]
    duplicate_ids: set[int] = set()
    by_sender_subject = defaultdict(list)
    by_sender = defaultdict(list)

    progress = make_progress() if interactive else None
    if progress:
        progress.start()
    try:
        started_at = time.time()
        last_update = started_at
        task = (
            progress.add_task("Preparing duplicate/digest rows", total=len(rows))
            if progress
            else None
        )
        for index, row in enumerate(rows, start=1):
            parsed = _parse_date(row.get("date"))
            row["parsed_date"] = parsed
            sender = (row.get("sender") or "").lower()
            if sender:
                by_sender[sender].append(row)
            if sender and parsed:
                by_sender_subject[(sender, _subject_key(row.get("subject")))].append(row)
            if progress and task is not None:
                progress.advance(task)
            elif rows:
                last_update = _log_progress(
                    f"Prepared duplicate/digest rows in {table_name}",
                    index,
                    len(rows),
                    started_at,
                    last_update,
                )

        started_at = time.time()
        last_update = started_at
        duplicate_groups = list(by_sender_subject.values())
        task = (
            progress.add_task("Detecting duplicate messages", total=len(duplicate_groups))
            if progress
            else None
        )
        for index, group in enumerate(duplicate_groups, start=1):
            group.sort(key=lambda item: item["parsed_date"])
            for row in group:
                window = [
                    other
                    for other in group
                    if abs((other["parsed_date"] - row["parsed_date"]).total_seconds()) <= 86400
                ]
                if len(window) > 1:
                    latest = max(window, key=lambda item: item["parsed_date"])
                    duplicate_ids.update(
                        item["id"] for item in window if item["id"] != latest["id"]
                    )
            if progress and task is not None:
                progress.advance(task)
            elif duplicate_groups:
                last_update = _log_progress(
                    f"Scanned duplicate groups in {table_name}",
                    index,
                    len(duplicate_groups),
                    started_at,
                    last_update,
                )

        digest_ids: set[int] = set()
        started_at = time.time()
        last_update = started_at
        sender_groups = list(by_sender.values())
        task = (
            progress.add_task("Detecting digest senders", total=len(sender_groups))
            if progress
            else None
        )
        for index, group in enumerate(sender_groups, start=1):
            dated = [row for row in group if row.get("parsed_date")]
            dated.sort(key=lambda item: item["parsed_date"])
            if any(_looks_like_digest(row.get("subject")) for row in group):
                digest_ids.update(
                    row["id"] for row in group if _looks_like_digest(row.get("subject"))
                )
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
            if progress and task is not None:
                progress.advance(task)
            elif sender_groups:
                last_update = _log_progress(
                    f"Scanned digest senders in {table_name}",
                    index,
                    len(sender_groups),
                    started_at,
                    last_update,
                )
    finally:
        if progress:
            progress.stop()

    cursor.execute(f"UPDATE {table_name} SET is_duplicate = 0, is_digest = 0")
    if duplicate_ids:
        cursor.executemany(
            f"UPDATE {table_name} SET is_duplicate = 1 WHERE id = ?",
            [(email_id,) for email_id in duplicate_ids],
        )
    if digest_ids:
        cursor.executemany(
            f"UPDATE {table_name} SET is_digest = 1 WHERE id = ?",
            [(email_id,) for email_id in digest_ids],
        )


def _propagate_thread_heuristics(conn, cursor, table_name: str, interactive: bool) -> int:
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_thread_id ON {table_name}(thread_id)"
    )
    conn.commit()
    cursor.execute(f"""
        SELECT thread_id, MIN(heuristic_category) AS heuristic_category
        FROM {table_name}
        WHERE thread_id IS NOT NULL
          AND thread_id != ''
          AND heuristic_category IS NOT NULL
        GROUP BY thread_id
    """)
    thread_categories = [tuple(row) for row in cursor.fetchall()]
    if not thread_categories:
        return 0

    progress = make_progress() if interactive else None
    if progress:
        progress.start()
    changed = 0
    try:
        task = (
            progress.add_task("Propagating thread classifications", total=len(thread_categories))
            if progress
            else None
        )
        started_at = time.time()
        last_update = started_at
        for index, (thread_id, category) in enumerate(thread_categories, start=1):
            cursor.execute(
                f"""
                UPDATE {table_name}
                SET heuristic_category = ?
                WHERE heuristic_category IS NULL
                  AND thread_id = ?
                """,
                (category, thread_id),
            )
            changed += cursor.rowcount
            if index % 1000 == 0:
                conn.commit()
            if progress and task is not None:
                progress.advance(task)
            else:
                last_update = _log_progress(
                    f"Propagated thread classifications in {table_name}",
                    index,
                    len(thread_categories),
                    started_at,
                    last_update,
                )
        conn.commit()
        return changed
    finally:
        if progress:
            progress.stop()


def _ensure_heuristics_schema(conn, cursor, table_name: str) -> None:
    add_column_if_missing(cursor, table_name, "heuristic_processed_at", "TEXT")
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_thread_id ON {table_name}(thread_id)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_heuristic_processed_at ON {table_name}(heuristic_processed_at)"
    )
    conn.commit()


def run_heuristics(recompute: bool = False):
    print("Starting heuristics... (can take a while)", flush=True)
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    download_model()
    # Suppress warning
    fasttext.FastText.eprint = lambda x: None
    model = fasttext.load_model(str(MODEL_PATH))

    conn = get_db()
    c = conn.cursor()
    is_interactive = sys.stdout.isatty()

    try:
        for table_name in [EMAIL_TABLE]:
            _ensure_heuristics_schema(conn, c, table_name)

            where_clause = "" if recompute else "WHERE heuristic_processed_at IS NULL"
            c.execute(
                f"""
                SELECT id, sender, sender_domain, subject, snippet, to_address,
                       delivered_to, headers, body_html, dmarc_fail, has_arc,
                       arc_auth_results
                FROM {table_name}
                {where_clause}
                """
            )
            rows = c.fetchall()

            mode = "all" if recompute else "new or unprocessed"
            print(f"Running heuristics on {len(rows)} {mode} emails in {table_name}...")

            updates = []
            my_domains = get_setting("my_domains", ["icloud.com", "appleid.com", "gmail.com"])
            progress = make_progress() if rows and is_interactive else None
            if progress:
                progress.start()
            try:
                task = (
                    progress.add_task(f"Running heuristics in {table_name}", total=len(rows))
                    if progress
                    else None
                )
                started_at = time.time()
                last_update = started_at
                for index, row in enumerate(rows, start=1):
                    email_id = row["id"]
                    sender = row["sender"] or ""
                    sender_domain = row["sender_domain"] or ""
                    subject = row["subject"] or ""
                    snippet = row["snippet"] or ""
                    to_address = row["to_address"] or ""
                    delivered_to = row["delivered_to"] or ""
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

                    unsub_links = []
                    if body_html:
                        try:
                            soup = BeautifulSoup(body_html, "html.parser")
                            for a in soup.find_all("a", href=True):
                                text = a.get_text().lower()
                                href = str(a["href"]).lower()
                                if (
                                    "unsubscribe" in text
                                    or "unsubscribe" in href
                                    or "opt-out" in text
                                ):
                                    unsub_links.append(str(a["href"]))
                            unsub_links = unsub_links[:5]
                        except Exception:
                            pass
                    unsub_links_json = json.dumps(unsub_links) if unsub_links else None

                    text_to_detect = (subject + " " + snippet)[:200].replace("\n", " ").strip()
                    lang = "unknown"
                    if text_to_detect:
                        try:
                            predictions = model.predict(text_to_detect)
                            lang = predictions[0][0].replace("__label__", "")
                        except Exception:
                            pass

                    is_not_for_me = 0
                    if not _addresses_match_domains([to_address, delivered_to], my_domains):
                        is_not_for_me = 1

                    category = None
                    action = None
                    confidence = None
                    heuristic_matches = {}

                    def get_header(name, headers=headers):
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

                    deterministic = _deterministic_notification_classification(
                        sender, sender_domain, subject, snippet, headers
                    )
                    if deterministic:
                        category, action, confidence, matches = deterministic
                        heuristic_matches.update(matches)

                    if list_id and not category:
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

                    heuristic_matches_json = (
                        json.dumps(heuristic_matches) if heuristic_matches else None
                    )
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
                            f"UPDATE {table_name} SET language=?, is_not_for_me=?, heuristic_category=COALESCE(heuristic_category, ?), heuristic_action=COALESCE(heuristic_action, ?), heuristic_confidence=COALESCE(heuristic_confidence, ?), body_unsubscribe_links=?, heuristic_matches=?, dmarc_arc_override=?, heuristic_processed_at=CURRENT_TIMESTAMP WHERE id=?",
                            updates,
                        )
                        conn.commit()
                        updates = []
                    if progress and task is not None:
                        progress.advance(task)
                    else:
                        last_update = _log_progress(
                            f"Ran heuristics in {table_name}",
                            index,
                            len(rows),
                            started_at,
                            last_update,
                        )
            finally:
                if progress:
                    progress.stop()

            if updates:
                c.executemany(
                    f"UPDATE {table_name} SET language=?, is_not_for_me=?, heuristic_category=COALESCE(heuristic_category, ?), heuristic_action=COALESCE(heuristic_action, ?), heuristic_confidence=COALESCE(heuristic_confidence, ?), body_unsubscribe_links=?, heuristic_matches=?, dmarc_arc_override=?, heuristic_processed_at=CURRENT_TIMESTAMP WHERE id=?",
                    updates,
                )
                conn.commit()

            print(f"Detecting duplicates and digests in {table_name}...")
            _update_duplicate_and_digest_flags(c, table_name=table_name, interactive=is_interactive)
            conn.commit()

            print(f"Propagating thread heuristic classifications in {table_name}...")
            changed = _propagate_thread_heuristics(conn, c, table_name, is_interactive)
            print(f"Propagated thread classifications to {changed} emails in {table_name}.")
    except KeyboardInterrupt:
        conn.rollback()
        print("\nHeuristics interrupted; committed work up to the last completed batch.")
        raise SystemExit(130) from None
    finally:
        conn.close()

    print("Heuristics complete.")


if __name__ == "__main__":
    run_heuristics()
