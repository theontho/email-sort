import csv
import json
from pathlib import Path

from email_sort.db import get_db


OUT_DIR = Path("out")


def export_ban_list(path: str = "out/ban_list.csv") -> None:
    OUT_DIR.mkdir(exist_ok=True)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sender_domain, language, is_not_for_me, category, dmarc_fail, dmarc_arc_override, COUNT(*) AS count
            FROM (
                SELECT sender_domain, language, is_not_for_me, category, dmarc_fail, dmarc_arc_override FROM fastmail
                UNION ALL
                SELECT sender_domain, language, is_not_for_me, category, dmarc_fail, dmarc_arc_override FROM google_emails
            )
            WHERE language != 'en' OR is_not_for_me = 1 OR category = 'Spam' OR (dmarc_fail = 1 AND dmarc_arc_override = 0)
            GROUP BY sender_domain
            ORDER BY count DESC
            """
        )
        with open(path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["sender_domain", "reason", "count"])
            for row in cursor.fetchall():
                reason = "Spam"
                if row["language"] != "en":
                    reason = f"Foreign Language ({row['language']})"
                elif row["is_not_for_me"]:
                    reason = "Not for me"
                elif row["dmarc_fail"] and not row["dmarc_arc_override"]:
                    reason = "Authentication Failed"
                writer.writerow([row["sender_domain"], reason, row["count"]])
    finally:
        conn.close()
    print(f"Wrote {path}")


def export_unsubscribe_list(path: str = "out/unsubscribe_list.csv") -> None:
    OUT_DIR.mkdir(exist_ok=True)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sender, sender_domain, category, list_unsubscribe, body_unsubscribe_links, is_digest, COUNT(*) AS count
            FROM (
                SELECT sender, sender_domain, category, list_unsubscribe, body_unsubscribe_links, is_digest FROM fastmail
                UNION ALL
                SELECT sender, sender_domain, category, list_unsubscribe, body_unsubscribe_links, is_digest FROM google_emails
            )
            WHERE (list_unsubscribe IS NOT NULL OR body_unsubscribe_links IS NOT NULL)
              AND (category IN ('Promotional','Newsletter','Spam','Social','Tech','Shopping','Health') OR is_digest = 1)
            GROUP BY sender
            ORDER BY is_digest DESC, count DESC
            """
        )
        with open(path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "sender",
                    "sender_domain",
                    "category",
                    "is_digest",
                    "count",
                    "list_unsubscribe",
                    "body_links",
                ]
            )
            for row in cursor.fetchall():
                writer.writerow(
                    [
                        row["sender"],
                        row["sender_domain"],
                        row["category"],
                        row["is_digest"],
                        row["count"],
                        row["list_unsubscribe"],
                        row["body_unsubscribe_links"],
                    ]
                )
    finally:
        conn.close()
    print(f"Wrote {path}")


def export_sender_reputation(path: str = "out/sender_reputation.csv") -> None:
    OUT_DIR.mkdir(exist_ok=True)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sender_stats ORDER BY total_emails DESC")
        rows = cursor.fetchall()
        with open(path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([column[0] for column in cursor.description])
            for row in rows:
                writer.writerow([row[column[0]] for column in cursor.description])
    finally:
        conn.close()
    print(f"Wrote {path}")


def export_corrections(path: str = "out/corrections.jsonl") -> None:
    OUT_DIR.mkdir(exist_ok=True)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM corrections ORDER BY corrected_at DESC")
        with open(path, "w") as file:
            for row in cursor.fetchall():
                file.write(json.dumps(dict(row), sort_keys=True) + "\n")
    finally:
        conn.close()
    print(f"Wrote {path}")


def export_results() -> None:
    export_sender_reputation()
    export_ban_list()
    export_unsubscribe_list()
