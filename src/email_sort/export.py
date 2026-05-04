import csv
import json
from pathlib import Path

from email_sort.db import EMAIL_TABLE, get_db

OUT_DIR = Path("out")


def export_ban_list(path: str = "out/ban_list.csv") -> None:
    OUT_DIR.mkdir(exist_ok=True)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT sender_domain,
                   CASE
                       WHEN language != 'en' THEN 'Foreign Language (' || language || ')'
                       WHEN is_not_for_me = 1 THEN 'Not for me'
                       WHEN rule_source = 'manual-correction' AND rule_category = 'Spam' THEN 'Spam (Rule)'
                       WHEN category = 'Spam' THEN 'Spam (LLM)'
                       WHEN rule_category = 'Spam' THEN 'Spam (Rule)'
                        WHEN heuristic_category = 'Spam' THEN 'Spam (Heuristic)'
                       WHEN dmarc_fail = 1 AND dmarc_arc_override = 0 THEN 'Authentication Failed'
                       ELSE 'Unknown'
                   END AS reason,
                   COUNT(*) AS count
            FROM {EMAIL_TABLE}
            WHERE language != 'en'
               OR is_not_for_me = 1
                OR category = 'Spam'
                OR rule_category = 'Spam'
                OR heuristic_category = 'Spam'
               OR (dmarc_fail = 1 AND dmarc_arc_override = 0)
            GROUP BY sender_domain, reason
            ORDER BY count DESC
            """
        )
        with open(path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["sender_domain", "reason", "count"])
            for row in cursor.fetchall():
                writer.writerow([row["sender_domain"], row["reason"], row["count"]])
    finally:
        conn.close()
    print(f"Wrote {path}")


def export_unsubscribe_list(path: str = "out/unsubscribe_list.csv") -> None:
    OUT_DIR.mkdir(exist_ok=True)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT sender,
                   sender_domain,
                   COALESCE(
                       CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
                       category,
                       rule_category,
                       heuristic_category
                   ) AS category,
                   MIN(list_unsubscribe) AS list_unsubscribe,
                   MIN(body_unsubscribe_links) AS body_unsubscribe_links,
                   MAX(is_digest) AS is_digest,
                   COUNT(*) AS count
            FROM {EMAIL_TABLE}
            WHERE (list_unsubscribe IS NOT NULL OR body_unsubscribe_links IS NOT NULL)
              AND (COALESCE(
                       CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
                       category,
                       rule_category,
                       heuristic_category
                   ) IN ('Promotional','Newsletter','Spam','Social','Tech','Shopping','Health','Automated') OR is_digest = 1)
            GROUP BY sender, sender_domain, COALESCE(
                CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
                category,
                rule_category,
                heuristic_category
            )
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
