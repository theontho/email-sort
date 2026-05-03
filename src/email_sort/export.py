import csv
import requests
from email_sort.db import get_db


def execute_unsubscribe(email_id, list_unsub, list_unsub_post):
    """
    Implements RFC 8058 and fallbacks.
    """
    if not list_unsub:
        return False

    # RFC 8058: Check for HTTPS and List-Unsubscribe-Post: List-Unsubscribe=One-Click
    if "List-Unsubscribe=One-Click" in (list_unsub_post or ""):
        # Extract HTTPS URL from list_unsub
        # list_unsub can be <mailto:xxx>, <https://xxx>
        import re

        urls = re.findall(r"<(https?://[^>]+)>", list_unsub)
        if urls:
            url = urls[0]
            try:
                print(f"Executing RFC 8058 POST to {url}")
                res = requests.post(
                    url,
                    data={"List-Unsubscribe": "One-Click"},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=10,
                )
                if res.status_code < 300:
                    return True
            except Exception as e:
                print(f"Error in RFC 8058 POST: {e}")

    # Fallback 1: HTTP GET
    import re

    urls = re.findall(r"<(https?://[^>]+)>", list_unsub)
    if urls:
        url = urls[0]
        try:
            print(f"Executing HTTP GET fallback to {url}")
            res = requests.get(url, timeout=10)
            if res.status_code < 300:
                return True
        except Exception as e:
            print(f"Error in HTTP GET: {e}")

    # Fallback 2: Mailto (not implemented here as it requires sending mail)
    return False


def export_results():
    conn = get_db()
    c = conn.cursor()

    print("Generating sender_reputation.csv...")
    c.execute("""
        SELECT sender_domain,
               COUNT(*) as total_emails,
               AVG(CASE WHEN category IN ('Spam','Promotional') THEN 1.0 ELSE 0.0 END) as spam_ratio,
               SUM(dmarc_fail) as dmarc_failures,
               MIN(date) as first_seen,
               MAX(date) as last_seen
        FROM fastmail
        GROUP BY sender_domain
        ORDER BY total_emails DESC
    """)
    with open("sender_reputation.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Domain",
                "Total Emails",
                "Spam/Promo Ratio",
                "DMARC Failures",
                "First Seen",
                "Last Seen",
            ]
        )
        for row in c.fetchall():
            writer.writerow(
                [
                    row["sender_domain"],
                    row["total_emails"],
                    f"{row['spam_ratio']:.2f}",
                    row["dmarc_failures"],
                    row["first_seen"],
                    row["last_seen"],
                ]
            )

    print("Generating ban_list.csv...")
    c.execute("""
        SELECT sender, sender_domain, language, is_not_for_me, category, dmarc_fail
        FROM fastmail
        WHERE language != 'en' OR is_not_for_me = 1 OR category = 'Spam' OR dmarc_fail = 1
        GROUP BY sender_domain
        ORDER BY count(id) DESC
    """)
    with open("ban_list.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Sender Domain", "Reason"])
        for row in c.fetchall():
            reason = "Spam"
            if row["language"] != "en":
                reason = f"Foreign Language ({row['language']})"
            elif row["is_not_for_me"]:
                reason = "Not for me"
            elif row["dmarc_fail"]:
                reason = "Authentication Failed"
            writer.writerow([row["sender_domain"], reason])

    print("Generating unsubscribe_list.csv...")
    c.execute("""
        SELECT id, sender, list_unsubscribe, list_unsubscribe_post, category, body_unsubscribe_links
        FROM fastmail
        WHERE (list_unsubscribe != '' OR body_unsubscribe_links IS NOT NULL)
        AND category IN ('Promotional', 'Newsletter', 'Spam', 'Social', 'Tech', 'Shopping', 'Health')
        GROUP BY sender
    """)
    with open("unsubscribe_list.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Sender", "Category", "Unsubscribe Header", "Body Links"])
        for row in c.fetchall():
            writer.writerow(
                [
                    row["sender"],
                    row["category"],
                    row["list_unsubscribe"],
                    row["body_unsubscribe_links"],
                ]
            )

    print("Export complete.")


if __name__ == "__main__":
    export_results()
