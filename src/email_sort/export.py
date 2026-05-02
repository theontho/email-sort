import csv
from email_sort.db import get_db


def export_results():
    conn = get_db()
    c = conn.cursor()

    print("Generating ban_list.csv...")
    c.execute("""
        SELECT sender, sender_domain, language, is_not_for_me, category, dmarc_fail
        FROM emails
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
        SELECT sender, list_unsubscribe, category
        FROM emails
        WHERE list_unsubscribe != '' 
        AND category IN ('Promotional', 'Newsletter', 'Spam', 'Social', 'Tech', 'Shopping', 'Health')
        GROUP BY sender
    """)
    with open("unsubscribe_list.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Sender", "Category", "Unsubscribe Header"])
        for row in c.fetchall():
            writer.writerow([row["sender"], row["category"], row["list_unsubscribe"]])

    print("Export complete. Check ban_list.csv and unsubscribe_list.csv")


if __name__ == "__main__":
    export_results()
