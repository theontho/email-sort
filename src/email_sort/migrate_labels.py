import json
import sqlite3
from email_sort.db import get_db
from tqdm import tqdm

def migrate_labels(table_name="google_emails"):
    conn = get_db()
    c = conn.cursor()
    
    print(f"Fetching emails from {table_name}...")
    c.execute(f"SELECT id, headers FROM {table_name} WHERE mailbox_ids IS NULL OR thread_id IS NULL OR delivered_to IS NULL")
    rows = c.fetchall()
    
    print(f"Migrating labels for {len(rows)} emails...")
    
    for i, (email_id, headers_json) in enumerate(tqdm(rows, desc="Migrating metadata")):
        if not headers_json:
            continue
            
        try:
            headers = json.loads(headers_json)
            
            # Gmail labels
            gmail_labels = headers.get("X-Gmail-Labels", [])
            mailbox_ids = ",".join(gmail_labels) if gmail_labels else ""
            
            # Thread ID
            thread_ids = headers.get("X-GM-THRID", [])
            thread_id = thread_ids[0] if thread_ids else ""
            
            # Delivered To
            delivered_tos = headers.get("Delivered-To", [])
            delivered_to = delivered_tos[0] if delivered_tos else ""
            
            c.execute(f"UPDATE {table_name} SET mailbox_ids = ?, thread_id = ?, delivered_to = ? WHERE id = ?", 
                      (mailbox_ids, thread_id, delivered_to, email_id))
        except Exception as e:
            print(f"Error migrating {email_id}: {e}")
            
        if i % 1000 == 0:
            conn.commit()
            
    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    import sys
    table = sys.argv[1] if len(sys.argv) > 1 else "google_emails"
    migrate_labels(table)
