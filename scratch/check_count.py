import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_fastmail_count():
    token = os.environ.get("FASTMAIL_TOKEN")
    if not token:
        print("Please set FASTMAIL_TOKEN environment variable.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    res = requests.get("https://api.fastmail.com/jmap/session", headers=headers)
    res.raise_for_status()
    session = res.json()

    api_url = session["apiUrl"]
    account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]

    # Query for all messages to get the total count
    query_req = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Email/query", {"accountId": account_id, "limit": 0}, "0"]
        ],
    }

    res = requests.post(api_url, headers=headers, json=query_req)
    res.raise_for_status()
    query_res = res.json()

    total = query_res["methodResponses"][0][1].get("total", "unknown")
    print(f"Total Fastmail emails: {total}")

    # Also check specific folders if possible
    query_folders = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Mailbox/get", {"accountId": account_id}, "0"]
        ],
    }
    res = requests.post(api_url, headers=headers, json=query_folders)
    res.raise_for_status()
    folders_res = res.json()
    
    mailboxes = folders_res["methodResponses"][0][1].get("list", [])
    print("\nBreakdown by folder:")
    for mb in mailboxes:
        print(f"- {mb['name']}: {mb['totalEmails']} emails ({mb['unreadEmails']} unread)")

if __name__ == "__main__":
    get_fastmail_count()
