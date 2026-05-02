import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()


def debug_jmap():
    token = os.environ.get("FASTMAIL_TOKEN")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = requests.get("https://api.fastmail.com/jmap/session", headers=headers)
    session = res.json()
    api_url = session["apiUrl"]
    account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]

    # Get the latest message ID
    query_req = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [["Email/query", {"accountId": account_id, "limit": 1}, "0"]],
    }
    res = requests.post(api_url, headers=headers, json=query_req)
    email_id = res.json()["methodResponses"][0][1]["ids"][0]

    # Fetch with bodyValues
    fetch_req = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            [
                "Email/get",
                {
                    "accountId": account_id,
                    "ids": [email_id],
                    "properties": [
                        "id",
                        "subject",
                        "preview",
                        "textBody",
                        "bodyValues",
                    ],
                    "bodyProperties": ["partId", "value", "isTruncated"],
                },
                "0",
            ]
        ],
    }
    res = requests.post(api_url, headers=headers, json=fetch_req)
    print(json.dumps(res.json(), indent=2))


if __name__ == "__main__":
    debug_jmap()
