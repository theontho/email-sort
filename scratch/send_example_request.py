import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get("LITELLM_URL", "http://localhost:1234/v1")
api_key = os.environ.get("LMSTUDIO_KEY", "lm-studio")
model = os.environ.get("MODEL_NAME", "qwen3.5-9b")

headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

data = {
    "model": model,
    "messages": [
        {
            "role": "system",
            "content": "You are an AI that classifies emails. Output ONLY the category and confidence, comma separated. Example: 'Promotional, 0.95'. Categories: Billing, Newsletter, Work, Personal, Promotional, Security, Shipping, Travel, Spam, Other",
        },
        {
            "role": "user",
            "content": "Sender: hello@e.gymshark.com\nSubject: Email gang gets an extra 20% off* 🫵\nSnippet: Shop the sale for up to 50% off* plus 20% off* 👏",
        },
    ],
    "max_tokens": 200,
    "temperature": 0.1,
}

print(f"Sending request to {url}/chat/completions...")
print(f"Request Body:\n{json.dumps(data, indent=2)}")
print("-" * 40)

try:
    response = requests.post(f"{url}/chat/completions", headers=headers, json=data, timeout=120)
    print(f"Status Code: {response.status_code}")
    print("Raw Response Body:")
    print(response.text)
except Exception as e:
    print(f"Error: {e}")
