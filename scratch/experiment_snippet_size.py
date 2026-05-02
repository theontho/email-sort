import os
import time
import requests
from dotenv import load_dotenv
from email_sort.db import get_db

load_dotenv()

url = os.environ.get("LITELLM_URL", "http://localhost:1234/v1")
api_key = os.environ.get("LMSTUDIO_KEY", "lm-studio")
model = os.environ.get("MODEL_NAME", "mistralai/devstral-small-2-2512")

def test_snippet_size(sender, subject, snippet, size):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # Pad snippet if it's too short for the experiment
    if len(snippet) < size:
        padded_snippet = (snippet * (size // len(snippet) + 1))[:size]
    else:
        padded_snippet = snippet[:size]
        
    prompt_body = f"Sender: {sender}\nSubject: {subject}\nSnippet: {padded_snippet}"
    
    data = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You are a highly efficient email classifier. Output ONLY the category and confidence, comma separated. Example: 'Promotional, 0.95'. Categories: Billing, Newsletter, Work, Personal, Promotional, Security, Shipping, Travel, Spam, Other"
            },
            {
                "role": "user",
                "content": prompt_body
            }
        ],
        "max_tokens": 50,
        "temperature": 0.1
    }
    
    start_time = time.time()
    try:
        response = requests.post(f"{url}/chat/completions", headers=headers, json=data, timeout=60)
        duration = time.time() - start_time
        if response.status_code == 200:
            return duration, response.json()["choices"][0]["message"]["content"].strip()
        else:
            return None, f"Error: {response.status_code}"
    except Exception as e:
        return None, str(e)

def run_experiment():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT sender, subject, snippet FROM emails WHERE snippet IS NOT NULL LIMIT 3")
    emails = c.fetchall()
    conn.close()

    sizes = [500, 1000, 2000, 4000, 8000, 16000]
    
    print(f"Experimenting with Model: {model}")
    print(f"{'Size (chars)':<15} | {'Avg Time (s)':<15} | {'Example Result'}")
    print("-" * 60)

    for size in sizes:
        durations = []
        last_result = ""
        for email in emails:
            duration, result = test_snippet_size(email["sender"], email["subject"], email["snippet"], size)
            if duration:
                durations.append(duration)
                last_result = result
        
        if durations:
            avg_time = sum(durations) / len(durations)
            print(f"{size:<15} | {avg_time:<15.4f} | {last_result}")
        else:
            print(f"{size:<15} | {'FAILED':<15} | {last_result}")

if __name__ == "__main__":
    run_experiment()
