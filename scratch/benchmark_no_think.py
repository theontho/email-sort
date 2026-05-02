import os
import time
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://192.168.1.95:1234/v1"
API_KEY = os.environ.get("LMSTUDIO_KEY", "lm-studio")
MODEL = "qwen/qwen3.6-35b-a3b"

client = OpenAI(base_url=BASE_URL, api_key=API_KEY)

SAMPLES = [
    {
        "sender": "shipping@example-store.com",
        "subject": "Your order #123456789 is on the way",
        "snippet": "Your shipment from Awesome Store is on the way. Scheduled delivery date Friday 5/15/2026. Estimated between 10:00am and 2:00pm. Track your package here.",
    },
    {
        "sender": "alerts@security-example.com",
        "subject": "Security Alert: New login detected",
        "snippet": "We noticed a new login to your account from a different device or location. If this was you, you can safely ignore this message. If not, please secure your account immediately.",
    },
    {
        "sender": "colleague@work-example.com",
        "subject": "Project Update Meeting",
        "snippet": "Hi John, I am working alongside a team that is looking to better understand the technical requirements for the upcoming phase. Would you be available for a quick chat tomorrow?",
    },
]

SYSTEM_PROMPT = "You are a highly efficient email classifier. Output ONLY three items, comma separated: category, confidence, suggested_category. \nExample: 'Promotional, 0.95, Fitness Gear Sale'. \nDO NOT include any other text. Categories: Billing, Newsletter, Work, Personal, Promotional, Security, Shipping, Travel, Spam, Other. The 'suggested_category' should be a freeform specific description (2-4 words) of the email's actual content."


def run_test(name, system_msg, user_prefix=""):
    print(f"\n--- Testing: {name} ---")
    results = []
    for i, sample in enumerate(SAMPLES):
        prompt_body = f"{user_prefix}Sender: {sample['sender']}\nSubject: {sample['subject']}\nSnippet: {sample['snippet']}"

        start_time = time.time()
        try:
            completion = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt_body},
                ],
                temperature=0.1,
                timeout=60.0,
            )
            duration = time.time() - start_time
            content = completion.choices[0].message.content
            has_think = "<think>" in content

            print(
                f"Sample {i + 1}: {duration:.2f}s | Think: {has_think} | Content: {content.strip()}"
            )
            results.append({"duration": duration, "has_think": has_think, "length": len(content)})
        except Exception as e:
            print(f"Sample {i + 1}: FAILED - {e}")

    avg_time = sum(r["duration"] for r in results) / len(results) if results else 0
    think_count = sum(1 for r in results if r["has_think"])
    print(f"SUMMARY {name}: Avg Time: {avg_time:.2f}s | Think Count: {think_count}/{len(SAMPLES)}")
    return results


# 1. Control
run_test("Control", SYSTEM_PROMPT)

# 2. System /no_think
run_test("System /no_think", "/no_think\n" + SYSTEM_PROMPT)

# 3. User /no_think
run_test("User /no_think", SYSTEM_PROMPT, user_prefix="/no_think\n")

# 4. Both
run_test("Both /no_think", "/no_think\n" + SYSTEM_PROMPT, user_prefix="/no_think\n")
