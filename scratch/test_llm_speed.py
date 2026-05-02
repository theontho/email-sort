import os
import json
import time
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

base_url = os.environ.get("LITELLM_URL", "http://localhost:1234/v1")
api_key = os.environ.get("LMSTUDIO_KEY", "lm-studio")
model_name = os.environ.get("MODEL_NAME", "qwen3.5-9b")

print(f"Testing model: {model_name} at {base_url}...")
start_time = time.time()

client = OpenAI(base_url=base_url, api_key=api_key)

try:
    completion = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "You are a helpful assistant. Reply only with 'pong'."},
            {"role": "user", "content": "ping"}
        ],
        temperature=0.1,
    )
    print(f"Response: {completion.choices[0].message.content}")
    print(f"Time taken: {time.time() - start_time:.2f} seconds")
except Exception as e:
    print(f"Error: {e}")
