from email_sort.db import get_db
import fasttext  # type: ignore
import os
import urllib.request
from email_sort.config import get_setting, get_config_dir

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# Path relative to centralized config directory
MODEL_PATH = get_config_dir() / "models" / "lid.176.bin"


def download_model():
    if not os.path.exists(MODEL_PATH):
        print("Downloading fasttext language model (this takes a moment)...")
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        urllib.request.urlretrieve(MODEL_URL, MODEL_PATH)


def run_heuristics():
    download_model()
    # Suppress warning
    fasttext.FastText.eprint = lambda x: None
    model = fasttext.load_model(MODEL_PATH)

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, subject, snippet, to_address FROM emails")
    rows = c.fetchall()

    print(f"Running heuristics on {len(rows)} emails...")

    updates = []
    my_domains = get_setting("my_domains", ["icloud.com", "appleid.com", "gmail.com"])

    for row in rows:
        email_id = row["id"]
        subject = row["subject"] or ""
        snippet = row["snippet"] or ""
        to_address = row["to_address"] or ""

        text_to_detect = (subject + " " + snippet)[:200].replace("\n", " ").strip()
        lang = "unknown"
        if text_to_detect:
            try:
                predictions = model.predict(text_to_detect)
                lang = predictions[0][0].replace("__label__", "")
            except Exception:
                # print(f"Error predicting: {e}")
                pass

        is_not_for_me = 0
        to_addr_lower = to_address.lower()
        if not any(domain in to_addr_lower for domain in my_domains):
            is_not_for_me = 1

        updates.append((lang, is_not_for_me, email_id))

        if len(updates) >= 1000:
            c.executemany("UPDATE emails SET language=?, is_not_for_me=? WHERE id=?", updates)
            conn.commit()
            updates = []

    if updates:
        c.executemany("UPDATE emails SET language=?, is_not_for_me=? WHERE id=?", updates)
        conn.commit()

    print("Heuristics complete.")


if __name__ == "__main__":
    run_heuristics()
