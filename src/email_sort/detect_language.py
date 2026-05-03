import os
import logging
import argparse
import fasttext  # type: ignore
from email_sort.db import get_db
from email_sort.heuristics import download_model, MODEL_PATH
from email_sort.config import get_config_dir
from email_sort.progress import make_progress

# Setup logging
log_path = get_config_dir() / "language_detection.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(str(log_path)), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Language detection logic


def detect_languages(table_name="fastmail", batch_size=1000):
    download_model()
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Model not found and could not be downloaded to {MODEL_PATH}")
        return

    logger.info(f"Loading model from {MODEL_PATH}...")
    model = fasttext.load_model(MODEL_PATH)
    logger.info("Model loaded.")

    conn = get_db()
    cursor = conn.cursor()

    # Find emails without language
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE language IS NULL")
    total = cursor.fetchone()[0]

    if total == 0:
        logger.info(f"No emails to process in {table_name}.")
        conn.close()
        return

    logger.info(f"Processing {total} emails in {table_name}...")

    # We'll process in batches to avoid loading everything into memory
    # and to commit periodically

    processed = 0
    progress = make_progress()
    with progress:
        task = progress.add_task(f"Detecting {table_name}", total=total)
        while True:
            cursor.execute(f"""
                SELECT id, subject, snippet 
                FROM {table_name} 
                WHERE language IS NULL 
                LIMIT {batch_size}
            """)
            rows = cursor.fetchall()
            if not rows:
                break

            updates = []
            for row in rows:
                email_id = row["id"]
                subject = row["subject"] or ""
                snippet = row["snippet"] or ""

                # Combine subject and snippet for better detection
                text = f"{subject} {snippet}".strip()
                # Fasttext prefers single line, no newlines
                text = text.replace("\n", " ").replace("\r", " ")

                if not text:
                    lang = "unknown"
                else:
                    try:
                        predictions = model.predict(text, k=1)
                        # predictions is (('__label__en',), array([0.98]))
                        lang_label = predictions[0][0]
                        lang = lang_label.replace("__label__", "")
                    except Exception as e:
                        logger.error(f"Error detecting language for ID {email_id}: {e}")
                        lang = "error"

                updates.append((lang, email_id))

            # Batch update
            cursor.executemany(f"UPDATE {table_name} SET language = ? WHERE id = ?", updates)
            conn.commit()

            processed += len(rows)
            progress.advance(task, len(rows))

    conn.close()
    logger.info(f"Finished processing {processed} emails in {table_name}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect language of emails using fastText")
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        help="Table to process (fastmail, google_emails, or all)",
    )
    parser.add_argument("--batch", type=int, default=1000, help="Batch size for processing")
    args = parser.parse_args()

    if args.table == "all":
        detect_languages("fastmail", args.batch)
        detect_languages("google_emails", args.batch)
    else:
        detect_languages(args.table, args.batch)
