import argparse
import logging
import os

import fasttext  # type: ignore

from email_sort.config import get_config_dir
from email_sort.db import EMAIL_TABLE, get_db
from email_sort.heuristics import MODEL_PATH, download_model
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


def detect_languages(source=None, batch_size=1000):
    download_model()
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Model not found and could not be downloaded to {MODEL_PATH}")
        return

    logger.info(f"Loading model from {MODEL_PATH}...")
    model = fasttext.load_model(str(MODEL_PATH))
    logger.info("Model loaded.")

    conn = get_db()
    cursor = conn.cursor()

    # Find emails without language
    source_filter = "AND source = ?" if source else ""
    params = (source,) if source else ()
    cursor.execute(
        f"SELECT COUNT(*) FROM {EMAIL_TABLE} WHERE language IS NULL {source_filter}", params
    )
    total = cursor.fetchone()[0]

    if total == 0:
        logger.info(f"No emails to process{f' for source={source}' if source else ''}.")
        conn.close()
        return

    logger.info(f"Processing {total} emails{f' for source={source}' if source else ''}...")

    # We'll process in batches to avoid loading everything into memory
    # and to commit periodically

    processed = 0
    progress = make_progress()
    with progress:
        task = progress.add_task(f"Detecting {source or 'all sources'}", total=total)
        while True:
            cursor.execute(
                f"""
                SELECT id, subject, snippet
                FROM {EMAIL_TABLE}
                WHERE language IS NULL
                {source_filter}
                LIMIT ?
            """,
                (*params, batch_size),
            )
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
            cursor.executemany(f"UPDATE {EMAIL_TABLE} SET language = ? WHERE id = ?", updates)
            conn.commit()

            processed += len(rows)
            progress.advance(task, len(rows))

    conn.close()
    logger.info(f"Finished processing {processed} emails.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect language of emails using fastText")
    parser.add_argument(
        "--source",
        type=str,
        help="Only process one source",
    )
    parser.add_argument("--batch", type=int, default=1000, help="Batch size for processing")
    args = parser.parse_args()

    detect_languages(args.source, args.batch)
