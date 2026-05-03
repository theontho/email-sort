import argparse
import collections
import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from openai import OpenAI
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.table import Table

from email_sort.config import get_config_dir, get_servers, get_setting
from email_sort.corrections import apply_sender_prefilters
from email_sort.db import EMAIL_TABLE, get_db
from email_sort.progress import make_progress
from email_sort.sender_analysis import apply_has_user_reply_prefilter

load_dotenv()

# Global console for size detection
console = Console()

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

log_path = get_config_dir() / "classification.log"
file_handler = logging.FileHandler(str(log_path))
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Silence noisy third-party loggers
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# Parse multiple servers from configuration
def get_worker_pool():
    worker_pool: queue.Queue[tuple[OpenAI, str, str, str]] = queue.Queue()
    total_workers = 0
    default_model = get_setting("model_name", "qwen3.5-9b")
    default_api_key = get_setting("lmstudio_key", "lm-studio")

    server_configs = get_servers()

    if server_configs:
        for cfg in server_configs:
            if cfg.get("disabled", False):
                logger.info(f"Skipping disabled server: {cfg.get('url')}")
                continue

            url = cfg["url"]
            workers_count = cfg.get("workers", 1)
            api_key = cfg.get("api_key", default_api_key)
            model_name = cfg.get("model", default_model)
            server_name = cfg.get("name", url.split("//")[-1].split(":")[0])

            client = OpenAI(base_url=url, api_key=api_key)
            for _ in range(workers_count):
                worker_pool.put((client, model_name, url, server_name))
                total_workers += 1
    else:
        # Fallback to env vars or defaults
        base_url = get_setting("litellm_url", "http://localhost:1234/v1")
        workers = int(get_setting("max_workers", 1))

        server_name = base_url.split("//")[-1].split(":")[0]
        client = OpenAI(base_url=base_url, api_key=default_api_key)
        for _ in range(workers):
            worker_pool.put((client, default_model, base_url, server_name))
            total_workers += 1

    return worker_pool, total_workers


stop_event = threading.Event()

# Global state for display
active_tasks: dict[int, str] = {}
finished_tasks: collections.deque[str] = collections.deque(maxlen=100)
display_lock = threading.Lock()


def update_status(email_id, msg, is_finished=False):
    # Log to file
    logger.info(msg)
    # Update state for live display
    with display_lock:
        if is_finished:
            if email_id in active_tasks:
                del active_tasks[email_id]
            finished_tasks.appendleft(msg)
        else:
            active_tasks[email_id] = msg


def create_display_table():
    table = Table.grid(expand=True)
    table.add_column(no_wrap=True)

    with display_lock:
        active = list(active_tasks.values())
        finished = list(finished_tasks)

    rows = active + finished

    # Use almost the entire height minus progress bar
    display_height = max(5, console.size.height - 1)

    for i in range(display_height - 1):  # -1 for progress bar
        if i < len(rows):
            table.add_row(rows[i])
        else:
            table.add_row(" ")

    return table


class StatusDisplay:
    """A dynamic renderable for the status window."""

    def __rich__(self):
        return create_display_table()


VALID_CATEGORIES = {
    "Finance",
    "Health",
    "Work",
    "Newsletter",
    "Promotional",
    "Social",
    "Home",
    "Education",
    "Tech",
    "Shopping",
    "Travel",
    "Security",
    "Shipping",
    "Personal",
    "Spam",
    "Other",
}

VALID_ACTIONS = {
    "Authentication",
    "Mandatory",
    "Informational",
    "Newsletter",
    "Promotional",
    "Social",
    "Personal",
}
NORMALIZED_CATEGORIES = {value.lower(): value for value in VALID_CATEGORIES}
NORMALIZED_ACTIONS = {value.lower(): value for value in VALID_ACTIONS}


def parse_classification(content: str) -> tuple[str, float, str, str]:
    category, confidence, suggested_category, action = "Other", 0.0, "", ""
    for line in reversed([line.strip() for line in content.splitlines() if line.strip()]):
        clean_content = line.replace("`", "").replace("'", "").replace('"', "").strip()
        parts = [p.strip() for p in clean_content.split(",")]
        if len(parts) < 2:
            continue
        candidate_category = NORMALIZED_CATEGORIES.get(parts[0].lower(), parts[0])
        try:
            candidate_confidence = float(parts[1])
        except ValueError, IndexError:
            continue
        candidate_action = parts[3] if len(parts) >= 4 else ""
        if candidate_category not in VALID_CATEGORIES:
            continue
        if candidate_action:
            candidate_action = NORMALIZED_ACTIONS.get(candidate_action.lower(), "")
        return (
            candidate_category,
            candidate_confidence,
            parts[2] if len(parts) >= 3 else "",
            candidate_action,
        )
    return category, confidence, suggested_category, action


def _write_batch(cursor, updates: list[tuple]) -> None:
    cursor.executemany(
        f"""
        UPDATE {EMAIL_TABLE}
        SET category = ?, confidence = ?, suggested_category = ?, action = ?,
            classify_model = ?, classify_time = ?
        WHERE id = ?
        """,
        updates,
    )


def classification_writer(
    result_queue: queue.Queue, pbar, pbar_task, batch_size: int = 100
) -> None:
    conn = get_db()
    pending: list[tuple] = []
    try:
        cursor = conn.cursor()
        while True:
            item = result_queue.get()
            if item is None:
                break
            pending.append(item)
            if len(pending) >= batch_size:
                _write_batch(cursor, pending)
                conn.commit()
                if pbar and pbar_task is not None:
                    pbar.update(pbar_task, advance=len(pending))
                pending.clear()
        if pending:
            _write_batch(cursor, pending)
            conn.commit()
            if pbar and pbar_task is not None:
                pbar.update(pbar_task, advance=len(pending))
    finally:
        conn.close()


def classify_single_email(worker_pool, result_queue, row):
    if stop_event.is_set():
        return

    email_id = row["id"]
    sender = row["sender"]
    subject = row["subject"] or ""
    snippet = (row["snippet"] or "")[:2000]

    prompt_body = f"Sender: {sender}\nSubject: {subject}\nSnippet: {snippet}"

    # Get a client from the pool
    client_info = worker_pool.get()
    client, model_name, server_url, server_name = client_info

    try:
        # Update status for sending
        update_status(
            email_id,
            f"[cyan]→[/cyan] [dim]ID {email_id}:[/dim] [blue]{sender[:40]}[/blue] [dim]({server_name})[/dim]",
        )

        start_time = time.time()
        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {
                    "role": "system",
                    "content": "You are a highly efficient email classifier. Output ONLY four items, comma separated: category, confidence, suggested_category, action.\n"
                    "Categories: Finance, Health, Work, Newsletter, Promotional, Social, Home, Education, Tech, Shopping, Travel, Security, Shipping, Personal, Spam, Other.\n"
                    "Action (Message Type): Authentication, Mandatory, Informational, Newsletter, Promotional, Social, Personal.\n"
                    "Example: 'Security, 0.98, Password Reset, Authentication'.\n"
                    "DO NOT include any other text. suggested_category should be 2-4 words.",
                },
                {"role": "user", "content": prompt_body},
            ],
            temperature=0.1,
            timeout=120.0,
            max_tokens=512,
            extra_body={"reasoning_effort": "none"},
        )

        if stop_event.is_set():
            return

        message = completion.choices[0].message
        content = (message.content or "").strip()
        if not content:
            content = (getattr(message, "reasoning_content", None) or "").strip()

        category, confidence, suggested_category, action = parse_classification(content)

        duration = time.time() - start_time

        result_queue.put(
            (
                category,
                confidence,
                suggested_category,
                action,
                f"{model_name}@{server_name}",
                duration,
                email_id,
            )
        )

        update_status(
            email_id,
            f"[green]✓[/green] [dim]ID {email_id}:[/dim] [bold]{category}[/bold] ({confidence}) | [yellow]{action}[/yellow] [dim]({duration:.1f}s on {server_name})[/dim]",
            is_finished=True,
        )

    except Exception as e:
        if not stop_event.is_set():
            err_msg = f"[red]✗[/red] [dim]ID {email_id}:[/dim] {str(e)[:50]}"
            update_status(email_id, err_msg, is_finished=True)
            logger.error(f"!!! Error classifying {email_id} on {server_url}: {e}")
    finally:
        # ALWAYS put the client info back in the pool
        worker_pool.put(client_info)


def classify_emails(limit=None, source=None, window=100, reclassify=None):
    from email_sort.db import init_db

    init_db()
    global finished_tasks
    finished_tasks = collections.deque(maxlen=window)

    conn = get_db()
    c = conn.cursor()

    source_filter = ""
    params: list = []
    if source:
        source_filter = "AND source = ?"
        params.append(source)

    if reclassify == "all":
        print("Clearing all LLM classification data for a fresh start...")
        c.execute(
            f"""
            UPDATE {EMAIL_TABLE}
            SET category = NULL, action = NULL, suggested_category = NULL,
                confidence = NULL, classify_model = NULL, classify_time = NULL
            WHERE (? IS NULL OR source = ?)
            """,
            (source, source),
        )
        conn.commit()
    elif reclassify:
        cats = [cat.strip() for cat in reclassify.split(",")]
        print(f"Clearing categories {cats} for re-classification...")
        placeholders = ",".join(["?"] * len(cats))
        c.execute(
            f"""
            UPDATE {EMAIL_TABLE}
            SET category = NULL, action = NULL, suggested_category = NULL,
                confidence = NULL, classify_model = NULL, classify_time = NULL
            WHERE category IN ({placeholders}) AND (? IS NULL OR source = ?)
            """,
            [*cats, source, source],
        )
        conn.commit()

    override_count = apply_sender_prefilters(source)
    reply_count = apply_has_user_reply_prefilter(source)
    if override_count or reply_count:
        print(
            f"Applied prefilters: {override_count} sender overrides, {reply_count} user-reply senders"
        )

    query = f"""
        SELECT id, sender, subject, snippet
        FROM {EMAIL_TABLE}
        WHERE (category IS NULL OR category = '')
        AND (heuristic_category IS NULL OR heuristic_category = '')
        AND language = 'en'
        AND is_not_for_me = 0
        AND (dmarc_fail = 0 OR dmarc_arc_override = 1)
        {source_filter}
        ORDER BY id
    """
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    c.execute(query, params)
    all_rows = [dict(row) for row in c.fetchall()]

    conn.close()

    if not all_rows:
        print("No emails to classify.")
        return

    worker_pool, total_workers = get_worker_pool()
    if total_workers <= 0:
        raise RuntimeError("No classification workers configured")
    logger.info(f"Starting classification with {total_workers} workers across multiple servers")

    # Progress tracking
    layout = Layout()
    layout.split_column(Layout(name="status"), Layout(name="progress", size=3))

    progress = make_progress(spinner=True, bar_width=None, console=console, expand=True)
    pbar_task = progress.add_task("Classifying Emails...", total=len(all_rows))
    layout["progress"].update(progress)
    layout["status"].update(StatusDisplay())

    try:
        with Live(layout, refresh_per_second=4, console=console, screen=True):
            result_queue: queue.Queue = queue.Queue()
            writer = threading.Thread(
                target=classification_writer,
                args=(result_queue, progress, pbar_task),
                daemon=True,
            )
            writer.start()
            with ThreadPoolExecutor(max_workers=total_workers) as executor:
                for row in all_rows:
                    if stop_event.is_set():
                        break
                    executor.submit(
                        classify_single_email,
                        worker_pool,
                        result_queue,
                        row,
                    )

                executor.shutdown(wait=True)
            result_queue.put(None)
            writer.join()
    except KeyboardInterrupt:
        stop_event.set()
        print("\nStopping classification...")
    finally:
        print("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify emails using LLM")
    parser.add_argument("--limit", type=int, help="Maximum number of emails to classify")
    parser.add_argument("--source", type=str, help="Only classify one source")
    parser.add_argument("--table", dest="source", help=argparse.SUPPRESS)
    parser.add_argument("--window", type=int, default=100, help="Number of status lines to buffer")
    parser.add_argument(
        "--reclassify",
        type=str,
        help="Re-classify emails. Use 'all' or a comma-separated list of categories to clear.",
    )
    args = parser.parse_args()

    classify_emails(
        limit=args.limit,
        source=args.source,
        window=args.window,
        reclassify=args.reclassify,
    )
