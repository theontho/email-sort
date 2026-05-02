import logging
import time
import threading
import argparse
import collections
import queue
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
from dotenv import load_dotenv
from email_sort.db import get_db
from email_sort.config import get_setting, get_servers, get_config_dir

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
    ProgressColumn,
)
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.text import Text
from rich.layout import Layout

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


lock = threading.Lock()
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


def classify_single_email(worker_pool, row, pbar, pbar_task, table_name="emails"):
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
        )

        if stop_event.is_set():
            return

        message = completion.choices[0].message
        content = (message.content or "").strip()

        category, confidence, suggested_category, action = "Other", 0.0, "", ""
        if content:
            clean_content = content.replace("`", "").replace("'", "").replace('"', "").strip()
            parts = [p.strip() for p in clean_content.split(",")]

            if len(parts) >= 1:
                category = parts[0]
            if len(parts) >= 2:
                try:
                    confidence = float(parts[1])
                except ValueError, IndexError:
                    confidence = 0.0
            if len(parts) >= 3:
                suggested_category = parts[2]
            if len(parts) >= 4:
                action = parts[3]

        # Validate category
        valid_categories = {
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
        if category not in valid_categories:
            category = "Other"

        # Validate action
        valid_actions = {
            "Authentication",
            "Mandatory",
            "Informational",
            "Newsletter",
            "Promotional",
            "Social",
            "Personal",
        }
        if action not in valid_actions:
            action = ""

        duration = time.time() - start_time

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE {table_name} SET category=?, confidence=?, suggested_category=?, action=?, classify_model=?, classify_time=? WHERE id=?",
            (
                category,
                confidence,
                suggested_category,
                action,
                f"{model_name}@{server_name}",
                duration,
                email_id,
            ),
        )
        conn.commit()
        conn.close()

        if pbar and pbar_task is not None:
            pbar.update(pbar_task, advance=1)

        update_status(
            email_id,
            f"[green]✓[/green] [dim]ID {email_id}:[/dim] [bold]{category}[/bold] ({confidence}) | [yellow]{action}[/yellow] [dim]({duration:.1f}s on {server_name})[/dim]",
            is_finished=True,
        )

    except Exception as e:
        if not stop_event.is_set():
            err_msg = f"[red]✗[/red] [dim]ID {email_id}:[/dim] {str(e)[:50]}"
            update_status(email_id, err_msg, is_finished=True)
            logger.error(f"!!! Error classifying {email_id} in {table_name} on {server_url}: {e}")
    finally:
        # ALWAYS put the client info back in the pool
        worker_pool.put(client_info)


class SpeedColumn(ProgressColumn):
    """Renders the processing speed."""

    def render(self, task):
        speed = task.speed
        if speed is None:
            return Text("0.00 emails/s", style="bold blue")
        return Text(f"{speed:.2f} emails/s", style="bold blue")


def classify_emails(limit=None, table_name="emails", window=100, reclassify=None):
    global finished_tasks
    finished_tasks = collections.deque(maxlen=window)

    conn = get_db()
    c = conn.cursor()

    tables = ["emails", "google_emails"] if table_name == "all" else [table_name]

    # Calculate per-table limit to ensure variety if a limit is provided
    table_limit = None
    if limit and len(tables) > 1:
        table_limit = (limit // len(tables)) + 1
    elif limit:
        table_limit = limit

    all_rows = []
    for t in tables:
        if reclassify == "all":
            print(f"Clearing all classification data in {t} for a fresh start...")
            c.execute(
                f"UPDATE {t} SET category = NULL, action = NULL, suggested_category = NULL, confidence = NULL, classify_model = NULL, classify_time = NULL"
            )
            conn.commit()
        elif reclassify:
            cats = [cat.strip() for cat in reclassify.split(",")]
            print(f"Clearing categories {cats} in {t} for re-classification...")
            placeholders = ",".join(["?"] * len(cats))
            c.execute(
                f"UPDATE {t} SET category = NULL, action = NULL, suggested_category = NULL, confidence = NULL, classify_model = NULL, classify_time = NULL WHERE category IN ({placeholders})",
                cats,
            )
            conn.commit()

        query = f"""
            SELECT id, sender, subject, snippet 
            FROM {t} 
            WHERE (category IS NULL OR category = '') 
            AND language = 'en' 
            AND is_not_for_me = 0
            AND dmarc_fail = 0
        """
        if table_limit:
            query += f" LIMIT {table_limit}"

        try:
            c.execute(query)
            rows = c.fetchall()
            # Attach table name to each row
            for row in rows:
                r_dict = dict(row)
                r_dict["_table"] = t
                all_rows.append(r_dict)
        except Exception as e:
            print(f"Error accessing table {t}: {e}")

    conn.close()

    if not all_rows:
        print("No emails to classify.")
        return

    # If we have a limit and multiple tables, we might have exceeded it.
    if limit and len(all_rows) > limit:
        all_rows = all_rows[:limit]

    worker_pool, total_workers = get_worker_pool()
    logger.info(
        f"Starting classification with {total_workers} workers across multiple servers on {len(tables)} table(s)"
    )

    # Progress tracking
    layout = Layout()
    layout.split_column(Layout(name="status"), Layout(name="progress", size=3))

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        SpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        expand=True,
    )
    pbar_task = progress.add_task("Classifying Emails...", total=len(all_rows))
    layout["progress"].update(progress)
    layout["status"].update(StatusDisplay())

    try:
        with Live(layout, refresh_per_second=4, console=console, screen=True):
            with ThreadPoolExecutor(max_workers=total_workers) as executor:
                for row in all_rows:
                    if stop_event.is_set():
                        break
                    executor.submit(
                        classify_single_email,
                        worker_pool,
                        row,
                        progress,
                        pbar_task,
                        row["_table"],
                    )

                executor.shutdown(wait=True)
    except KeyboardInterrupt:
        stop_event.set()
        print("\nStopping classification...")
    finally:
        print("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify emails using LLM")
    parser.add_argument("--limit", type=int, help="Maximum number of emails to classify")
    parser.add_argument("--table", type=str, default="all", help="Table name to classify")
    parser.add_argument("--window", type=int, default=100, help="Number of status lines to buffer")
    parser.add_argument(
        "--reclassify",
        type=str,
        help="Re-classify emails. Use 'all' or a comma-separated list of categories to clear.",
    )
    args = parser.parse_args()

    classify_emails(
        limit=args.limit,
        table_name=args.table,
        window=args.window,
        reclassify=args.reclassify,
    )
