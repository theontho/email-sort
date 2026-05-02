import argparse
import sys
import time
import os
from email_sort.db import get_db, DB_PATH
from email_sort.config import get_config_path, get_config_dir


def command_ingest_fastmail(args):
    from email_sort.ingest_fastmail import ingest_fastmail

    ingest_fastmail()


def command_ingest_mbox(args):
    from email_sort.ingest_mbox import parse_mbox

    parse_mbox(args.mbox_path, args.table)


def command_classify(args):
    from email_sort.classify import classify_emails

    classify_emails(
        limit=args.limit,
        table_name=args.table,
        window=args.window,
        reclassify=args.reclassify,
    )


def command_detect_language(args):
    from email_sort.detect_language import detect_languages

    if args.table == "all":
        detect_languages("emails", args.batch)
        detect_languages("google_emails", args.batch)
    else:
        detect_languages(args.table, args.batch)


def command_init_db(args):
    from email_sort.db import init_db

    init_db()
    print(f"Database initialized at {DB_PATH}")


def command_heuristics(args):
    from email_sort.heuristics import run_heuristics

    run_heuristics()


def command_export(args):
    from email_sort.export import export_results

    export_results()


def command_config(args):
    """Shows the current configuration and data paths."""
    print("--- Email Sort Configuration ---")
    print(f"Config File:   {get_config_path()}")
    print(f"Config Dir:    {get_config_dir()}")
    print(f"Database:      {DB_PATH}")

    log_file = get_config_dir() / "classification.log"
    print(f"Log File:      {log_file}")

    from email_sort.heuristics import MODEL_PATH

    print(f"Model Path:    {MODEL_PATH}")

    if not get_config_path().exists():
        print("\n[WARNING] Config file does not exist yet.")
        print(f"Create it at {get_config_path()} to customize settings.")


def command_watch(args):
    log_file = get_config_dir() / "classification.log"

    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    print("--- Email Sort Progress Monitor ---")
    print("Press Ctrl+C to stop watching.")
    print("")

    try:
        while True:
            os.system("cls" if os.name == "nt" else "clear")
            print("--- Email Sort Progress Monitor ---")
            print(time.strftime("%a %b %d %H:%M:%S %Z %Y"))
            print("")
            print(f"Database: {DB_PATH}")
            print("")

            conn = get_db()
            c = conn.cursor()

            c.execute("SELECT COUNT(*) FROM emails")
            total = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM emails WHERE category IS NOT NULL")
            classified = c.fetchone()[0]

            c.execute(
                "SELECT COUNT(*) FROM emails WHERE language != 'en' OR is_not_for_me = 1 OR dmarc_fail = 1"
            )
            filtered = c.fetchone()[0]

            conn.close()

            remaining = total - classified - filtered

            print(f"Total Emails:    {total}")
            print(f"Filtered:        {filtered} (Non-English / Not for me)")
            print(f"Classified:      {classified}")
            print(f"Remaining:       {remaining}")

            if total > 0:
                percent = ((classified + filtered) * 100) // total
                print(f"Overall Progress: {percent}%")

            print("")
            print("--- Last 5 Log Entries ---")
            if log_file.exists():
                try:
                    with open(log_file, "r") as f:
                        lines = f.readlines()
                        for line in lines[-5:]:
                            print(line, end="")
                except Exception:
                    print("Could not read log file.")
            else:
                print(f"No log file found yet ({log_file})")

            print("")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopped watching.")


def main():
    parser = argparse.ArgumentParser(description="email-sort CLI toolkit")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ingest-fastmail
    parser_fastmail = subparsers.add_parser(
        "ingest-fastmail", help="Ingest emails from Fastmail using JMAP"
    )
    parser_fastmail.set_defaults(func=command_ingest_fastmail)

    # ingest-mbox
    parser_mbox = subparsers.add_parser(
        "ingest-mbox", help="Ingest emails from an MBOX file"
    )
    parser_mbox.add_argument("mbox_path", type=str, help="Path to the mbox file")
    parser_mbox.add_argument(
        "--table",
        type=str,
        default="emails",
        help="Database table name (default: emails)",
    )
    parser_mbox.set_defaults(func=command_ingest_mbox)

    # classify
    parser_classify = subparsers.add_parser(
        "classify", help="Classify emails using LLM"
    )
    parser_classify.add_argument(
        "--limit", type=int, help="Maximum number of emails to classify"
    )
    parser_classify.add_argument(
        "--table", type=str, default="all", help="Table name to classify (default: all)"
    )
    parser_classify.add_argument(
        "--window", type=int, default=100, help="Number of status lines to buffer"
    )
    parser_classify.add_argument(
        "--reclassify",
        type=str,
        help="Re-classify emails. Use 'all' or a comma-separated list of categories to clear.",
    )
    parser_classify.set_defaults(func=command_classify)

    # detect-language
    parser_lang = subparsers.add_parser(
        "detect-language", help="Detect language of emails using fastText"
    )
    parser_lang.add_argument(
        "--table",
        type=str,
        default="all",
        help="Table to process (emails, google_emails, or all)",
    )
    parser_lang.add_argument(
        "--batch", type=int, default=1000, help="Batch size for processing"
    )
    parser_lang.set_defaults(func=command_detect_language)

    # init-db
    parser_init_db = subparsers.add_parser(
        "init-db", help="Initialize the SQLite database"
    )
    parser_init_db.set_defaults(func=command_init_db)

    # heuristics
    parser_heuristics = subparsers.add_parser(
        "heuristics",
        help="Run fast heuristics to detect language and obvious automated/non-personal emails",
    )
    parser_heuristics.set_defaults(func=command_heuristics)

    # export
    parser_export = subparsers.add_parser(
        "export", help="Export results to CSV (ban_list.csv and unsubscribe_list.csv)"
    )
    parser_export.set_defaults(func=command_export)

    # config
    parser_config = subparsers.add_parser(
        "config", help="Show current configuration and paths"
    )
    parser_config.set_defaults(func=command_config)

    # watch
    parser_watch = subparsers.add_parser(
        "watch", help="Watch progress of email classification"
    )
    parser_watch.set_defaults(func=command_watch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
